#include "apps/server/routes.h"
#include "core/engine/engine_factory.h"
#include "storage/resources.h"
#include "tests/support/arrow_batch.h"
#include "tests/support/checked.h"
#include "tests/support/fake_storage.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

namespace glamdb::NServer {
TEST(NativeRoutes, CapabilitiesReflectThePublicFacade)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto backend = std::make_shared<test::FakeStorage>(root);
    auto engine = NTransport::Unwrap(detail::EngineFactory::Create({test::store_id, root.string()}, backend));
    TRoutes routes(engine, {}, "secret");
    const auto response = routes.Handle({"GET", "/capabilities", {{"authorization", "Bearer secret"}}, {}});
    ASSERT_EQ(response.Status, 200);
    const auto value = NTransport::ParseJson(response.Body);
    const auto expected = engine->GetCapabilities();
    EXPECT_EQ(value["store_id"], test::store_id);
    EXPECT_EQ(value["features"]["append"], expected.append);
    EXPECT_EQ(value["features"]["scan"], expected.scan);
    EXPECT_EQ(value["features"]["stats"], expected.metadata);
    EXPECT_EQ(value["features"]["snapshots"], expected.metadata);
    EXPECT_EQ(value["features"]["files"], expected.metadata);
    EXPECT_EQ(value["features"]["explain"], expected.metadata);
    EXPECT_EQ(value["file_pruning"], expected.file_pruning);
    EXPECT_EQ(value["row_group_pruning"], expected.row_group_pruning);
    EXPECT_FALSE(value["durability_qualified"].get<bool>());
}

TEST(NativeRoutes, OutcomeLookupRetainsUnknownRecoveryContext)
{
    test::JournalFaultVfs vfs;
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto backend = std::make_shared<test::FakeStorage>(root);
    auto engine =
        NTransport::Unwrap(detail::EngineFactory::Create({test::store_id, root.string()}, backend, vfs.Name()));
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}})
                  .disposition,
        CommitDisposition::Committed);
    vfs.Arm(2, true);
    const auto unknown = engine->AppendDurable({test::collection_id, test::operation_id, {}, {test::Batch()}});
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    TRoutes routes(engine, {}, "secret");
    NTransport::TRequest request;
    request.Method = "GET";
    request.Target = std::string("/collections/") + test::collection_id + "/operations/" + test::operation_id;
    request.Headers["authorization"] = "Bearer secret";
    const auto response = routes.Handle(request);
    ASSERT_EQ(response.Status, 503);
    const auto value = NTransport::ParseJson(response.Body);
    ASSERT_TRUE(value.contains("outcome")) << response.Body;
    EXPECT_EQ(value["outcome"], "commit_unknown");
    EXPECT_EQ(value["operation_id"], test::operation_id);
    const auto& recovery = test::Present(unknown.recovery);
    EXPECT_EQ(value["recovery"]["attempt_id"], recovery.attempt_id);
    EXPECT_EQ(value["recovery"]["payload_hash"], recovery.payload_hash);
    EXPECT_TRUE(value["recovery"]["publication_started"].get<bool>());
    request.Method = "POST";
    request.Target = std::string("/collections/") + test::collection_id + "/append";
    request.Headers["content-type"] = "application/json";
    request.Body = NTransport::TJson(
        {{"operation_id", test::operation_id}, {"rows", {{{"n", "1"}}, {{"n", nullptr}}, {{"n", "1"}}}}})
                       .dump();
    const auto retried = routes.Handle(request);
    ASSERT_EQ(retried.Status, 503);
    EXPECT_EQ(NTransport::ParseJson(retried.Body)["recovery"], value["recovery"]);
    request.Body = NTransport::TJson({{"operation_id", test::operation_id}, {"rows", {{{"n", "2"}}}}}).dump();
    const auto conflict = routes.Handle(request);
    EXPECT_EQ(conflict.Status, 409) << conflict.Body;
    EXPECT_EQ(NTransport::ParseJson(conflict.Body)["error"]["code"], "OPERATION_CONFLICT");
    request.Method = "GET";
    request.Target = std::string("/collections/") + test::collection_id + "/operations/" + test::operation_id;
    vfs.Disarm();
    ASSERT_TRUE(engine->Reconcile());
    EXPECT_EQ(NTransport::ParseJson(routes.Handle(request).Body)["outcome"], "committed");
}

TEST(NativeRoutes, DecodedInputLimitIsAnInputErrorWithoutBindingAnOperation)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto backend = std::make_shared<test::FakeStorage>(root);
    auto engine = NTransport::Unwrap(detail::EngineFactory::Create({test::store_id, root.string()}, backend));
    const SchemaSpec schema{
        {{"a", FieldType::String, false}, {"b", FieldType::String, false}, {"c", FieldType::String, false}}};
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "wide", schema}).disposition,
        CommitDisposition::Committed);
    TRoutes routes(engine, {}, "secret");
    NTransport::TRequest request;
    request.Method = "POST";
    request.Target = std::string("/collections/") + test::collection_id + "/append";
    request.Headers = {{"authorization", "Bearer secret"}, {"content-type", "application/json"}};
    const auto text = std::string(800000, 'x');
    request.Body =
        NTransport::TJson({{"operation_id", test::operation_id}, {"rows", {{{"a", text}, {"b", text}, {"c", text}}}}})
            .dump();
    const auto response = routes.Handle(request);
    EXPECT_EQ(response.Status, 413) << response.Body;
    const auto value = NTransport::ParseJson(response.Body);
    EXPECT_EQ(value["error"]["code"], "INPUT_TOO_LARGE");
    EXPECT_EQ(value["error"]["retry_hint"], "FixInput");
    EXPECT_EQ(value["outcome"], "definitely_not_committed");
    const auto outcome = engine->FindAppendOutcome(test::collection_id, test::operation_id);
    ASSERT_TRUE(outcome);
    EXPECT_FALSE(outcome->has_value());
}
} // namespace glamdb::NServer

namespace glamdb::NServer {
TEST(NativeRoutes, SharedPressurePreservesLookupAndOutgoingResponseCharges)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    auto created = CreateStore(
        {.store_id = test::store_id, .absolute_root = (temporary.path / "store").string(), .resources = owner});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = *created;
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}})
                  .disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->AppendDurable({test::collection_id, test::operation_id, {}, {test::Batch()}}).disposition,
        CommitDisposition::Committed);
    TRoutes routes(engine, {}, "secret");
    NTransport::TRequest request;
    request.Method = "GET";
    request.Target = std::string("/collections/") + test::collection_id + "/operations/" + test::operation_id;
    request.Headers["authorization"] = "Bearer secret";
    auto saturated = owner->Reserve(ResourceClass::Data, owner->GetLimits().memory_bytes[0]);
    ASSERT_TRUE(saturated);
    {
        auto response = routes.Handle(request);
        ASSERT_EQ(response.Status, 200) << response.Body;
        EXPECT_GT(owner->GetUsage().memory_bytes[3], 0);
    }
    EXPECT_EQ(owner->GetUsage().memory_bytes[3], 0);
    request.Target = std::string("/collections/") + test::collection_id + "/scan";
    request.Method = "POST";
    request.Body = "{}";
    request.Headers["content-type"] = "application/json";
    EXPECT_EQ(routes.Handle(request).Status, 507);
    saturated->reset();
    auto response = routes.Handle(request);
    EXPECT_EQ(response.Status, 200) << response.Body;
    EXPECT_GT(owner->GetUsage().memory_bytes[0], 0);
}
} // namespace glamdb::NServer
