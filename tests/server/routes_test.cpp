#include "apps/server/routes.h"
#include "core/engine/engine_factory.h"
#include "tests/support/arrow_batch.h"
#include "tests/support/checked.h"
#include "tests/support/fake_storage.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

namespace glamdb::NServer {
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
