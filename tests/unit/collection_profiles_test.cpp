#include "storage/engine.h"
#include "storage/resources.h"

#include "apps/transport/outcome_json.h"
#include "core/engine/engine_factory.h"
#include "core/engine/store_lease.h"
#include "tests/support/checked.h"
#include "tests/support/fake_storage.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

namespace glamdb {
namespace {

TEST(CollectionProfiles, MaximumEscapedInventoryFitsTheCompleteClientBound)
{
    uint64_t bytes = 1024;
    for (int index = 0; index < 64; ++index) {
        CollectionInfo collection;
        collection.collection_id = detail::NewUuid();
        collection.table_uuid = detail::NewUuid();
        collection.name = std::string(126, '\1') + std::to_string(index + 10);
        collection.creation_sequence = index + 1;
        collection.write_settings.dictionary = DictionaryPolicy::Disabled;
        collection.write_settings.dictionary_page_size_limit = 8LL << 20;
        for (int field = 0; field < 64; ++field) {
            const auto name = std::string(126, '\1') + std::to_string(field + 10);
            collection.schema.fields.push_back({name, FieldType::TimestampUsUtc, true});
            collection.write_settings.columns.push_back({name,
                ParquetCompression{ParquetCodec::Uncompressed},
                DictionaryPolicy::Disabled,
                ParquetEncoding::DeltaBinaryPacked});
        }
        const auto json = json_adapter::CollectionJson(collection);
        ASSERT_TRUE(json) << json.error().message;
        bytes += json->size() + 1;
    }
    EXPECT_LT(bytes, 16ULL << 20);
}

TEST(CollectionProfiles, SixteenCollectionsKeepExactIdentityAndCapacity)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    const auto backend = std::make_shared<test::FakeStorage>(root);
    CreateStoreRequest request{test::store_id, root.string()};
    request.limits.max_collections = 16;
    auto created = detail::EngineFactory::Create(request, backend);
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    const SchemaSpec schema{{{"n", FieldType::Int64, true}}};
    std::vector<CreateCollectionRequest> definitions;
    for (int index = 0; index < 16; ++index) {
        definitions.push_back({detail::NewUuid(), "table-" + std::to_string(index), schema});
        const auto outcome = engine->CreateCollection(definitions.back());
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed);
        ASSERT_TRUE(outcome.info);
        EXPECT_EQ(test::Present(outcome.info).collection_id, definitions.back().collection_id);
    }
    const auto rejected = engine->CreateCollection({detail::NewUuid(), "overflow", schema});
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(rejected.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(engine->CreateCollection(definitions.back()).disposition, CommitDisposition::Committed);
    EXPECT_EQ(engine->ListCollections()->size(), 16U);
    ASSERT_TRUE(engine->Shutdown(std::chrono::seconds(5)));
    engine.reset();
    OpenStoreRequest reopen{root.string(), test::store_id};
    const auto tooSmall = detail::EngineFactory::Open(reopen, backend);
    ASSERT_FALSE(tooSmall);
    EXPECT_EQ(tooSmall.error().code, "CONFIG_TOO_SMALL");
    reopen.limits.max_collections = 16;
    auto opened = detail::EngineFactory::Open(reopen, backend);
    ASSERT_TRUE(opened) << opened.error().message;
    for (const auto& definition : definitions) {
        const auto resolved = (*opened)->ResolveCollection(definition.exact_name);
        ASSERT_TRUE(resolved);
        EXPECT_EQ(resolved->collection_id, definition.collection_id);
        EXPECT_EQ(resolved->schema, schema);
    }
}

TEST(CollectionProfiles, LargerProfilesReserveBeforeCreatingTheStore)
{
    for (const size_t resource : {1U, 2U, 4U}) {
        SCOPED_TRACE(resource);
        test::TempDir temporary;
        const auto root = temporary.path / "store";
        ResourceLimits limits;
        limits.memory_bytes[resource] = resource == 1 ? 64ULL << 20 : resource == 4 ? 768ULL << 20 : 0;
        auto owner = std::make_shared<ResourceOwner>(limits);
        CreateStoreRequest request{test::store_id, root.string()};
        request.resources = owner;
        request.limits.max_collections = 64;
        const auto backend = std::make_shared<test::FakeStorage>(root);
        const auto rejected = detail::EngineFactory::Create(request, backend);
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error().code, "RESOURCE_EXHAUSTED");
        EXPECT_FALSE(std::filesystem::exists(root));
        EXPECT_EQ(owner->GetUsage().memory_bytes, (std::array<uint64_t, 5>{}));
        request.limits.max_collections = 8;
        ASSERT_TRUE(detail::EngineFactory::Create(request, backend));
    }
}

} // namespace
} // namespace glamdb
