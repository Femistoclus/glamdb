#include "core/storage/kv_state.h"
#include "core/storage/snapshot_retention.h"
#include "core/storage/table_profile.h"
#include "tests/support/memory_file_io.h"

#include <gtest/gtest.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>

namespace NGlamDB::NStorage {
using iceberg::Error;
namespace {

constexpr auto StoreID = "11111111-1111-4111-8111-111111111111";
constexpr auto CollectionID = "22222222-2222-4222-8222-222222222222";
const std::string Location = std::string("store://") + StoreID + "/warehouse/" + CollectionID;

iceberg::Result<std::shared_ptr<iceberg::TableMetadata>> Metadata(bool kv)
{
    const TSchemaSpec schema{{{"key", EFieldType::String}}};
    ICEBERG_ASSIGN_OR_RAISE(auto profile, kv ? MakeKVTableProfile(schema) : MakeTableProfile(schema));
    ICEBERG_ASSIGN_OR_RAISE(auto metadata,
        iceberg::TableMetadata::Make(*profile.Schema,
            *iceberg::PartitionSpec::Unpartitioned(),
            *iceberg::SortOrder::Unsorted(),
            Location,
            profile.Properties));
    return std::shared_ptr<iceberg::TableMetadata>(std::move(metadata));
}

TEST(KVRetention, AppendPolicyProfileStillAcceptsRetentionExtensions)
{
    auto metadata = Metadata(false);
    ASSERT_TRUE(metadata);
    ASSERT_TRUE(WriteRetentionPolicy(**metadata, {1, 0}, 0));
    const auto schema = (*metadata)->Schema();
    ASSERT_TRUE(schema);
    EXPECT_TRUE(CheckTableProfile({*schema, (*metadata)->properties.configs()}));
    const auto policy = ReadRetentionPolicy(**metadata);
    ASSERT_TRUE(policy);
    EXPECT_EQ(policy->KeepLast, 1);
    EXPECT_EQ(*ExpiredSequence(**metadata), 0);
    EXPECT_EQ(*ExpirationPrefix(**metadata, *policy, 100), 0U);
}

TEST(KVRetention, RejectsEveryRetentionPropertyOnKVProfilesIncludingZeroWatermark)
{
    auto metadata = Metadata(true);
    ASSERT_TRUE(metadata);
    const auto schema = (*metadata)->Schema();
    ASSERT_TRUE(schema);
    for (const std::string key :
        {RetentionVersionKey, RetentionKeepKey, RetentionAgeKey, ExpiredSequenceKey, "glamdb.retention.future"})
    {
        auto properties = (*metadata)->properties.configs();
        properties[key] = key == ExpiredSequenceKey || key == RetentionAgeKey ? "0" : "1";
        EXPECT_FALSE(CheckTableProfile({*schema, properties})) << key;
        (*metadata)->properties = iceberg::TableProperties::FromMap(properties);
        EXPECT_FALSE(ExpiredSequence(**metadata)) << key;
        auto stripped = properties;
        EXPECT_FALSE(StripRetentionProperties(stripped)) << key;
        properties.erase(key);
        (*metadata)->properties = iceberg::TableProperties::FromMap(std::move(properties));
    }
    EXPECT_TRUE(CheckTableProfile({*schema, (*metadata)->properties.configs()}));
    EXPECT_EQ(*ExpiredSequence(**metadata), 0);
}

TEST(KVRetention, RefusesPolicySelectionAndRetirementBeforeAnyFileIO)
{
    auto metadata = Metadata(true);
    ASSERT_TRUE(metadata);
    const auto properties = (*metadata)->properties.configs();
    EXPECT_FALSE(ReadRetentionPolicy(**metadata));
    EXPECT_FALSE(WriteRetentionPolicy(**metadata, {1, 0}, 0));
    EXPECT_EQ((*metadata)->properties.configs(), properties);
    EXPECT_FALSE(ExpirationPrefix(**metadata, {}, 100));
    EXPECT_FALSE(ExpirationPrefix(**metadata, {1, 0}, 100));
    auto io = std::make_shared<glamdb::test::TMemoryFileIO>();
    std::size_t reads = 0;
    io->OnRead = [&](const auto&) {
        ++reads;
    };
    EXPECT_FALSE(FindExpiredMetadata(io, **metadata, 0, {}));
    EXPECT_FALSE(FindExpiredMetadata(io, **metadata, 1, {}));
    EXPECT_EQ(reads, 0U);
    EXPECT_TRUE(io->Files.empty());
}

TEST(KVRetention, RecoveryRejectsExpiredLedgerAndPolicyBeforeReadingManifests)
{
    auto metadata = Metadata(true);
    ASSERT_TRUE(metadata);
    auto io = std::make_shared<glamdb::test::TMemoryFileIO>();
    const auto root = Location + "/metadata/00000-33333333-3333-7333-8333-333333333333.metadata.json";
    ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*io, root, **metadata));
    std::size_t manifestReads = 0;
    io->OnRead = [&](const std::string& path) {
        if (path.ends_with(".avro")) {
            ++manifestReads;
        }
    };
    auto expired = std::make_shared<iceberg::Snapshot>();
    expired->snapshot_id = 7;
    expired->sequence_number = 1;
    const std::vector<std::shared_ptr<iceberg::Snapshot>> ledger{expired};
    const auto rejectedLedger = VerifyReachable(io, root, StoreID, CollectionID, {}, {}, ledger);
    ASSERT_FALSE(rejectedLedger);
    EXPECT_NE(rejectedLedger.error().message.find("KV"), std::string::npos);
    auto properties = (*metadata)->properties.configs();
    properties[RetentionVersionKey] = "1";
    properties[ExpiredSequenceKey] = "0";
    (*metadata)->properties = iceberg::TableProperties::FromMap(std::move(properties));
    ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*io, root, **metadata));
    EXPECT_FALSE(VerifyKVReachable(io, root, StoreID, CollectionID));
    EXPECT_FALSE(VerifyReachable(io, root, StoreID, CollectionID));
    EXPECT_EQ(manifestReads, 0U);
}

TEST(KVRetention, ClosureRejectsAnExpiredKVGenerationBeforeFileIO)
{
    auto metadata = Metadata(true);
    ASSERT_TRUE(metadata);
    auto index = std::make_shared<TScanMembership>();
    index->KeyValue = true;
    TBaseState base;
    base.Metadata = *metadata;
    base.ScanIndex = index;
    base.ExpiredSnapshotIDs = {7};
    auto candidate = base;
    auto io = std::make_shared<glamdb::test::TMemoryFileIO>();
    std::size_t reads = 0;
    io->OnRead = [&](const auto&) {
        ++reads;
    };
    EXPECT_FALSE(CheckKVClosure(io, std::move(candidate), base, {}, {}));
    EXPECT_EQ(reads, 0U);
}

} // namespace
} // namespace NGlamDB::NStorage
