#include "core/storage/snapshot_retention.h"

#include <gtest/gtest.h>
#include <iceberg/arrow/arrow_io_util.h>
#include <iceberg/file_io.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

namespace NGlamDB::NStorage {
namespace {

iceberg::TableMetadata History()
{
    iceberg::TableMetadata metadata;
    for (const auto& [id, time] : {std::pair{900, 10}, std::pair{2, 90}, std::pair{500, 100}}) {
        auto snapshot = std::make_shared<iceberg::Snapshot>();
        snapshot->snapshot_id = id;
        snapshot->timestamp_ms = iceberg::TimePointMs(std::chrono::milliseconds(time));
        metadata.snapshots.push_back(std::move(snapshot));
    }
    return metadata;
}

TEST(SnapshotRetentionPolicy, RetainsUnionOfCountAndMinimumAgeWithExactBoundary)
{
    const auto metadata = History();
    EXPECT_EQ(*ExpirationPrefix(metadata, {1, 0}, 100), 2U);
    EXPECT_EQ(*ExpirationPrefix(metadata, {2, 0}, 100), 1U);
    EXPECT_EQ(*ExpirationPrefix(metadata, {1, 11}, 100), 1U);
    EXPECT_EQ(*ExpirationPrefix(metadata, {1, 10}, 100), 2U);
    EXPECT_EQ(*ExpirationPrefix(metadata, {std::nullopt, 11}, 100), 1U);
    EXPECT_EQ(*ExpirationPrefix(metadata, {}, 100), 0U);
}

TEST(SnapshotRetentionPolicy, FutureTimestampStopsPrefixWithoutExpiringCurrent)
{
    auto metadata = History();
    metadata.snapshots[1]->timestamp_ms = iceberg::TimePointMs(std::chrono::milliseconds(200));
    EXPECT_EQ(*ExpirationPrefix(metadata, {1, 1}, 100), 1U);
    EXPECT_EQ(*ExpirationPrefix(metadata, {4096, 0}, 100), 0U);
    EXPECT_TRUE(CheckRetentionPolicy({1, 253402300799999LL}));
    EXPECT_FALSE(CheckRetentionPolicy({0, 0}));
    EXPECT_TRUE(CheckRetentionPolicy({4097, 0}));
    EXPECT_FALSE(CheckRetentionPolicy({(1LL << 40) + 1, 0}));
    EXPECT_FALSE(CheckRetentionPolicy({1, -1}));
    EXPECT_FALSE(CheckRetentionPolicy({1, 253402300800000LL}));
}

TEST(SnapshotRetentionPolicy, RejectsUnversionedAndNoncanonicalPersistentProperties)
{
    for (const std::string value : {"01", "+1", "-1", "9223372036854775808"}) {
        std::unordered_map<std::string, std::string> properties{{RetentionVersionKey, "1"}, {RetentionKeepKey, value}};
        EXPECT_FALSE(StripRetentionProperties(properties));
    }
    std::unordered_map<std::string, std::string> properties{{RetentionKeepKey, "1"}};
    EXPECT_FALSE(StripRetentionProperties(properties));
}

TEST(SnapshotRetentionPolicy, BoundsAvroRecordsBeforeOpeningTheManifestListReader)
{
    std::shared_ptr<iceberg::FileIO> io = iceberg::arrow::MakeMockFileIO();
    const auto marker = std::string(16, 's');
    // One schema property, then a block claiming two records without any payload.
    // The container guard must reject the record count before schema materialization.
    const auto bytes = std::string("Obj\1") + '\x02' + '\x16' + "avro.schema" + '\x0c' + "\"null\"" + '\0' + marker +
        '\x04' + '\0' + marker;
    ASSERT_TRUE(io->WriteFile("retired.avro", bytes));
    auto metadata = History();
    for (const auto& snapshot : metadata.snapshots) {
        snapshot->manifest_list = "retired.avro";
    }
    TStorageLimits limits;
    limits.Store.Manifests = 1;
    const auto result = FindExpiredMetadata(io, metadata, 1, limits);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().message, "Avro container exceeds its record limit.");
}

} // namespace
} // namespace NGlamDB::NStorage
