#include "core/storage/metadata_validation.h"
#include "core/storage/table_profile.h"

#include <gtest/gtest.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>

#include <functional>
#include <limits>
#include <ranges>

namespace NGlamDB::NStorage {
namespace {

class TMetadataValidationTest : public ::testing::Test
{
protected:
    TTableProfile Profile_;
    std::unique_ptr<iceberg::TableMetadata> Base_;
    const std::string Location_ =
        "store://11111111-1111-4111-8111-111111111111/warehouse/22222222-2222-4222-8222-222222222222";
    const std::unordered_map<std::string, std::string> Summary_{{"operation-id", "op"}, {"payload-hash", "hash"}};

    void SetUp() override
    {
        auto profile = MakeTableProfile({{{"key", EFieldType::String}}});
        ASSERT_TRUE(profile);
        Profile_ = std::move(*profile);
        auto base = iceberg::TableMetadata::Make(*Profile_.Schema,
            *iceberg::PartitionSpec::Unpartitioned(),
            *iceberg::SortOrder::Unsorted(),
            Location_,
            Profile_.Properties);
        ASSERT_TRUE(base);
        Base_ = std::move(*base);
    }

    iceberg::TableMetadata Append(const iceberg::TableMetadata& base, std::int64_t id) const
    {
        auto next = base;
        next.current_snapshot_id = id;
        ++next.last_sequence_number;
        next.last_updated_ms += std::chrono::milliseconds(1);
        auto snapshot = std::make_shared<iceberg::Snapshot>();
        snapshot->snapshot_id = id;
        if (!base.snapshots.empty()) {
            snapshot->parent_snapshot_id = base.current_snapshot_id;
        }
        snapshot->sequence_number = next.last_sequence_number;
        snapshot->timestamp_ms = next.last_updated_ms;
        snapshot->manifest_list = "list.avro";
        snapshot->schema_id = base.current_schema_id;
        snapshot->summary = Summary_;
        snapshot->summary.emplace("operation", "append");
        next.snapshots.push_back(std::move(snapshot));
        next.snapshot_log.push_back({next.last_updated_ms, id});
        next.refs["main"] = std::make_shared<iceberg::SnapshotRef>(
            iceberg::SnapshotRef{.snapshot_id = id, .retention = iceberg::SnapshotRef::Branch{}});
        return next;
    }
};

TEST_F(TMetadataValidationTest, AcceptsFreshProfileAndOnlyAppendChanges)
{
    ASSERT_TRUE(CheckCreateMetadata(*Base_, Profile_, Location_));
    auto first = Append(*Base_, 71);
    ASSERT_TRUE(CheckAppendMetadata(*Base_, first, Summary_));
    auto second = Append(first, 17);
    EXPECT_TRUE(CheckAppendMetadata(first, second, Summary_));
    EXPECT_FALSE(CheckAppendMetadata(*Base_, second, Summary_));
    EXPECT_FALSE(CheckCreateMetadata(first, Profile_, Location_));
}

TEST_F(TMetadataValidationTest, AcceptsIndependentMetadataAndSnapshotTimes)
{
    auto first = Append(*Base_, 71);
    first.last_updated_ms += std::chrono::milliseconds(50);
    ASSERT_NE(first.last_updated_ms, first.snapshots.back()->timestamp_ms);
    ASSERT_EQ(first.snapshot_log.back().timestamp_ms, first.snapshots.back()->timestamp_ms);
    ASSERT_TRUE(CheckAppendMetadata(*Base_, first, Summary_));

    auto second = Append(first, 17);
    second.last_updated_ms -= std::chrono::milliseconds(100);
    ASSERT_LT(second.last_updated_ms, second.snapshots.back()->timestamp_ms);
    EXPECT_TRUE(CheckAppendMetadata(first, second, Summary_));
}

TEST_F(TMetadataValidationTest, RejectsChangedSnapshotLogTimes)
{
    auto first = Append(*Base_, 71);
    first.last_updated_ms += std::chrono::milliseconds(50);
    auto second = Append(first, 17);
    second.last_updated_ms += std::chrono::milliseconds(50);
    second.snapshot_log.back().timestamp_ms = second.last_updated_ms;
    EXPECT_FALSE(CheckAppendMetadata(first, second, Summary_));
    second.snapshot_log.back().timestamp_ms = second.snapshots.back()->timestamp_ms;
    ASSERT_TRUE(CheckAppendMetadata(first, second, Summary_));
    second.snapshot_log.front().timestamp_ms += std::chrono::milliseconds(1);
    EXPECT_FALSE(CheckAppendMetadata(first, second, Summary_));
}

TEST_F(TMetadataValidationTest, RejectsChangesOutsideTheAppendDelta)
{
    using TMutator = std::function<void(iceberg::TableMetadata*)>;
    const std::vector<TMutator> changes{
        [](auto* item) {
            item->format_version = 3;
        },
        [](auto* item) {
            item->table_uuid = "33333333-3333-4333-8333-333333333333";
        },
        [](auto* item) {
            item->location += "other";
        },
        [](auto* item) {
            ++item->last_column_id;
        },
        [](auto* item) {
            item->schemas.clear();
        },
        [](auto* item) {
            item->schemas.push_back(nullptr);
        },
        [](auto* item) {
            ++item->current_schema_id;
        },
        [](auto* item) {
            item->partition_specs.clear();
        },
        [](auto* item) {
            ++item->default_spec_id;
        },
        [](auto* item) {
            ++item->last_partition_id;
        },
        [](auto* item) {
            item->properties = iceberg::TableProperties::FromMap({});
        },
        [](auto* item) {
            item->sort_orders.clear();
        },
        [](auto* item) {
            ++item->default_sort_order_id;
        },
        [](auto* item) {
            ++item->next_row_id;
        },
        [](auto* item) {
            item->metadata_log.push_back({item->last_updated_ms, "old.json"});
        },
        [](auto* item) {
            item->refs.emplace("other", item->refs.at("main"));
        },
        [](auto* item) {
            item->refs.at("main")->retention = iceberg::SnapshotRef::Tag{};
        },
        [](auto* item) {
            item->snapshot_log.back().snapshot_id = 12;
        },
        [](auto* item) {
            item->snapshots.back()->parent_snapshot_id = 12;
        },
        [](auto* item) {
            item->snapshots.back()->summary.erase("payload-hash");
        },
        [](auto* item) {
            item->snapshots.back()->summary["operation"] = "overwrite";
        },
        [](auto* item) {
            item->snapshots.back()->first_row_id = 1;
        },
        [](auto* item) {
            item->snapshots.back()->manifest_list.clear();
        },
        [](auto* item) {
            item->snapshots.back()->schema_id.reset();
        },
        [](auto* item) {
            item->snapshots.back()->snapshot_id = -1;
            item->current_snapshot_id = -1;
            item->snapshot_log.back().snapshot_id = -1;
            item->refs.at("main")->snapshot_id = -1;
        },
        [](auto* item) {
            auto fields = item->schemas.front()->fields() | std::ranges::to<std::vector>();
            fields.front() = fields.front().WithDoc("changed documentation");
            item->schemas.front() = std::make_shared<iceberg::Schema>(std::move(fields), 0);
        },
    };
    for (std::size_t index = 0; index < changes.size(); ++index) {
        SCOPED_TRACE(index);
        auto candidate = Append(*Base_, 71);
        changes[index](&candidate);
        EXPECT_FALSE(CheckAppendMetadata(*Base_, candidate, Summary_));
    }
}

TEST_F(TMetadataValidationTest, PreservesPreviousSnapshotsAndHandlesSequenceOverflow)
{
    auto first = Append(*Base_, 71);
    auto second = Append(first, 17);
    second.snapshots.front() = std::make_shared<iceberg::Snapshot>(*first.snapshots.front());
    second.snapshots.front()->summary["extra"] = "mutation";
    EXPECT_FALSE(CheckAppendMetadata(first, second, Summary_));
    second.snapshots.front()->summary = first.snapshots.front()->summary;
    second.snapshots.front()->manifest_list = "replaced.avro";
    EXPECT_FALSE(CheckAppendMetadata(first, second, Summary_));
    EXPECT_FALSE(CheckAppendMetadata(first, Append(first, 71), Summary_));
    first.last_sequence_number = std::numeric_limits<std::int64_t>::max();
    EXPECT_FALSE(CheckAppendMetadata(first, second, Summary_));
}

TEST_F(TMetadataValidationTest, CreateRejectsForeignLocationProfileAndInvalidUuid)
{
    EXPECT_FALSE(CheckCreateMetadata(*Base_, Profile_, Location_ + "/other"));
    auto different = MakeTableProfile({{{"value", EFieldType::Int64}}});
    ASSERT_TRUE(different);
    EXPECT_FALSE(CheckCreateMetadata(*Base_, *different, Location_));
    Base_->table_uuid = "invalid";
    EXPECT_FALSE(CheckCreateMetadata(*Base_, Profile_, Location_));
}

TEST(ManifestEquality, ComparesEveryFieldRatherThanOnlyThePath)
{
    iceberg::ManifestFile left;
    left.manifest_path = "same.avro";
    auto right = left;
    EXPECT_TRUE(EqualManifest(left, right));
    right.manifest_length = 1;
    EXPECT_FALSE(EqualManifest(left, right));
    right = left;
    right.added_rows_count = 0;
    EXPECT_FALSE(EqualManifest(left, right));
    right = left;
    right.min_sequence_number = 1;
    EXPECT_FALSE(EqualManifest(left, right));
    right = left;
    right.key_metadata.push_back(1);
    EXPECT_FALSE(EqualManifest(left, right));
}

}
}
