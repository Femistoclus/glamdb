// Allocation failure on the scan path is resource pressure: it is reported as
// RESOURCE_EXHAUSTED, never as CORRUPTION that would close readiness.

#include "core/engine/storage_mapping.h"
#include "core/scan/scan_cursor.h"
#include "core/scan/scan_planner.h"
#include "core/storage/table_profile.h"
#include "tests/support/memory_file_io.h"

#include <gtest/gtest.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>

#include <new>

namespace glamdb::detail {
namespace {

constexpr char StoreId[] = "00000000-0000-4000-8000-000000000001";
constexpr char CollectionId[] = "00000000-0000-4000-8000-000000000011";

TEST(ScanResourceClassification, AllocationFailureReadingCommittedRootIsNotCorruption)
{
    const SchemaSpec schema{{{"key", FieldType::String, false}, {"count", FieldType::Int64, true}}};
    const std::string table = std::string("store://") + StoreId + "/warehouse/" + CollectionId;
    const std::string root = table + "/metadata/00000-a0.metadata.json";

    auto io = std::make_shared<test::TMemoryFileIO>();
    auto profile = NGlamDB::NStorage::MakeTableProfile(ToStorageSchema(schema));
    ASSERT_TRUE(profile.has_value());
    auto metadata = iceberg::TableMetadata::Make(*profile->Schema,
        *iceberg::PartitionSpec::Unpartitioned(),
        *iceberg::SortOrder::Unsorted(),
        table,
        profile->Properties);
    ASSERT_TRUE(metadata.has_value());
    ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*io, root, **metadata).has_value());
    const BaseIdentity base{StoreId, CollectionId, (*metadata)->table_uuid, root, std::nullopt, 0, 0};

    // Control: the committed initial root plans to the empty view.
    const auto control = PlanScan(io, base, SnapshotSelector::Latest());
    ASSERT_TRUE(control) << control.error().code << ": " << control.error().message;
    EXPECT_FALSE(control->SnapshotID);

    // Simulates std::bad_alloc anywhere inside ReadTableMetadata (e.g. the bounded
    // std::string read buffer or JSON parsing of a multi-MiB root).
    const auto failAllocation = [&](const std::string& path) {
        if (path == root) {
            throw std::bad_alloc();
        }
    };

    io->OnRead = failAllocation;
    const auto planned = PlanScan(io, base, SnapshotSelector::Latest());
    io->OnRead = nullptr;
    ASSERT_FALSE(planned);
    EXPECT_NE(planned.error().code, "CORRUPTION") << planned.error().message;
    EXPECT_EQ(planned.error().code, "RESOURCE_EXHAUSTED") << planned.error().message;

    // OpenScan surfaces the same code; Engine::Scan calls CloseOnCorruption on it
    // (core/engine/engine.cpp:1147-1150), so CORRUPTION here closes readiness.
    TScanContext context{.Collection = {CollectionId, "events", (*metadata)->table_uuid, schema, 1, std::nullopt},
        .Base = base,
        .FileIO = io,
        .Closing = nullptr,
        .Lease = nullptr,
        .OnError = {}};
    io->OnRead = failAllocation;
    const auto cursor = OpenScan(std::move(context), ScanRequest{.collection_id = CollectionId});
    io->OnRead = nullptr;
    ASSERT_FALSE(cursor);
    EXPECT_NE(cursor.error().code, "CORRUPTION") << cursor.error().message;
}

} // namespace
} // namespace glamdb::detail
