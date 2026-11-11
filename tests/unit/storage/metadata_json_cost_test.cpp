// Table metadata serialization must stay linear in retained snapshots: every
// append writes a new root through TableMetadataUtil::Write. The pinned
// iceberg-cpp ToJsonList deep-copied the accumulated array per element (see
// cmake/dependencies/patches/iceberg-cpp); doubling the history then
// quadrupled the time instead of doubling it.

#include "tests/support/memory_file_io.h"

#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>

namespace glamdb {
namespace {

iceberg::TableMetadata MetadataWithSnapshots(int count)
{
    iceberg::TableMetadata metadata;
    metadata.format_version = 2;
    metadata.table_uuid = "01a1c818-40f3-760c-8139-973d9b81c9a0";
    metadata.location = "store://00000000-0000-0000-0000-000000000001/warehouse/00112233-4455-6677-8899-aabbccddeeff";
    metadata.last_sequence_number = count;
    std::optional<std::int64_t> parent;
    for (int index = 0; index < count; ++index) {
        auto snapshot = std::make_shared<iceberg::Snapshot>();
        snapshot->snapshot_id = 1000000000000LL + index;
        snapshot->parent_snapshot_id = parent;
        snapshot->sequence_number = index + 1;
        snapshot->timestamp_ms = iceberg::TimePointMsFromUnixMs(1700000000000LL + index);
        snapshot->manifest_list = metadata.location + "/metadata/snap-" + std::to_string(snapshot->snapshot_id) +
            "-0-01a1c818-40f3-760c-8139-973d9b81c9a0.avro";
        snapshot->schema_id = 0;
        snapshot->summary = {{"operation", "append"},
            {"glamdb.operation-id", "00000000-0000-4000-8000-" + std::to_string(100000000000LL + index)},
            {"glamdb.content-hash", std::string(64, 'a')},
            {"added-data-files", "1"},
            {"added-records", "1"},
            {"added-files-size", "1542"},
            {"changed-partition-count", "1"},
            {"total-records", std::to_string(index + 1)},
            {"total-files-size", std::to_string(1542 * (index + 1))},
            {"total-data-files", std::to_string(index + 1)},
            {"total-delete-files", "0"},
            {"total-position-deletes", "0"},
            {"total-equality-deletes", "0"},
            {"engine-name", "glamdb"},
            {"iceberg-version", "0.4.0"},
            {"engine-version", "0.1.0"}};
        metadata.snapshots.push_back(snapshot);
        metadata.snapshot_log.push_back({snapshot->timestamp_ms, snapshot->snapshot_id});
        parent = snapshot->snapshot_id;
    }
    metadata.current_snapshot_id = count > 0 ? *parent : iceberg::kInvalidSnapshotId;
    return metadata;
}

double WriteMs(int count, std::size_t* bytes)
{
    const auto metadata = MetadataWithSnapshots(count);
    double best = 1e18;
    for (int repeat = 0; repeat < 3; ++repeat) {
        test::TMemoryFileIO io;
        const auto start = std::chrono::steady_clock::now();
        const auto written = iceberg::TableMetadataUtil::Write(io, "memory://root.metadata.json", metadata);
        const auto elapsed = std::chrono::steady_clock::now() - start;
        EXPECT_TRUE(written) << written.error().message;
        *bytes = io.Files.begin()->second.size();
        best = std::min(best, std::chrono::duration<double, std::milli>(elapsed).count());
    }
    return best;
}

TEST(IcebergMetadataJsonCost, SerializationIsLinearInRetainedSnapshots)
{
    std::size_t smallBytes = 0;
    std::size_t largeBytes = 0;
    const double small = WriteMs(500, &smallBytes);
    const double large = WriteMs(1000, &largeBytes);
    std::printf("metadata json n=500 bytes=%zu ms=%.1f | n=1000 bytes=%zu ms=%.1f | ratio=%.2f | us/KB(1000)=%.1f\n",
        smallBytes,
        small,
        largeBytes,
        large,
        large / small,
        1000.0 * large / (largeBytes / 1024.0));
    // The output is linear in n (bytes ratio ~2); linear serialization keeps the time ratio near 2.
    EXPECT_LT(large / small, 3.0) << "doubling the history must not quadruple root serialization time";
}

} // namespace
} // namespace glamdb
