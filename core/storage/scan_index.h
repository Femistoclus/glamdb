#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace iceberg {
struct DataFile;
} // namespace iceberg

namespace NGlamDB::NStorage {

struct TScanDataFile
{
    std::string Path;
    std::int64_t SizeBytes = 0;
    std::int64_t RecordCount = 0;
    // The manifest entry's column metrics (EncodeScanMetrics), for pruning.
    std::string Metrics{};

    bool operator==(const TScanDataFile&) const = default;
};

// One field's manifest metrics. Bounds are Iceberg single-value encodings
// (string bounds may be truncated) and view the encoded descriptor.
struct TScanFieldMetrics
{
    std::optional<std::int64_t> NullCount{};
    std::optional<std::int64_t> NanCount{};
    std::optional<std::string_view> Lower{};
    std::optional<std::string_view> Upper{};
};

// Encodes null/NaN counts and bounds of every field in one compact string
// (about 5 + 8 per count + 4 + bound bytes per field). A field whose value
// count differs from the record count is omitted, so nothing is concluded
// from inconsistent metrics.
std::string EncodeScanMetrics(const iceberg::DataFile& file);

// nullopt when the field has no metrics or the encoding is malformed.
std::optional<TScanFieldMetrics> FindScanFieldMetrics(std::string_view metrics, std::int32_t fieldID);

struct TSnapshotBoundary
{
    std::int64_t SnapshotID;
    std::size_t FileCount;
};

// Built only by strict append-closure verification. Files are chronological;
// each retained snapshot selects a prefix. Boundaries are sorted by snapshot
// ID (IDs themselves are opaque). This representation requires the fixed-schema,
// unpartitioned, append-only profile: no deletes, branches or compaction.
struct TScanMembership
{
    std::vector<TScanDataFile> Files;
    std::vector<TSnapshotBoundary> Snapshots;
};

} // namespace NGlamDB::NStorage
