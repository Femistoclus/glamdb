#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace NGlamDB::NStorage {

struct TScanDataFile
{
    std::string Path;
    std::int64_t SizeBytes = 0;
    std::int64_t RecordCount = 0;

    bool operator==(const TScanDataFile&) const = default;
};

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
