#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace iceberg {
struct DataFile;
}

namespace NGlamDB::NStorage {

struct TScanDataFile
{
    std::string Path;
    std::int64_t SizeBytes = 0;
    std::int64_t RecordCount = 0;
    std::shared_ptr<const iceberg::DataFile> DataFile{};
    std::int64_t IntroducedSequence = 0;
    std::optional<std::int64_t> RemovedSequence{};

    bool operator==(const TScanDataFile& other) const
    {
        return Path == other.Path && SizeBytes == other.SizeBytes && RecordCount == other.RecordCount;
    }
};

struct TSnapshotBoundary
{
    std::int64_t SnapshotID;
    std::size_t FileCount;
    std::int64_t SequenceNumber = 0;
};

// Built only by strict closure verification. Append snapshots select a
// chronological file prefix. KeyValue snapshots select file lifetimes by their
// sequence number. Boundaries are sorted by opaque snapshot ID for lookup only.
// Both profiles require one fixed-schema, unpartitioned, linear history; files
// have fresh paths and cannot be resurrected.
struct TScanMembership
{
    std::vector<TScanDataFile> Files;
    std::vector<TSnapshotBoundary> Snapshots;
    bool KeyValue = false;
};

} // namespace NGlamDB::NStorage
