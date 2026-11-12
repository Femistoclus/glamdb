#pragma once

#include "core/storage/capacity.h"
#include "core/storage/scan_index.h"

#include <iceberg/manifest/manifest_list.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace iceberg {
class FileIO;
struct TableMetadata;
struct Snapshot;
}

namespace NGlamDB::NIO {
class TIOAttempt;
}

namespace NGlamDB::NStorage {

struct TBaseState
{
    std::string MetadataLocation;
    std::shared_ptr<const iceberg::TableMetadata> Metadata;
    std::vector<iceberg::ManifestFile> CurrentManifests;
    TStorageUsage Usage;
    std::shared_ptr<const TScanMembership> ScanIndex{};
    TSortedSnapshotIDs ExpiredSnapshotIDs{};
    // Sorted union across every retained snapshot, including merged-out inputs.
    TManifestPathSet RetainedManifestPaths{};
    // The latest data publication's list and manifest inputs/outputs, excluding
    // the root. Maintenance preserves this conservative recovery byte charge.
    std::uint64_t CurrentCommitMetadataBytes = 0;
    // Decoded Avro bytes of each current manifest (CurrentManifests order) as
    // the verifier measured them; they size the next manifest merge. Empty
    // when a current manifest was not measured.
    std::vector<std::uint64_t> CurrentManifestDecodedBytes{};
};

iceberg::Result<TBaseState> CheckAppendClosure(const std::shared_ptr<iceberg::FileIO>& fileIO,
    TBaseState candidate,
    const std::vector<iceberg::ManifestFile>& previousManifests,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const TStorageLimits& limits);

// Verifies one retained append without walking the whole history: the root
// parses with the supported profile, `snapshotID` is retained, its transition
// from the parent snapshot is a valid append, and its manifest list, new
// manifests and data files are intact, including exact preservation of rewritten
// parent entries. Returns the root state (usage covers the root only).
iceberg::Result<TBaseState> VerifyRetainedSnapshot(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    std::int64_t snapshotID,
    const TStorageLimits& limits = {});

// What a verified checkpoint (catalog version 6) asserts about an append
// collection whose every publication was verified by the publication guard.
struct TCheckpointedState
{
    std::string RootLocation;
    std::uint64_t RootBytes = 0;
    std::string RootSha256;
    std::optional<std::int64_t> SnapshotID;
    std::int64_t Sequence = 0;
    // Cumulative usage, including archived metadata still on disk.
    TStorageUsage Usage;
    // The ancestry before the oldest root snapshot: the expiration ledger
    // (`ExpiredIDs`, any order) followed by `Archived` archived snapshots.
    std::vector<std::int64_t> ExpiredIDs;
    std::uint64_t Archived = 0;
    // The snapshot with sequence |ExpiredIDs| + Archived (the oldest root
    // snapshot's parent), and its live data files when it is archived.
    std::optional<std::int64_t> PrefixParent;
    std::optional<std::uint64_t> PrefixDataFiles;
};

// Rebuilds the state VerifyReachable proves, from the checkpoint, the current
// root (size and SHA-256 must match), the manifest lists of the root
// snapshots and the current manifests. Reads no data-file footer, no archived
// or expired metadata and no superseded manifest. The scan index holds the
// same files in the same order: list order is preserved per origin by every
// append and merge. A mismatch is an error; callers fall back to full
// verification.
iceberg::Result<TBaseState> RebuildFromCheckpoint(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& storeID,
    const std::string& collectionID,
    const TCheckpointedState& expected,
    const TStorageLimits& limits);

// `expired` is the ancestry before the oldest root snapshot, oldest first:
// the expiration ledger, then the snapshot archive. Its first `expiredCount`
// entries (all when absent) are expired; the rest stay readable.
iceberg::Result<TBaseState> VerifyReachable(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    const TStorageLimits& limits = {},
    const std::string& workspaceDirectory = {},
    std::span<const std::shared_ptr<iceberg::Snapshot>> expired = {},
    std::optional<std::size_t> expiredCount = std::nullopt);

}
