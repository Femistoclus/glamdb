#pragma once

#include "core/storage/base_state.h"

#include <iceberg/result.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace iceberg {
class FileIO;
struct ManifestFile;
struct Snapshot;
}

namespace NGlamDB::NStorage {

// The Iceberg root keeps the newest `hot_snapshots` snapshots. An append moves
// the oldest root snapshots to the catalog archive once the root would hold
// more than hot + slack, so one cut serves slack + 1 appends.
std::size_t ArchiveSlack(std::int64_t hot);
// Bounds the work of one publication. A longer backlog shrinks by up to this
// many snapshots per append, or by explicit archive maintenance.
inline constexpr std::size_t MaxArchiveCutPerAppend = 64;
inline constexpr std::size_t MaxArchivePerMaintenance = 1024;
// Oldest root snapshots an append moves to the archive when the root after
// the append holds `rootSnapshots`; zero without a setting.
std::size_t AppendArchiveCut(std::size_t rootSnapshots, std::optional<std::int64_t> hot);
// Oldest root snapshots archive maintenance moves: down to `hot`, bounded.
std::size_t MaintenanceArchiveCut(std::size_t rootSnapshots, std::optional<std::int64_t> hot);

// Bytes, digest and manifests of one manifest list, decoded from the same
// bytes that were hashed.
struct TVerifiedManifestList
{
    std::uint64_t Bytes = 0;
    std::string Sha256;
    std::vector<iceberg::ManifestFile> Manifests;
};

// Reads a committed manifest list within the per-file and inventory limits.
// With an expected identity, a different size or digest is corruption.
iceberg::Result<TVerifiedManifestList> ReadManifestListIdentity(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& path,
    const TStorageLimits& limits,
    std::optional<std::uint64_t> expectedBytes = std::nullopt,
    std::string_view expectedSha256 = {});

// One root snapshot moved to the archive with what reading and verifying it
// without the root requires.
struct TArchivedSnapshot
{
    std::shared_ptr<const iceberg::Snapshot> Snapshot;
    std::uint64_t ManifestListBytes = 0;
    std::string ManifestListSha256;
    // Live data files: the snapshot's prefix of the chronological membership.
    std::uint64_t DataFiles = 0;
    // Manifests whose last referencing snapshot is this one. They remain on
    // disk and keep counting against the store budgets while archived.
    std::uint64_t RetiredManifests = 0;
    std::uint64_t RetiredManifestBytes = 0;
};

struct TArchiveCut
{
    std::vector<TArchivedSnapshot> Snapshots;
    // Sorted manifests no root snapshot references after the cut.
    std::vector<std::string> RetiredPaths;
};

// Describes moving `cut` (the oldest root snapshots, oldest first) out of the
// root. `next` is the oldest snapshot that stays in the root; `index` is the
// verified membership that still holds the boundaries of `cut`. Reads each
// list of `cut` and of `next` once. Manifests are referenced by a contiguous
// snapshot range in the verified append profile, so a manifest of a cut
// snapshot that `next` lacks is referenced by no later snapshot.
iceberg::Result<TArchiveCut> DescribeArchiveCut(const std::shared_ptr<iceberg::FileIO>& fileIO,
    std::span<const std::shared_ptr<iceberg::Snapshot>> cut,
    const iceberg::Snapshot& next,
    const TScanMembership& index,
    std::string_view tableLocation,
    const TStorageLimits& limits);

// Removes the archived boundaries and retired manifests from a verified state.
void ApplyArchiveCut(TBaseState& state, const TArchiveCut& cut);

// Archived (or expiring archived) snapshot identity recorded by the catalog.
struct TArchivedList
{
    std::int64_t SnapshotID = 0;
    std::int64_t Sequence = 0;
    std::string Path;
    std::uint64_t Bytes = 0;
    std::string Sha256;
};

}
