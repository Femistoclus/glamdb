#include "core/storage/snapshot_archive.h"

#include "core/io/sha256.h"
#include "core/io/store_uri.h"
#include "core/scan/buffer_file.h"
#include "core/storage/format_registry.h"
#include "core/storage/format_verifier.h"
#include "core/storage/iceberg_error_boundary.h"

#include <iceberg/file_io.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/snapshot.h>

#include <algorithm>
#include <limits>
#include <map>

namespace NGlamDB::NStorage {
using iceberg::Error;

std::size_t ArchiveSlack(std::int64_t hot)
{
    return hot <= 0 ? 0 : static_cast<std::size_t>(std::min<std::int64_t>(hot, 16));
}

std::size_t AppendArchiveCut(std::size_t rootSnapshots, std::optional<std::int64_t> hot)
{
    if (!hot || *hot <= 0) {
        return 0;
    }
    const auto keep = static_cast<std::uint64_t>(*hot);
    if (rootSnapshots <= keep || rootSnapshots - keep <= ArchiveSlack(*hot)) {
        return 0;
    }
    return std::min<std::size_t>(rootSnapshots - keep, MaxArchiveCutPerAppend);
}

std::size_t MaintenanceArchiveCut(std::size_t rootSnapshots, std::optional<std::int64_t> hot)
{
    if (!hot || *hot <= 0 || rootSnapshots <= static_cast<std::uint64_t>(*hot)) {
        return 0;
    }
    return std::min<std::size_t>(rootSnapshots - static_cast<std::size_t>(*hot), MaxArchivePerMaintenance);
}

iceberg::Result<TVerifiedManifestList> ReadManifestListIdentity(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& path,
    const TStorageLimits& limits,
    std::optional<std::uint64_t> expectedBytes,
    std::string_view expectedSha256)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<TVerifiedManifestList> {
        if (!fileIO ||
            limits.ManifestListBytes > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
            limits.Store.Manifests > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
        {
            return iceberg::InvalidArgument("Invalid manifest-list verification limits.");
        }
        RegisterStorageFormats();
        ICEBERG_ASSIGN_OR_RAISE(auto input, fileIO->NewInputFile(path));
        ICEBERG_ASSIGN_OR_RAISE(auto size, input->Size());
        if (size <= 0 || static_cast<std::uint64_t>(size) > limits.ManifestListBytes) {
            return iceberg::ValidationFailed("Invalid manifest-list bytes: {}.", path);
        }
        if (expectedBytes && *expectedBytes != static_cast<std::uint64_t>(size)) {
            return iceberg::Invalid("CORRUPTION: Archived manifest list size differs: {}.", path);
        }
        ICEBERG_ASSIGN_OR_RAISE(auto bytes, fileIO->ReadFile(path, static_cast<std::size_t>(size)));
        if (bytes.size() != static_cast<std::size_t>(size)) {
            return iceberg::Invalid("CORRUPTION: Manifest list length changed while reading: {}.", path);
        }
        TVerifiedManifestList result;
        result.Bytes = bytes.size();
        NIO::TSha256 hash;
        if (const auto error = hash.Update(std::as_bytes(std::span(bytes.data(), bytes.size())))) {
            return iceberg::IOError("Cannot hash manifest list: {}.", error.message());
        }
        result.Sha256 = NIO::HexDigest(hash.Finalize());
        if (!expectedSha256.empty() && result.Sha256 != expectedSha256) {
            return iceberg::Invalid("CORRUPTION: Archived manifest list digest differs: {}.", path);
        }
        const auto buffered = std::make_shared<glamdb::detail::TBufferFileIO>(path, std::move(bytes));
        ICEBERG_ASSIGN_OR_RAISE(auto bufferedInput, buffered->NewInputFile(path));
        ICEBERG_RETURN_UNEXPECTED(CheckAvroContainer(*bufferedInput,
            size,
            {static_cast<std::int64_t>(limits.ManifestListBytes),
                static_cast<std::int64_t>(limits.Store.Manifests),
                static_cast<std::int64_t>(limits.ManifestListBytes)}));
        ICEBERG_ASSIGN_OR_RAISE(auto reader, iceberg::ManifestListReader::Make(path, buffered));
        ICEBERG_ASSIGN_OR_RAISE(result.Manifests, reader->Files());
        if (result.Manifests.empty() || result.Manifests.size() > limits.Store.Manifests) {
            return iceberg::ValidationFailed("Manifest list inventory exceeds capacity: {}.", path);
        }
        return result;
    });
}

iceberg::Result<TArchiveCut> DescribeArchiveCut(const std::shared_ptr<iceberg::FileIO>& fileIO,
    std::span<const std::shared_ptr<iceberg::Snapshot>> cut,
    const iceberg::Snapshot& next,
    const TScanMembership& index,
    std::string_view tableLocation,
    const TStorageLimits& limits)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<TArchiveCut> {
        if (cut.empty() || tableLocation.size() < 44) {
            return iceberg::InvalidArgument("Invalid snapshot archive cut.");
        }
        const auto table = NIO::TStoreUri::ParseTable(tableLocation, tableLocation.substr(8, 36));
        if (!table) {
            return iceberg::ValidationFailed("Invalid archive table location.");
        }
        const auto checkList = [&](const std::string& path) -> iceberg::Status {
            const auto uri = NIO::TStoreUri::Parse(path, table.Value().StoreId());
            if (!uri || uri.Value().CollectionId() != table.Value().CollectionId() ||
                uri.Value().Kind() != NIO::EFileKind::ManifestList)
            {
                return iceberg::ValidationFailed("Invalid archived manifest-list path: {}.", path);
            }
            return {};
        };
        using TManifests = std::map<std::string, std::uint64_t, std::less<>>;
        const auto readManifests = [&](const std::string& path,
                                       TVerifiedManifestList& list) -> iceberg::Result<TManifests> {
            ICEBERG_RETURN_UNEXPECTED(checkList(path));
            ICEBERG_ASSIGN_OR_RAISE(list, ReadManifestListIdentity(fileIO, path, limits));
            TManifests manifests;
            for (const auto& manifest : list.Manifests) {
                if (manifest.manifest_length <= 0 ||
                    static_cast<std::uint64_t>(manifest.manifest_length) > limits.ManifestBytes ||
                    !manifests.emplace(manifest.manifest_path, static_cast<std::uint64_t>(manifest.manifest_length))
                         .second)
                {
                    return iceberg::ValidationFailed("Invalid archived manifest inventory: {}.", path);
                }
            }
            return manifests;
        };
        TArchiveCut result;
        result.Snapshots.reserve(cut.size());
        std::optional<std::int64_t> previous;
        TVerifiedManifestList list;
        ICEBERG_ASSIGN_OR_RAISE(auto current, readManifests(cut.front() ? cut.front()->manifest_list : "", list));
        for (std::size_t i = 0; i < cut.size(); ++i) {
            const auto& snapshot = cut[i];
            if (!snapshot || (previous && snapshot->parent_snapshot_id != previous)) {
                return iceberg::ValidationFailed("Archive cut is not one ancestry chain.");
            }
            previous = snapshot->snapshot_id;
            const auto boundary =
                std::ranges::lower_bound(index.Snapshots, snapshot->snapshot_id, {}, &TSnapshotBoundary::SnapshotID);
            if (boundary == index.Snapshots.end() || boundary->SnapshotID != snapshot->snapshot_id ||
                boundary->FileCount == 0 || boundary->FileCount > index.Files.size())
            {
                return iceberg::ValidationFailed("Archived snapshot has no verified membership boundary.");
            }
            std::uint64_t files = 0;
            for (const auto& manifest : list.Manifests) {
                const auto added = manifest.added_files_count.value_or(-1);
                const auto existing = manifest.existing_files_count.value_or(-1);
                if (added < 0 || existing < 0) {
                    return iceberg::ValidationFailed("Archived manifest has no file counters.");
                }
                files += static_cast<std::uint64_t>(added) + static_cast<std::uint64_t>(existing);
            }
            if (files != boundary->FileCount) {
                return iceberg::Invalid("CORRUPTION: Archived manifest list differs from its verified membership.");
            }
            TArchivedSnapshot archived{.Snapshot = snapshot,
                .ManifestListBytes = list.Bytes,
                .ManifestListSha256 = list.Sha256,
                .DataFiles = files};
            const auto& following = i + 1 < cut.size() ? cut[i + 1] : nullptr;
            if (i + 1 < cut.size() && !following) {
                return iceberg::ValidationFailed("Archive cut is not one ancestry chain.");
            }
            const auto& nextPath = following ? following->manifest_list : next.manifest_list;
            if (!following && next.parent_snapshot_id != snapshot->snapshot_id) {
                return iceberg::ValidationFailed("Archive cut is not followed by its child.");
            }
            ICEBERG_ASSIGN_OR_RAISE(auto later, readManifests(nextPath, list));
            for (const auto& [path, bytes] : current) {
                if (!later.contains(path)) {
                    archived.RetiredManifests += 1;
                    archived.RetiredManifestBytes += bytes;
                    result.RetiredPaths.push_back(path);
                }
            }
            result.Snapshots.push_back(std::move(archived));
            current = std::move(later);
        }
        std::ranges::sort(result.RetiredPaths);
        if (std::ranges::adjacent_find(result.RetiredPaths) != result.RetiredPaths.end()) {
            return iceberg::Invalid("CORRUPTION: A manifest is referenced by a non-contiguous snapshot range.");
        }
        return result;
    });
}

void ApplyArchiveCut(TBaseState& state, const TArchiveCut& cut)
{
    if (cut.Snapshots.empty()) {
        return;
    }
    if (state.ScanIndex) {
        auto index = std::make_shared<TScanMembership>(*state.ScanIndex);
        std::erase_if(index->Snapshots, [&](const TSnapshotBoundary& boundary) {
            return std::ranges::any_of(cut.Snapshots, [&](const TArchivedSnapshot& archived) {
                return archived.Snapshot->snapshot_id == boundary.SnapshotID;
            });
        });
        state.ScanIndex = std::move(index);
    }
    std::vector<std::string> retained;
    retained.reserve(state.RetainedManifestPaths.size());
    std::ranges::set_difference(state.RetainedManifestPaths, cut.RetiredPaths, std::back_inserter(retained));
    state.RetainedManifestPaths = std::move(retained);
}

}
