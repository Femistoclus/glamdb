#pragma once

#include "core/api/canonical.h"
#include "core/catalog/durable_sqlite_catalog.h"
#include "storage/metadata.h"

#include <span>

namespace iceberg {
class FileIO;
} // namespace iceberg

namespace NGlamDB::NStorage {
struct TBaseState;
}

namespace glamdb::detail {

class TSnapshotMetadataCache;

// Archived snapshots one expiration call can remove. The engine passes one
// more row so the backend knows the oldest archived snapshot that remains.
inline constexpr uint64_t MaxExpiredArchivePerCall = 4096;

struct CollectionView
{
    CollectionInfo info;
    BaseIdentity base;
    // Pinned together with the engine's confirmed view. Custom backends may
    // omit this and use the uncached authoritative planner.
    std::shared_ptr<const NGlamDB::NStorage::TBaseState> scan_state{};
    std::vector<ExpiredSnapshotRecord> expired_snapshots{};
};

// The manifest list of one archived snapshot as its catalog row records it.
struct ArchivedListReference
{
    std::string collection_id;
    std::string manifest_list;
    int64_t bytes = 0;
    std::string sha256;
};

// A collection's verified checkpoint with the archived snapshot that precedes
// its oldest root snapshot (the newest archive row), for OpenFromCheckpoints.
struct CollectionCheckpointView
{
    CollectionCheckpoint checkpoint;
    std::optional<ArchivedSnapshotRecord> newest_archived{};
};

class StagedCreate
{
public:
    virtual ~StagedCreate() = default;
    virtual CollectionInfo Info() const = 0;
    virtual iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) = 0;
    virtual Status VerifyClosure(std::string_view final_metadata_location) = 0;
    // After VerifyClosure: the verified state for the catalog checkpoint, or
    // nothing when this backend does not provide one.
    virtual std::optional<CollectionCheckpoint> Checkpoint() const
    {
        return std::nullopt;
    }
};

class StagedAppend
{
public:
    virtual ~StagedAppend() = default;
    virtual AppendResult Receipt() const = 0;
    virtual bool IsNoOp() const
    {
        return false;
    }
    // This call runs inside the coordinator's owned outer transaction. It must
    // use a fresh Iceberg table with retries=0 and metadata deletion disabled,
    // the same catalog instance, and the exact durable prepared binding.
    virtual iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) = 0;
    // Called by the catalog immediately before INSERT/CAS, after final root
    // completion. Validate every new reachable receipt, reused base membership,
    // sticky I/O errors and candidate reopen capacity. A closed flag is not
    // proof.
    virtual Status VerifyClosure(std::string_view final_metadata_location) = 0;
    // After VerifyClosure: the oldest root snapshots this append moved to the
    // snapshot archive, for the same outer transaction.
    virtual std::vector<ArchivedSnapshotRecord> Archived() const
    {
        return {};
    }
    // After VerifyClosure: the verified state for the catalog checkpoint.
    virtual std::optional<CollectionCheckpoint> Checkpoint() const
    {
        return std::nullopt;
    }
};

class StagedExpiration
{
public:
    virtual ~StagedExpiration() = default;
    virtual std::span<const ExpiredSnapshotRecord> Expired() const = 0;
    virtual std::span<const std::string> Garbage() const = 0;
    virtual SnapshotRetentionPolicy Policy() const = 0;
    virtual bool Changed() const = 0;
    virtual iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) = 0;
    virtual Status VerifyClosure(std::string_view location) = 0;
    // Leading Expired() records that leave the snapshot archive.
    virtual int64_t ArchivedExpired() const
    {
        return 0;
    }
    // Archive maintenance: after VerifyClosure, the snapshots it moved.
    virtual std::vector<ArchivedSnapshotRecord> Archived() const
    {
        return {};
    }
    // After VerifyClosure: the verified state for the catalog checkpoint.
    virtual std::optional<CollectionCheckpoint> Checkpoint() const
    {
        return std::nullopt;
    }
};

class StorageBackend
{
public:
    struct Health
    {
        bool read_available = false;
        bool write_available = true;
        bool write_failure_latched = false;
    };
    virtual ~StorageBackend() = default;
    // `archived`: every archive row's manifest list. Archived lists and the
    // manifests they reference are retained metadata, not orphans.
    virtual Result<StorageFootprint> GetStorageFootprint(std::span<const ArchivedListReference> /*archived*/)
    {
        return std::unexpected(ErrorDetail{"DEPENDENCY_UNAVAILABLE",
            "physical store inventory is not installed",
            RetryHint::OperatorAction});
    }
    virtual EngineCapabilities GetCapabilities() const
    {
        return {.backend = BackendKind::Custom};
    }
    // Concurrent, bounded observation only: no file I/O or writer/cache locks.
    virtual Health GetHealth() const
    {
        return {};
    }
    virtual Result<std::unique_ptr<StagedCreate>>
    PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt_id) = 0;
    // Pre-binding admission against the exact committed base that the
    // coordinator is about to bind. A rejection consumes no operation ID.
    // Allocate the normalized writer batch here and retain it in input.
    virtual Status CheckAppendAdmissible(const StoreInfo&,
        const CollectionInfo&,
        const BaseIdentity&,
        NormalizedInput&,
        std::chrono::steady_clock::time_point = std::chrono::steady_clock::time_point::max(),
        const std::function<bool()>& = {})
    {
        return {};
    }
    virtual Result<std::unique_ptr<StagedAppend>> PrepareAppend(const StoreInfo& store,
        const BoundOperation& binding,
        const CollectionInfo& collection,
        const NormalizedInput& input,
        std::chrono::steady_clock::time_point deadline) = 0;
    // `archived` holds the oldest archived snapshots (at most 4097), from
    // which expiration starts.
    virtual Result<std::unique_ptr<StagedExpiration>> PrepareExpiration(const StoreInfo&,
        const CollectionInfo&,
        const BaseIdentity&,
        const std::optional<SnapshotRetentionPolicy>&,
        std::span<const ArchivedSnapshotRecord>)
    {
        return std::unexpected(
            ErrorDetail{"DEPENDENCY_UNAVAILABLE", "snapshot maintenance is not installed", RetryHint::OperatorAction});
    }
    // Moves the oldest root snapshots above the collection's hot_snapshots to
    // the archive, bounded per call. Changed() is false when nothing moves.
    virtual Result<std::unique_ptr<StagedExpiration>>
    PrepareArchive(const StoreInfo&, const CollectionInfo&, const BaseIdentity&)
    {
        return std::unexpected(
            ErrorDetail{"DEPENDENCY_UNAVAILABLE", "snapshot archive is not installed", RetryHint::OperatorAction});
    }
    virtual Status RemoveExpiredMetadata(std::string_view, std::string_view)
    {
        return std::unexpected(
            ErrorDetail{"DEPENDENCY_UNAVAILABLE", "metadata cleanup is not installed", RetryHint::OperatorAction});
    }
    // Called only after the outer pointer/receipt transaction has resolved.
    // Committed promotes the exact candidate; DefinitelyNotCommitted discards
    // it; CommitUnknown revokes the verified generation until recovery.
    virtual void PublicationFinished(std::string_view, std::string_view, CommitDisposition result)
    {
        if (result == CommitDisposition::CommitUnknown) {
            InvalidateCache();
        }
    }
    virtual void InvalidateCache()
    { }
    // A committed append superseded this base's root, and no reader can still be
    // resolving it. Recovery and scans read only the current root, so the
    // backend may remove the file; failure leaves an ordinary orphan.
    virtual void RetireRoot(const BaseIdentity&)
    { }
    // Independent, bidirectional validation: each committed receipt matches
    // exactly one retained snapshot/tag, every tag has that committed receipt,
    // Bound/Failed have none; validate UUID/schema/ancestry and reachable
    // files. Absence is authoritative only after the old SQLite handle is
    // closed.
    virtual Status VerifyRetained(const StoreInfo& store,
        std::span<const CollectionView> collections,
        std::span<const OperationRecord> operations) = 0;
    // Production backends stream catalog records. The fallback preserves the
    // existing seam for small test/custom backends.
    virtual Status VerifyRetainedCatalog(const StoreInfo& store,
        std::span<const CollectionView> collections,
        DurableSqliteCatalogStore& catalog)
    {
        return VerifyRetained(store, collections, catalog.Operations());
    }
    // Fresh open without re-verifying history: rebuilds every collection's
    // verified generation from its catalog checkpoint, current root and
    // current manifests. Any error means the caller must run the full
    // VerifyRetainedCatalog path instead; no generation is left installed.
    virtual Status
    OpenFromCheckpoints(const StoreInfo&, std::span<const CollectionView>, std::span<const CollectionCheckpointView>)
    {
        return std::unexpected(
            ErrorDetail{"DEPENDENCY_UNAVAILABLE", "checkpoint open is not installed", RetryHint::OperatorAction});
    }
    // After a successful full verification: one checkpoint per append
    // collection of the verified generation (none for KV collections).
    virtual std::vector<CollectionCheckpoint> VerifiedCheckpoints()
    {
        return {};
    }
    // Recovery workspace bytes that verifying the confirmed generation needs
    // (its conservative visited-key envelope); 0 when not applicable. Never
    // throws.
    virtual uint64_t RecoveryWorkspaceBytes()
    {
        return 0;
    }
    // Independent read-only view for scans; it never joins a writer attempt.
    virtual Result<std::shared_ptr<iceberg::FileIO>> ReadOnlyFileIO()
    {
        return std::unexpected(
            ErrorDetail{"DEPENDENCY_UNAVAILABLE", "storage reader is not installed", RetryHint::OperatorAction});
    }
    // No I/O. Only an exact, explicitly confirmed root of the current verified
    // generation may be pinned; tentative candidates are never returned.
    virtual Result<std::shared_ptr<const NGlamDB::NStorage::TBaseState>> VerifiedScanState(const BaseIdentity&)
    {
        return nullptr;
    }
    virtual std::shared_ptr<TSnapshotMetadataCache> ReadMetadataCache()
    {
        return nullptr;
    }
};

} // namespace glamdb::detail
