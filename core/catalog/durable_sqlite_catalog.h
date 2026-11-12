#pragma once

#include "core/catalog/sqlite.h"
#include "storage/types.h"

#include <array>
#include <functional>
#include <mutex>
#include <span>

#include <iceberg/catalog/sql/catalog_store.h>

namespace glamdb::detail {

enum class OperationState : uint8_t { Bound, Committed, Failed };
struct BoundOperation
{
    std::string collection_id;
    std::string operation_id;
    std::string payload_hash;
    int32_t hash_version = 1;
    BaseIdentity base;
    std::string attempt_id;
    bool operator==(const BoundOperation&) const = default;
};
struct OperationRecord
{
    BoundOperation binding;
    OperationState state = OperationState::Bound;
    std::optional<AppendOutcome> outcome;
    std::string outcome_bytes;
};
struct CatalogProfile
{
    std::string sqlite_version;
    std::string source_id;
    std::string vfs;
    std::vector<std::string> compile_options;
    std::string journal_mode;
    int64_t synchronous = 0;
    int64_t foreign_keys = 0;
    int64_t busy_timeout = 0;
    int64_t cache_size = 0;
    int64_t mmap_size = 0;
};

struct ExpiredSnapshotRecord
{
    int64_t snapshot_id;
    int64_t sequence_number;
    std::string operation_id;
};

// A snapshot moved out of the Iceberg root into the catalog archive. It stays
// readable: its manifest list and manifests remain on disk.
struct ArchivedSnapshotRecord
{
    int64_t snapshot_id = 0;
    int64_t sequence_number = 0;
    std::optional<int64_t> parent_snapshot_id;
    int64_t timestamp_ms = 0;
    std::string operation_id;
    std::string manifest_list;
    int64_t manifest_list_bytes = 0;
    // Lowercase hex SHA-256 of the manifest-list bytes.
    std::string manifest_list_sha256;
    // Live data files of the snapshot: the length of its chronological
    // membership prefix in the append-only profile.
    int64_t data_files = 0;
    // Manifests whose last referencing snapshot is this one.
    int64_t retired_manifests = 0;
    int64_t retired_manifest_bytes = 0;
    // Lossless encoding of the complete Iceberg snapshot entry.
    std::string snapshot_blob;
    bool operator==(const ArchivedSnapshotRecord&) const = default;
};

// Archived snapshots below a sequence, oldest first, read together with the
// current expiration watermark.
struct ArchivedHistory
{
    std::vector<ArchivedSnapshotRecord> snapshots;
    int64_t expired_through = 0;
};

// Verified state of one append collection after its latest publication,
// written by the same outer transaction (catalog version 6). A fresh open that
// finds a matching checkpoint for every collection rebuilds the in-memory state
// from the current root and manifests instead of re-verifying the history.
struct CollectionCheckpoint
{
    std::string collection_id;
    std::optional<int64_t> snapshot_id;
    int64_t sequence_number = 0;
    std::string metadata_location;
    int64_t root_bytes = 0;
    // Lowercase hex SHA-256 of the root file.
    std::string root_sha256;
    // Cumulative storage usage in TStorageUsage order: root bytes, snapshots,
    // snapshot log entries, data files, manifests, manifest bytes, manifest
    // list bytes, footer bytes.
    std::array<int64_t, 8> usage{};
    // Maintained by the catalog: operation records of the collection (every
    // binding counts), archived rows, expired ledger rows and a SHA-256 chain
    // over the publications since the checkpoint row was established.
    int64_t operations = 0;
    int64_t archived = 0;
    int64_t expired = 0;
    std::string receipt_chain;
    bool operator==(const CollectionCheckpoint&) const = default;
};

// Every call, including the entire outer publication, owns this same recursive
// serialization lock. The callback must be synchronous and use this instance.
class DurableSqliteCatalogStore final : public iceberg::sql::CatalogStore
{
public:
    using Guard = std::function<Status(std::string_view)>;
    using Body = std::function<iceberg::Status()>;
    static std::shared_ptr<DurableSqliteCatalogStore>
    Create(const std::filesystem::path& path, StoreInfo info, Limits limits, std::string vfs = {});
    // `checkpointed`: a catalog of version 6 or later validates only its
    // header and O(collections) registry here; the caller then either opens
    // from verified checkpoints or runs ValidateInventory itself.
    static std::shared_ptr<DurableSqliteCatalogStore> Open(const std::filesystem::path& path,
        StoreInfo info,
        Limits limits,
        std::string vfs = {},
        bool checkpointed = false);
    void Reopen();
    void Close();
    CatalogProfile Profile();
    std::vector<CollectionInfo> Collections();
    CollectionInfo Collection(std::string_view id);
    CollectionInfo Resolve(std::string_view name);
    BaseIdentity Base(std::string_view id);
    std::vector<OperationRecord> Operations();
    using OperationConsumer = std::function<void(const OperationRecord&)>;
    // Keyset pages hold at most 256 serialized records. The callback runs
    // under the catalog lock, with no live SQLite statement between pages.
    // It may update terminal state, but must not change keys or membership.
    void ForEachOperation(const OperationConsumer& consume, std::string_view collection_id = {});
    std::optional<OperationRecord> FindOperation(std::string_view collection_id, std::string_view operation_id);
    std::optional<OperationRecord> TryFindOperation(std::string_view collection_id, std::string_view operation_id);
    void BindOperation(const BoundOperation& binding, std::string_view migrationOwner = {});
    void RecordFailure(const BoundOperation& binding, const AppendOutcome& outcome);
    // Called after the guarded body: the verified state the publication
    // leaves, or nothing (the collection then has no checkpoint).
    using CheckpointSource = std::function<std::optional<CollectionCheckpoint>()>;
    CollectionInfo PublishCreate(CollectionInfo collection,
        const Guard& guard,
        const Body& body,
        std::string_view migrationIdentity = {},
        std::string_view migrationOperation = {},
        const CheckpointSource& checkpoint = {});
    std::optional<std::string> MigrationIdentity(std::string_view id);
    std::optional<std::pair<CollectionInfo, std::optional<OperationRecord>>>
    TryMigrationReplay(std::string_view id, std::string_view operation, std::string_view identity);
    // Settings the collection was created with (empty when none were given).
    WriteSettings CreatedWriteSettings(std::string_view id);
    // Compare-and-set of canonical settings; see UpdateWriteSettingsRequest.
    CollectionInfo UpdateWriteSettings(std::string_view id, int64_t expected_version, WriteSettings settings);
    // Called after the guarded body: the root snapshots the publication moved
    // to the archive, oldest first.
    using ArchiveSource = std::function<std::vector<ArchivedSnapshotRecord>()>;
    void PublishAppend(const BoundOperation& binding,
        const AppendResult& result,
        const Guard& guard,
        const Body& body,
        const ArchiveSource& archived = {},
        const CheckpointSource& checkpoint = {});
    void PublishNoOp(const BoundOperation& binding, const AppendResult& result, const Guard& guard);
    // The first `archived_expired` records leave the snapshot archive.
    void PublishExpiration(const BaseIdentity& base,
        std::span<const ExpiredSnapshotRecord> expired,
        std::span<const std::string> garbage,
        const Guard& guard,
        const Body& body,
        int64_t archived_expired = 0,
        const CheckpointSource& checkpoint = {},
        std::optional<int64_t> lower_default_hot = std::nullopt);
    // Lowers a defaulted hot_snapshots setting (to a retention keep_last)
    // and keeps it marked as defaulted. INVALID_ARGUMENT otherwise.
    void LowerDefaultHotSnapshots(std::string_view collection_id, int64_t hot_snapshots);
    void PublishArchive(const BaseIdentity& base,
        const Guard& guard,
        const Body& body,
        const ArchiveSource& archived,
        const CheckpointSource& checkpoint = {});
    std::vector<ExpiredSnapshotRecord> ExpiredSnapshots(std::string_view collection_id);
    bool IsExpiredSnapshot(std::string_view collection_id, int64_t snapshot_id);
    // The setting a collection was created with (absent when none was given).
    std::optional<int64_t> CreatedHotSnapshots(std::string_view collection_id);
    CollectionInfo UpdateHotSnapshots(std::string_view collection_id, std::optional<int64_t> hot_snapshots);
    // Indexed lookups; empty results before the archive catalog version.
    std::optional<ArchivedSnapshotRecord> FindArchivedSnapshot(std::string_view collection_id, int64_t snapshot_id);
    bool IsArchivedManifestList(std::string_view collection_id, std::string_view path);
    std::vector<ArchivedSnapshotRecord> OldestArchivedSnapshots(std::string_view collection_id, int64_t limit);
    ArchivedHistory ArchivedSnapshotsBelow(std::string_view collection_id, int64_t before_sequence);
    // Keyset pages of 256 rows in sequence order, with no live statement
    // between pages. The callback must not change the archive.
    using ArchiveConsumer = std::function<void(const ArchivedSnapshotRecord&)>;
    void ForEachArchivedSnapshot(std::string_view collection_id, const ArchiveConsumer& consume);
    int64_t PendingMetadataCount(std::string_view id);
    std::vector<std::string> PendingMetadata(std::string_view collection_id);
    void AcknowledgeMetadata(std::string_view collection_id, std::span<const std::string> paths);
    // With `reestablish` and catalog version 6, also replaces every
    // collection's checkpoint after full verification proved the store:
    // `reseed` holds one per append collection, other collections lose theirs.
    void DurabilityFence(std::span<const CollectionCheckpoint> reseed = {}, bool reestablish = false);
    int64_t FenceValue();
    void ValidateInventory();
    // Header, registry and operation capacity: O(collections). No SQLite
    // integrity check and no walk over operation records.
    void ValidateInventoryLight();
    // Catalog version 6 or later.
    bool HasCheckpoints();
    std::optional<CollectionCheckpoint> Checkpoint(std::string_view collection_id);
    // Requires each checkpoint (one per collection, in any order) to match the
    // registry pointer and snapshot, the archive and expiration row counts of
    // its collection, and their operation counts to sum to the catalog's
    // exact operation count. Indexed counts only; CORRUPTION otherwise.
    void CheckCheckpoints(std::span<const CollectionCheckpoint> checkpoints);
    // After detected corruption: the next open must verify fully. Waits for
    // a publication of another thread to end (the caller must hold no engine
    // lock that publication needs, such as the commit gate) and returns once
    // the deletion is durable: true. False when it cannot be made durable now:
    // after an uncertain transaction, or when called from inside this
    // thread's own publication, whose outer transaction then deletes the
    // checkpoints right after it ends (in that thread, before returning).
    bool DropCheckpoints();
    // The archived snapshot with the highest sequence, if any.
    std::optional<ArchivedSnapshotRecord> NewestArchivedSnapshot(std::string_view collection_id);
    // Bound (interrupted) operations in key order. Version 6 seeks a partial
    // index; older catalogs walk every record. The callback may record a
    // terminal outcome.
    void ForEachBoundOperation(const OperationConsumer& consume);
    // ValidateInventory unless this connection has run no transaction since
    // its last complete validation.
    void ValidateInventoryIfChanged();

    iceberg::Status Initialize() override;
    iceberg::Result<std::vector<std::string>> ListNamespaceNames() override;
    iceberg::Result<std::vector<iceberg::sql::NamespaceProperty>> GetNamespaceProperties(std::string_view ns) override;
    iceberg::Status
    InsertNamespaceProperty(std::string_view ns, std::string_view key, std::optional<std::string_view> value) override;
    iceberg::Status DeleteNamespaceProperty(std::string_view ns, std::string_view key) override;
    iceberg::Result<int64_t> DeleteNamespace(std::string_view ns) override;
    iceberg::Result<std::vector<std::string>> ListTableNames(std::string_view ns) override;
    iceberg::Result<bool> TableExists(std::string_view ns, std::string_view name) override;
    iceberg::Result<std::optional<std::string>> GetTableMetadataLocation(std::string_view ns,
        std::string_view name) override;
    iceberg::Status InsertTable(std::string_view ns, std::string_view name, std::string_view location) override;
    iceberg::Result<int64_t> UpdateTableMetadataLocation(std::string_view ns,
        std::string_view name,
        std::string_view location,
        std::string_view previous,
        std::string_view expected) override;
    iceberg::Result<int64_t> DeleteTable(std::string_view ns, std::string_view name) override;
    iceberg::Result<int64_t> RenameTable(std::string_view from_ns,
        std::string_view from_name,
        std::string_view to_ns,
        std::string_view to_name) override;
    iceberg::Status RunInTransaction(const Body& body) override;

private:
    DurableSqliteCatalogStore(std::filesystem::path path, StoreInfo info, Limits limits, std::string vfs);
    struct Publication
    {
        bool creating = false;
        std::string name;
        std::optional<BaseIdentity> base;
        Guard guard;
        std::string location;
        int mutations = 0;
        std::optional<ErrorDetail> first_error{};
    };
    void OpenConnection(bool create, bool checkpointed = false);
    void Configure();
    void ValidateHeader();
    void Transaction(const std::function<void()>& body);
    void CheckCapacity(bool operation);
    int64_t OperationCount();
    void CheckFreeBytes();
    bool HasWriteSettings();
    void EnsureWriteSettings();
    void EnsureKVModes();
    void EnsureRetention();
    bool HasArchive();
    void EnsureArchive();
    void EnsureCheckpoints();
    void LowerDefaultHot(std::string_view collection_id, int64_t hot_snapshots);
    int64_t CountRows(std::string_view table, std::string_view collection_id);
    // Inside a publication or fence transaction of a version 5+ catalog.
    // `kind` and `receipt` (encoded outcome, may be empty) extend the chain.
    void WriteCheckpoint(std::string_view collection_id,
        std::optional<CollectionCheckpoint> record,
        std::string_view kind,
        std::string_view receipt,
        int64_t archived_delta,
        int64_t expired_delta,
        bool reseed);
    void InsertArchived(std::string_view collection_id, std::span<const ArchivedSnapshotRecord> archived);
    ArchivedSnapshotRecord ReadArchived(const SqlRow& row);
    int64_t ArchiveWatermark(std::string_view collection_id);
    void CheckBinding(const BoundOperation& binding);
    void Complete(const BoundOperation& binding, const AppendOutcome& outcome);
    struct PointerTarget
    {
        std::string_view ns;
        std::string_view name;
    };
    void GuardPointer(PointerTarget target, std::string_view location);
    OperationRecord ReadOperation(const SqlRow& row);
    CollectionInfo ReadCollection(const SqlRow& row);

    std::filesystem::path path_;
    StoreInfo info_;
    Limits limits_;
    std::string vfs_;
    Database database_;
    std::recursive_mutex mutex_;
    Publication* publication_ = nullptr;
    bool transaction_ = false;
    bool rollback_only_ = false;
    bool uncertain_ = false;
    // DropCheckpoints was called inside this connection's outer transaction.
    bool drop_checkpoints_after_transaction_ = false;
    void DeleteCheckpoints();
    void DropCheckpointsAfterTransaction() noexcept;
    // Exact row count of `operations`. The store has one owning process, so a
    // count derived on this connection stays exact while committed bindings
    // increment it; it is empty whenever that cannot be proven.
    std::optional<int64_t> operation_count_;
    // A complete ValidateInventory ran and no transaction has started since.
    bool inventory_validated_ = false;
};

} // namespace glamdb::detail
