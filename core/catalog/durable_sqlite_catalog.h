#pragma once

#include "core/catalog/sqlite.h"
#include "storage/types.h"

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

// Every call, including the entire outer publication, owns this same recursive
// serialization lock. The callback must be synchronous and use this instance.
class DurableSqliteCatalogStore final : public iceberg::sql::CatalogStore
{
public:
    using Guard = std::function<Status(std::string_view)>;
    using Body = std::function<iceberg::Status()>;
    static std::shared_ptr<DurableSqliteCatalogStore>
    Create(const std::filesystem::path& path, StoreInfo info, Limits limits, std::string vfs = {});
    static std::shared_ptr<DurableSqliteCatalogStore>
    Open(const std::filesystem::path& path, StoreInfo info, Limits limits, std::string vfs = {});
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
    void BindOperation(const BoundOperation& binding);
    void RecordFailure(const BoundOperation& binding, const AppendOutcome& outcome);
    CollectionInfo PublishCreate(CollectionInfo collection, const Guard& guard, const Body& body);
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
        const ArchiveSource& archived = {});
    // The first `archived_expired` records leave the snapshot archive.
    void PublishExpiration(const BaseIdentity& base,
        std::span<const ExpiredSnapshotRecord> expired,
        std::span<const std::string> garbage,
        const Guard& guard,
        const Body& body,
        int64_t archived_expired = 0);
    void PublishArchive(const BaseIdentity& base, const Guard& guard, const Body& body, const ArchiveSource& archived);
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
    void DurabilityFence();
    int64_t FenceValue();
    void ValidateInventory();

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
    void OpenConnection(bool create);
    void Configure();
    void ValidateHeader();
    void Transaction(const std::function<void()>& body);
    void CheckCapacity(bool operation);
    void CheckFreeBytes();
    bool HasWriteSettings();
    void EnsureWriteSettings();
    void EnsureRetention();
    bool HasArchive();
    void EnsureArchive();
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
};

} // namespace glamdb::detail
