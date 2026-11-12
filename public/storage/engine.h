#pragma once

#include "storage/kv.h"
#include "storage/metadata.h"
#include "storage/scan.h"
#include "storage/types.h"

#include <string_view>

namespace arrow {
class Schema;
}

namespace glamdb {
namespace detail {
struct EngineFactory;
struct TKvInput;
class TReadBudget;
}

// Synchronous facade. Storage production and scanning are separate components;
// unavailable components report DEPENDENCY_UNAVAILABLE rather than success.
class Engine
{
public:
    struct Impl;
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    const StoreInfo& GetStoreInfo() const;
    // Immutable effective limits selected at creation/open.
    const Limits& GetLimits() const;
    std::shared_ptr<ResourceOwner> GetResourceOwner() const;
    // No catalog/file I/O and no writer wait; remains callable during recovery/shutdown.
    StoreHealth GetHealth() const;
    EngineCapabilities GetCapabilities() const;
    // Bounded physical inventory; refuses while a writer/recovery owns the store.
    Result<StorageFootprint> GetStorageFootprint() const;
    Result<SnapshotHistory> ListSnapshots(std::string_view collection_id) const;
    Result<SnapshotRetentionPolicy> GetSnapshotRetention(std::string_view collection_id) const;
    // Explicit metadata maintenance, serialized with appends. Already planned
    // cursors continue reading. New reads/preconditions on expired IDs report
    // SNAPSHOT_EXPIRED. An uncertain publication requires Reconcile and retry.
    Result<SnapshotExpirationResult> ExpireSnapshots(const SnapshotExpirationRequest& request);
    Result<CollectionStatistics> GetStatistics(std::string_view collection_id,
        SnapshotSelector selector = SnapshotSelector::Latest()) const;
    Result<TFileListing> ListFiles(std::string_view collectionID,
        SnapshotSelector selector = SnapshotSelector::Latest()) const;
    // Only opaque IDs returned by ListFiles; membership is checked before file I/O.
    Result<TFileDetails> GetFileDetails(std::string_view collectionID,
        std::string_view fileID,
        SnapshotSelector selector = SnapshotSelector::Latest()) const;
    // Plans and validates one fixed view without reading Parquet data.
    Result<ScanExplanation> ExplainScan(const ScanRequest& request) const;
    CreateOutcome CreateCollection(const CreateCollectionRequest& request);
    // Changes the settings of data files that later appends write; existing
    // files and snapshots are unchanged. Serialized with appends: an append
    // that is already bound keeps the settings it read. Not part of any
    // append's operation identity.
    Result<CollectionInfo> UpdateWriteSettings(const UpdateWriteSettingsRequest& request);
    // Cache misses return OPERATION_IN_PROGRESS while a writer is active;
    // confirmed hits remain readable. Neither lookup waits for the writer.
    Result<CollectionInfo> GetCollection(std::string_view collection_id) const;
    Result<CollectionInfo> ResolveCollection(std::string_view exact_name) const;
    Result<std::vector<CollectionInfo>> ListCollections() const;
    // Fixed listings; repeat all parameters on continuation. Tokens expire after
    // five minutes and are valid only in this Engine and authorization context.
    Result<TCollectionPage> ListCollectionsPage(const TMetadataPageRequest& request = {}) const;
    Result<SnapshotHistory> ListSnapshotsPage(std::string_view collectionID,
        const TMetadataPageRequest& request = {}) const;
    Result<TFileListing> ListFilesPage(std::string_view collectionID,
        const TMetadataPageRequest& request = {},
        SnapshotSelector selector = SnapshotSelector::Latest()) const;
    Result<std::shared_ptr<const arrow::Schema>> Schema(std::string_view collection_id) const;
    // Immutable confirmed schema for decoding retries while recovery fences reads.
    // AppendDurable still validates identity, payload and write admission.
    Result<SchemaSpec> SchemaForAppend(std::string_view collection_id) const;
    AppendOutcome AppendDurable(const AppendRequest& request);
    AppendOutcome BulkAppendDurable(const BulkAppendRequest& request);
    AppendOutcome UpsertDurable(const AppendRequest& request);
    AppendOutcome DeleteDurable(const DeleteRequest& request);
    AppendOutcome BulkUpsertDurable(const BulkAppendRequest& request);
    AppendOutcome CompactDurable(const CompactionRequest& request);
    MigrationOutcome MigrateAppendToKV(const MigrationRequest& request);
    Result<LookupResult> Lookup(const LookupRequest& request) const;
    Result<std::optional<AppendOutcome>> FindAppendOutcome(std::string_view collection_id,
        std::string_view operation_id) const;
    // Reads one fixed view. The cursor retains the store until it is released.
    Result<std::shared_ptr<ScanCursor>> Scan(const ScanRequest& request) const;
    Status Reconcile();
    Status Shutdown(std::chrono::milliseconds timeout);

private:
    void CloseOnCorruption(const ErrorDetail& error) const;
    CreateOutcome CreateCollectionImpl(const CreateCollectionRequest& request,
        std::string_view migrationIdentity,
        std::string_view migrationOperation = {});
    AppendOutcome AppendDurableImpl(const AppendRequest& request,
        const BulkAppendRequest* bulk,
        const detail::TKvInput* kv = nullptr);
    Result<std::shared_ptr<ScanCursor>> ScanImpl(const ScanRequest& request,
        std::shared_ptr<detail::TReadBudget> budget,
        bool chronologicalFiles = false) const;
    explicit Engine(std::shared_ptr<Impl> implementation);
    std::shared_ptr<Impl> impl_;
    friend struct detail::EngineFactory;
};

Result<std::shared_ptr<Engine>> CreateStore(const CreateStoreRequest& request);
Result<std::shared_ptr<Engine>> OpenExisting(const OpenStoreRequest& request);

} // namespace glamdb
