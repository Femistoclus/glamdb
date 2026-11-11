#pragma once

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
    std::shared_ptr<ResourceOwner> GetResourceOwner() const;
    // No catalog/file I/O and no writer wait; remains callable during recovery/shutdown.
    StoreHealth GetHealth() const;
    EngineCapabilities GetCapabilities() const;
    Result<SnapshotHistory> ListSnapshots(std::string_view collection_id) const;
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
    Result<std::optional<AppendOutcome>> FindAppendOutcome(std::string_view collection_id,
        std::string_view operation_id) const;
    // Reads one fixed view. The cursor retains the store until it is released.
    Result<std::shared_ptr<ScanCursor>> Scan(const ScanRequest& request) const;
    Status Reconcile();
    Status Shutdown(std::chrono::milliseconds timeout);

private:
    explicit Engine(std::shared_ptr<Impl> implementation);
    std::shared_ptr<Impl> impl_;
    friend struct detail::EngineFactory;
};

Result<std::shared_ptr<Engine>> CreateStore(const CreateStoreRequest& request);
Result<std::shared_ptr<Engine>> OpenExisting(const OpenStoreRequest& request);

} // namespace glamdb
