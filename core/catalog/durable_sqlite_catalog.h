#pragma once

#include "core/catalog/sqlite.h"
#include "storage/types.h"

#include <functional>
#include <mutex>

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
    void PublishAppend(const BoundOperation& binding, const AppendResult& result, const Guard& guard, const Body& body);
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
