#pragma once

#include "core/api/canonical.h"
#include "core/catalog/durable_sqlite_catalog.h"

#include <span>

namespace glamdb::detail {

struct CollectionView {
    CollectionInfo info;
    BaseIdentity base;
};

class StagedCreate {
public:
    virtual ~StagedCreate() = default;
    virtual CollectionInfo Info() const = 0;
    virtual iceberg::Status Publish(iceberg::sql::CatalogStore &catalog) = 0;
    virtual Status VerifyClosure(std::string_view final_metadata_location) = 0;
};

class StagedAppend {
public:
    virtual ~StagedAppend() = default;
    virtual AppendResult Receipt() const = 0;
    // This call runs inside the coordinator's owned outer transaction. It must
    // use a fresh Iceberg table with retries=0 and metadata deletion disabled,
    // the same catalog instance, and the exact durable prepared binding.
    virtual iceberg::Status Publish(iceberg::sql::CatalogStore &catalog) = 0;
    // Called by the catalog immediately before INSERT/CAS, after final root
    // completion. Validate every new reachable receipt, reused base membership,
    // sticky I/O errors and candidate reopen capacity. A closed flag is not
    // proof.
    virtual Status VerifyClosure(std::string_view final_metadata_location) = 0;
};

class StorageBackend {
public:
    virtual ~StorageBackend() = default;
    virtual Result<std::unique_ptr<StagedCreate>>
    PrepareCreate(const StoreInfo &store, const CreateCollectionRequest &request, std::string_view attempt_id) = 0;
    virtual Result<std::unique_ptr<StagedAppend>> PrepareAppend(const StoreInfo &store, const BoundOperation &binding,
                                                                const CollectionInfo &collection,
                                                                const NormalizedInput &input,
                                                                std::chrono::steady_clock::time_point deadline) = 0;
    // Independent, bidirectional validation: each committed receipt matches
    // exactly one retained snapshot/tag, every tag has that committed receipt,
    // Bound/Failed have none; validate UUID/schema/ancestry and reachable
    // files. Absence is authoritative only after the old SQLite handle is
    // closed.
    virtual Status VerifyRetained(const StoreInfo &store, std::span<const CollectionView> collections,
                                  std::span<const OperationRecord> operations) = 0;
};

} // namespace glamdb::detail
