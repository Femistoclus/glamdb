#pragma once

#include "core/engine/storage_backend.h"

#include <atomic>

namespace glamdb::test {

// Dependency double only. These files represent metadata evidence, not Parquet,
// Iceberg manifests, durable file receipts or a usable storage implementation.
class FakeStorage final : public detail::StorageBackend
{
public:
    explicit FakeStorage(std::filesystem::path root)
        : root(std::move(root))
    { }
    Result<std::unique_ptr<detail::StagedCreate>>
    PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt) override;
    Result<std::unique_ptr<detail::StagedAppend>> PrepareAppend(const StoreInfo& store,
        const detail::BoundOperation& binding,
        const CollectionInfo& collection,
        const detail::NormalizedInput& input,
        std::chrono::steady_clock::time_point deadline) override;
    Status VerifyRetained(const StoreInfo& store,
        std::span<const detail::CollectionView> collections,
        std::span<const detail::OperationRecord> operations) override;

    std::filesystem::path Physical(std::string_view uri) const;
    std::filesystem::path root;
    std::filesystem::path last_root;
    std::atomic<int> prepares{0};
    bool fail_prepare = false;
    bool fail_guard = false;
    bool fail_after_pointer = false;
    bool throw_verifier = false;
    std::function<void()> on_prepare;
    std::function<void()> on_pointer;
    std::function<void()> on_verify;
};

} // namespace glamdb::test
