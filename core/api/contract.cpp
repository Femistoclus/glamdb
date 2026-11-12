#include "storage/contract.h"

#include <algorithm>
#include <initializer_list>
#include <string>

namespace glamdb {
namespace {

constexpr TStorageContractVersion FirstVersion{1, 0};
// 1.1 adds the snapshot archive and verified-checkpoint operations, features
// and limits; 1.0 describes exactly what it did before.
constexpr TStorageContractVersion CurrentVersion{1, 1};

void AddOperations(TStorageContract& contract, std::initializer_list<EStorageOperation> operations)
{
    contract.Operations.insert(contract.Operations.end(), operations.begin(), operations.end());
}

TStorageContract DescribeContract(TStorageContractVersion version,
    CollectionMode mode,
    const EngineCapabilities& capabilities,
    const Limits& limits)
{
    TStorageContract contract;
    contract.Version = version;
    contract.Mode = mode;
    contract.EffectiveLimits = {.Engine = limits,
        .SchemaFields = static_cast<int64_t>(MaxSchemaFields),
        .AppendRows = 10000,
        .AppendDecodedBytes = 16LL << 20,
        .ScanBatchRows = 65536,
        .BulkRows = BulkAppendLimits::MaxRows,
        .BulkDecodedBytes = BulkAppendLimits::MaxDecodedBytes,
        .LookupKeys = 4096,
        .DeleteKeys = 10000};
    // These operations belong to the coordinator/catalog, even without storage.
    AddOperations(contract,
        {EStorageOperation::CollectionMetadata,
            EStorageOperation::FindOutcome,
            EStorageOperation::Reconcile,
            EStorageOperation::Shutdown});
    contract.Features.push_back(EStorageFeature::AuthoritativeOutcomes);
    const bool native = capabilities.backend == BackendKind::LocalStorage;
    const bool supportedMode =
        capabilities.backend != BackendKind::Unavailable && (mode == CollectionMode::Append || capabilities.key_value);
    if (supportedMode && capabilities.metadata) {
        AddOperations(contract,
            {EStorageOperation::SnapshotHistory,
                EStorageOperation::Statistics,
                EStorageOperation::Files,
                EStorageOperation::FileDetails});
    }
    if (supportedMode && capabilities.scan) {
        AddOperations(contract, {EStorageOperation::Scan, EStorageOperation::ExplainScan});
        contract.Features.push_back(EStorageFeature::FixedSnapshotScan);
        if (mode == CollectionMode::KeyValue) {
            contract.Operations.push_back(EStorageOperation::Lookup);
        }
    }
    if (supportedMode && capabilities.append) {
        contract.Operations.push_back(EStorageOperation::CreateCollection);
        if (mode == CollectionMode::Append) {
            contract.Operations.push_back(EStorageOperation::Append);
            contract.Features.push_back(EStorageFeature::SnapshotPreconditions);
            contract.Features.push_back(EStorageFeature::PreserveAppendDuplicates);
            if (native) {
                AddOperations(contract, {EStorageOperation::BulkAppend, EStorageOperation::ExpireSnapshots});
                if (capabilities.key_value && capabilities.scan) {
                    contract.Operations.push_back(EStorageOperation::MigrateAppendToKV);
                }
            }
        } else if (native) {
            AddOperations(contract,
                {EStorageOperation::Upsert,
                    EStorageOperation::Delete,
                    EStorageOperation::BulkUpsert,
                    EStorageOperation::Compact});
            contract.Features.push_back(EStorageFeature::SnapshotPreconditions);
            contract.Features.push_back(EStorageFeature::DurableNoOp);
        }
    }
    if (supportedMode && capabilities.write_settings) {
        contract.Operations.push_back(EStorageOperation::UpdateWriteSettings);
    }
    if (capabilities.backend != BackendKind::Unavailable && capabilities.storage_footprint) {
        contract.Operations.push_back(EStorageOperation::StorageFootprint);
    }
    if (contract.HasOperation(EStorageOperation::Scan) || contract.HasOperation(EStorageOperation::Append) ||
        contract.HasOperation(EStorageOperation::Upsert))
    {
        contract.Features.push_back(EStorageFeature::TypedNulls);
    }
    if (version < CurrentVersion) {
        return contract;
    }
    // Full verification is the store-wide recovery path, like Reconcile.
    contract.Operations.push_back(EStorageOperation::VerifyStoreFull);
    // Archive maintenance is native append-only, like retention.
    if (native && capabilities.snapshot_archive && mode == CollectionMode::Append &&
        contract.HasOperation(EStorageOperation::Append))
    {
        AddOperations(contract, {EStorageOperation::UpdateSnapshotArchive, EStorageOperation::ArchiveSnapshots});
        contract.EffectiveLimits.DefaultHotSnapshots = glamdb::DefaultHotSnapshots;
        contract.EffectiveLimits.MaxHotSnapshots = glamdb::MaxHotSnapshots;
        if (contract.HasOperation(EStorageOperation::SnapshotHistory) || contract.HasOperation(EStorageOperation::Scan))
        {
            contract.Features.push_back(EStorageFeature::ArchivedSnapshotReads);
        }
        // Stores with a KV collection always open by full verification.
        contract.Features.push_back(EStorageFeature::CheckpointOpen);
    }
    return contract;
}

} // namespace

bool TStorageContract::HasOperation(EStorageOperation operation) const
{
    return std::ranges::find(Operations, operation) != Operations.end();
}

bool TStorageContract::HasFeature(EStorageFeature feature) const
{
    return std::ranges::find(Features, feature) != Features.end();
}

Result<TStorageContract> NegotiateStorageContract(const TStorageContractRequest& request,
    const EngineCapabilities& capabilities,
    const Limits& limits)
{
    if (request.MinVersion > request.MaxVersion ||
        (request.Mode != CollectionMode::Append && request.Mode != CollectionMode::KeyValue))
    {
        return std::unexpected(ErrorDetail{"INVALID_ARGUMENT", "invalid contract version range or collection mode"});
    }
    // The highest implemented version inside the inclusive range.
    TStorageContractVersion version;
    if (request.MinVersion <= CurrentVersion && request.MaxVersion >= CurrentVersion) {
        version = CurrentVersion;
    } else if (request.MinVersion <= FirstVersion && request.MaxVersion >= FirstVersion) {
        version = FirstVersion;
    } else {
        return std::unexpected(ErrorDetail{"UNSUPPORTED_CONTRACT_VERSION", "no supported contract version in range"});
    }
    auto contract = DescribeContract(version, request.Mode, capabilities, limits);
    for (const auto operation : request.RequiredOperations) {
        if (!contract.HasOperation(operation)) {
            return std::unexpected(ErrorDetail{"UNSUPPORTED_OPERATION",
                "required storage operation is unsupported for this mode/backend: " +
                    std::to_string(static_cast<uint16_t>(operation))});
        }
    }
    for (const auto feature : request.RequiredFeatures) {
        if (!contract.HasFeature(feature)) {
            return std::unexpected(ErrorDetail{"UNSUPPORTED_FEATURE",
                "required storage feature is unsupported for this mode/backend: " +
                    std::to_string(static_cast<uint16_t>(feature))});
        }
    }
    return contract;
}

} // namespace glamdb
