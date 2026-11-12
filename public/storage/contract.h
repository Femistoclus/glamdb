#pragma once

#include "storage/metadata.h"

#include <compare>
#include <cstdint>
#include <vector>

namespace glamdb {

// Native source/semantic contract, independent of package, disk and receipt versions.
struct TStorageContractVersion
{
    uint16_t Major = 1;
    uint16_t Minor = 0;
    auto operator<=>(const TStorageContractVersion&) const = default;
};

enum class EStorageOperation : uint16_t {
    CollectionMetadata = 1,
    CreateCollection,
    UpdateWriteSettings,
    SnapshotHistory,
    Statistics,
    Files,
    FileDetails,
    ExplainScan,
    Scan,
    Append,
    BulkAppend,
    Upsert,
    Delete,
    BulkUpsert,
    Lookup,
    FindOutcome,
    Compact,
    ExpireSnapshots,
    MigrateAppendToKV,
    Reconcile,
    StorageFootprint,
    Shutdown
};

enum class EStorageFeature : uint16_t {
    TypedNulls = 1,
    FixedSnapshotScan,
    PreserveAppendDuplicates,
    AuthoritativeOutcomes,
    SnapshotPreconditions,
    DurableNoOp,
    // Proposed capabilities: never advertised by contract 1.0.
    IndependentReadView,
    ScanSplits,
    StagedWrites,
    StableCAbi,
    ArrowFlight,
    RemoteDescriptors,
    Overwrite,
    Merge,
    SqlMutations,
    SchemaEvolution,
    CrossCollectionTransactions
};

struct TStorageContractRequest
{
    // Inclusive lexicographic range; the highest mutually supported version wins.
    TStorageContractVersion MinVersion{};
    TStorageContractVersion MaxVersion{};
    // Intended mode, not a lookup of a collection or a claim that it exists.
    CollectionMode Mode = CollectionMode::Append;
    std::vector<EStorageOperation> RequiredOperations{};
    std::vector<EStorageFeature> RequiredFeatures{};
};

// Validation ceilings, not reservations or guaranteed available capacity. Ceilings
// apply to supported operations as described below; shared admission still applies.
struct TStorageContractLimits
{
    Limits Engine{};
    int64_t SchemaFields = 0;
    // Shared ordinary AppendDurable/UpsertDurable input ceilings, before KV
    // duplicate resolution. Apply when Upsert is advertised even if Append is not.
    int64_t AppendRows = 0;
    int64_t AppendDecodedBytes = 0;
    int64_t ScanBatchRows = 0;
    int64_t BulkRows = 0;
    int64_t BulkDecodedBytes = 0;
    int64_t LookupKeys = 0;
    int64_t DeleteKeys = 0;
};

struct TStorageContract
{
    TStorageContractVersion Version{};
    CollectionMode Mode = CollectionMode::Append;
    std::vector<EStorageOperation> Operations{};
    std::vector<EStorageFeature> Features{};
    TStorageContractLimits EffectiveLimits{};

    bool HasOperation(EStorageOperation operation) const;
    bool HasFeature(EStorageFeature feature) const;
};

// Pure negotiation: no engine, I/O, readiness check, reservation or publication.
// Invalid ranges/modes: INVALID_ARGUMENT; no version overlap:
// UNSUPPORTED_CONTRACT_VERSION; unmet requirements: UNSUPPORTED_OPERATION or
// UNSUPPORTED_FEATURE. Unknown requirement values fail closed. All use FixInput.
Result<TStorageContract> NegotiateStorageContract(const TStorageContractRequest& request,
    const EngineCapabilities& capabilities,
    const Limits& limits);

} // namespace glamdb
