#pragma once

#include "storage/metadata.h"
#include "storage/read_view.h"
#include "storage/scan_split.h"
#include "storage/write_session.h"

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
    Shutdown,
    AcquireReadView,
    ReadViewInfo,
    ScanReadView,
    RenewReadView,
    ReleaseReadView,
    CancelReadView,
    PlanScan,
    NextScanSplits,
    ReadSplit,
    CancelScanPlan,
    NegotiateScan,
    BeginWrite,
    PutPart,
    GetPartReceipt,
    SealWrite,
    CommitWrite,
    GetWriteStatus,
    GetWriteOutcome,
    AbortWrite,
    CleanupWrites,
    // Contract 1.5: snapshot archive maintenance and explicit full verification.
    UpdateSnapshotArchive,
    ArchiveSnapshots,
    VerifyStoreFull
};

enum class EStorageFeature : uint16_t {
    TypedNulls = 1,
    FixedSnapshotScan,
    PreserveAppendDuplicates,
    AuthoritativeOutcomes,
    SnapshotPreconditions,
    DurableNoOp,
    // Native read views are advertised only from contract 1.1.
    IndependentReadView,
    // Native splits are advertised only from contract 1.2.
    ScanSplits,
    // Native append/upsert sessions from 1.4; requires an explicit staging owner.
    StagedWrites,
    // Proposed capabilities, not advertised.
    StableCAbi,
    ArrowFlight,
    RemoteDescriptors,
    Overwrite,
    Merge,
    SqlMutations,
    SchemaEvolution,
    CrossCollectionTransactions,
    // Native scan negotiation is advertised only from contract 1.3.
    ScanNegotiation,
    // Contract 1.5: snapshots older than a collection's hot window stay
    // readable by ID from the catalog archive; a fresh open of an archive-tier
    // store rebuilds from verified checkpoints instead of re-verifying history.
    ArchivedSnapshotReads,
    CheckpointOpen
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
    // Present only when native IndependentReadView is advertised.
    std::optional<TReadViewLimits> ReadViews{};
    std::optional<TScanSplitLimits> ScanSplits{};
    // Default configurable staging ceilings. A directory retains its chosen limits.
    std::optional<TWriteSessionLimits> WriteSessions{};
    // Contract 1.5, when UpdateSnapshotArchive is advertised: hot_snapshots of
    // a new append collection that does not choose one, and its largest value.
    int64_t DefaultHotSnapshots = 0;
    int64_t MaxHotSnapshots = 0;
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
