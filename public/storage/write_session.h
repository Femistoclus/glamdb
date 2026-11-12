#pragma once

#include "storage/types.h"

#include <functional>

namespace glamdb {
class Engine;

struct TWriteIdentity
{
    std::string CollectionID;
    std::string OperationID;
};

struct TWritePart
{
    uint32_t Task = 0;
    uint32_t Attempt = 0;
    uint32_t Part = 0;
    auto operator<=>(const TWritePart&) const = default;
};

struct TBeginWrite
{
    TWriteIdentity Identity;
    MutationKind Mutation = MutationKind::None; // None = append; Upsert = KV last-wins.
    SnapshotPrecondition Precondition{};
    std::chrono::seconds Lifetime{3600};
};

enum class EWriteState : uint8_t { Uploading, Sealed, CommitStarted, Aborted, Reclaimed };
struct TWriteStatus
{
    // Keeps the original returned descriptor charged until its final owner.
    std::shared_ptr<const void> ResourceLease{};
    EWriteState State = EWriteState::Uploading;
    int64_t ExpiresAtSeconds = 0;
    int64_t Rows = 0;
    std::string PayloadHash;
    std::vector<TWritePart> Selected;
};
struct TWritePartReceipt
{
    std::shared_ptr<const void> ResourceLease{};
    int64_t Rows = 0;
    std::string CanonicalHash;
};

struct TWriteSessionLimits
{
    // Includes retained tombstones. Exhaustion requires another explicit directory.
    uint32_t MaxSessions = 128;
    uint32_t MaxParts = 4096;
    uint32_t MaxBatches = 8192;
    uint64_t MaxSpoolBytes = 256ULL << 20;
};

// Native local staging only. Owns the supplied engine and a separate directory
// lock; never opens another store. The directory is permanently bound to that
// store ID and exact authorization scope. Access to this facade conveys that scope.
// One active call; competing calls fail OPERATION_IN_PROGRESS without waiting.
// Callbacks must return promptly, cooperate with cancellation, and not reenter.
// Source buffers belong to the caller and must stay immutable during Next/use.
class TWriteSessions
{
public:
    using TNext = std::function<Result<std::shared_ptr<arrow::RecordBatch>>()>;
    static Result<std::shared_ptr<TWriteSessions>> Open(std::shared_ptr<Engine> engine,
        const std::string& absoluteDirectory,
        const std::string& authorizationScope,
        TWriteSessionLimits limits = {});
    ~TWriteSessions();
    TWriteSessions(const TWriteSessions&) = delete;
    TWriteSessions& operator=(const TWriteSessions&) = delete;

    // Identity is caller-known before any acknowledgment, including after restart.
    Result<TWriteStatus> BeginWrite(const TBeginWrite& request);
    Result<TWritePartReceipt> PutPart(const TWriteIdentity& identity,
        TWritePart part,
        TNext next,
        std::shared_ptr<const std::atomic<bool>> cancellation = {});
    // Resolve a lost PutPart reply without replaying a one-shot source.
    Result<std::optional<TWritePartReceipt>> GetPartReceipt(const TWriteIdentity& identity, TWritePart part);
    // Sort by task then part, exactly one attempt per selected task. Input order
    // is irrelevant; row order within each selected part is preserved.
    Result<TWriteStatus> SealWrite(const TWriteIdentity& identity, std::vector<TWritePart> selected);
    Result<AppendOutcome> CommitWrite(const TWriteIdentity& identity,
        std::shared_ptr<const std::atomic<bool>> cancellation = {});
    Result<TWriteStatus> GetWriteStatus(const TWriteIdentity& identity);
    Result<std::optional<AppendOutcome>> GetWriteOutcome(const TWriteIdentity& identity);
    Status AbortWrite(const TWriteIdentity& identity);
    // Expires unstarted sessions; removes payload only after safe abort or a
    // matching authoritative terminal receipt. Retains descriptors and receipts.
    Status Cleanup();

private:
    struct TImpl;
    explicit TWriteSessions(std::unique_ptr<TImpl> impl);
    std::unique_ptr<TImpl> Impl_;
};
} // namespace glamdb
