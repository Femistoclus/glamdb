#pragma once

#include "core/io/local_fs.h"
#include "core/io/sha256.h"
#include "core/io/write_latch.h"

#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

namespace NGlamDB::NIO {

enum class EAttemptKind : std::uint8_t { CreateCollection, Append };
enum class EFileState : std::uint8_t { Open, Synced, Closed, DirSynced, Failed };

struct TAttemptSpec
{
    std::string StoreId;
    std::string CollectionId;
    std::string AttemptId;
    EAttemptKind Kind = EAttemptKind::Append;
    std::optional<std::string> OperationId;
};

struct TAttemptLimits
{
    std::uint64_t NewFileBytes = 256ULL << 20;
    std::uint64_t IoBytes = 512ULL << 20;
    std::size_t DataFiles = 1;

    // Explicit finite profile for one multi-file append. Ordinary attempts
    // retain the one-file declaration and registration limit.
    static TAttemptLimits BulkAppend();
};

struct TDurableFileReceipt
{
    std::string Path;
    EFileKind Kind;
    std::uint64_t Length;
    TSha256Digest Sha256;
    std::string AttemptId;
};

struct TFileRecord
{
    std::string Path;
    EFileKind Kind;
    EFileState State = EFileState::Open;
    std::uint64_t Length = 0;
    std::optional<TSha256Digest> Sha256 = TSha256{}.Finalize();
    std::optional<TDurableFileReceipt> Receipt = std::nullopt;
    bool FormatVerified = false;
    std::error_code FirstError{};
    // Warehouse bytes of the durable file as MeasureTree counts them: the
    // larger of its length and its allocated blocks after the file sync.
    std::uint64_t PhysicalBytes = 0;
};

struct TAttemptBudget
{
    std::uint64_t NewFileBytes = 0;
    std::uint64_t ReservedNewFileBytes = 0;
    std::uint64_t IoBytes = 0;
};

class TIOAttempt;

class TWriteReservation
{
public:
    TWriteReservation(const TWriteReservation&) = delete;
    TWriteReservation& operator=(const TWriteReservation&) = delete;
    TWriteReservation(TWriteReservation&& other) noexcept;
    TWriteReservation& operator=(TWriteReservation&& other) noexcept;
    ~TWriteReservation();

    std::error_code Finish(std::uint64_t accepted, std::optional<TSha256Digest> digest = std::nullopt);

private:
    friend class TIOAttempt;
    std::shared_ptr<TIOAttempt> Attempt_;
    std::string Path_;
    std::uint64_t Requested_;

    TWriteReservation(std::shared_ptr<TIOAttempt> attempt, std::string path, std::uint64_t requested);
    void Abandon() noexcept;
};

class TIOAttempt : public std::enable_shared_from_this<TIOAttempt>
{
public:
    static TResult<std::shared_ptr<TIOAttempt>>
    Make(TAttemptSpec spec, std::shared_ptr<TWriteLatch> latch, TAttemptLimits limits = {});

    const std::string& Id() const noexcept;
    const std::string& StoreId() const noexcept;
    const std::string& CollectionId() const noexcept;
    EAttemptKind Kind() const noexcept;
    const std::shared_ptr<TWriteLatch>& Latch() const noexcept;
    std::error_code DeclareDataFile(std::string_view path);
    TResult<EFileKind> RegisterFile(std::string_view path);
    std::error_code CheckWrite(std::string_view path);
    void FailFile(std::string_view path, std::error_code error);
    std::error_code Advance(std::string_view path, EFileState next);
    TResult<TDurableFileReceipt>
    CompleteFile(std::string_view path, std::uint64_t length, const TSha256Digest& digest, std::uint64_t allocated = 0);
    std::error_code MarkFormatVerified(std::string_view path);
    TResult<TWriteReservation> ReserveWrite(std::string_view path, std::uint64_t requested);
    std::error_code ChargeRead(std::uint64_t requested);
    std::error_code CheckBinding() const;
    std::error_code CanRead(std::string_view path) const;
    TResult<TFileRecord> Record(std::string_view path) const;
    std::vector<TFileRecord> Records() const;
    std::vector<TDurableFileReceipt> Receipts() const;
    TAttemptBudget Budget() const;
    std::error_code FirstError() const;
    std::error_code Seal();
    void Deactivate();

private:
    friend class TWriteReservation;
    struct TEntry
    {
        TFileRecord Record;
        std::uint64_t PendingWrites = 0;
    };

    TAttemptSpec Spec_;
    std::shared_ptr<TWriteLatch> Latch_;
    TAttemptLimits Limits_;
    mutable std::mutex Mutex_;
    std::map<std::string, TEntry, std::less<>> Files_;
    std::set<std::string, std::less<>> DeclaredData_;
    TAttemptBudget Budget_;
    std::error_code FirstError_;
    bool Sealed_ = false;
    bool Active_ = true;

    TIOAttempt(TAttemptSpec spec, std::shared_ptr<TWriteLatch> latch, TAttemptLimits limits);
    std::error_code FailLocked(std::string_view path, std::error_code error);
    std::error_code AdmitLocked(std::string_view path);
    std::error_code FinishWrite(std::string_view path,
        std::uint64_t requested,
        std::optional<std::uint64_t> accepted,
        std::optional<TSha256Digest> digest);
};

}
