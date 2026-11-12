#include "core/io/io_attempt.h"

#include "core/io/store_uri.h"

#include <algorithm>
#include <utility>

namespace NGlamDB::NIO {
namespace {

bool IsAttemptId(std::string_view value)
{
    return IsCanonicalUuid(value) && value[14] == '7' &&
        (value[19] == '8' || value[19] == '9' || value[19] == 'a' || value[19] == 'b');
}

std::size_t FileLimit(EFileKind kind)
{
    return kind == EFileKind::Manifest ? 8 : 1;
}

bool NextState(EFileState current, EFileState next)
{
    return (current == EFileState::Open && next == EFileState::Synced) ||
        (current == EFileState::Synced && next == EFileState::Closed) ||
        (current == EFileState::Closed && next == EFileState::DirSynced);
}

}

TWriteReservation::TWriteReservation(std::shared_ptr<TIOAttempt> attempt, std::string path, std::uint64_t requested)
    : Attempt_(std::move(attempt))
    , Path_(std::move(path))
    , Requested_(requested)
{ }

TWriteReservation::TWriteReservation(TWriteReservation&& other) noexcept
    : Attempt_(std::move(other.Attempt_))
    , Path_(std::move(other.Path_))
    , Requested_(other.Requested_)
{ }

TWriteReservation& TWriteReservation::operator=(TWriteReservation&& other) noexcept
{
    if (this != &other) {
        Abandon();
        Attempt_ = std::move(other.Attempt_);
        Path_ = std::move(other.Path_);
        Requested_ = other.Requested_;
    }
    return *this;
}

TWriteReservation::~TWriteReservation()
{
    Abandon();
}

void TWriteReservation::Abandon() noexcept
{
    if (Attempt_) {
        auto attempt = std::move(Attempt_);
        [[maybe_unused]] const auto recordedError = attempt->FinishWrite(Path_, Requested_, std::nullopt, std::nullopt);
    }
}

std::error_code TWriteReservation::Finish(std::uint64_t accepted, std::optional<TSha256Digest> digest)
{
    if (!Attempt_) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    auto attempt = std::move(Attempt_);
    return attempt->FinishWrite(Path_, Requested_, accepted, digest);
}

TIOAttempt::TIOAttempt(TAttemptSpec spec, std::shared_ptr<TWriteLatch> latch, TAttemptLimits limits)
    : Spec_(std::move(spec))
    , Latch_(std::move(latch))
    , Limits_(limits)
{ }

TResult<std::shared_ptr<TIOAttempt>>
TIOAttempt::Make(TAttemptSpec spec, std::shared_ptr<TWriteLatch> latch, TAttemptLimits limits)
{
    if (!latch || !IsCanonicalUuid(spec.StoreId) || !IsCanonicalUuid(spec.CollectionId) ||
        !IsAttemptId(spec.AttemptId) ||
        (spec.Kind != EAttemptKind::Append && spec.Kind != EAttemptKind::CreateCollection) ||
        (spec.Kind == EAttemptKind::Append && (!spec.OperationId || !IsCanonicalUuid(*spec.OperationId))) ||
        (spec.Kind == EAttemptKind::CreateCollection && spec.OperationId))
    {
        return std::errc::invalid_argument;
    }
    if (auto error = latch->Error()) {
        return error;
    }
    return std::shared_ptr<TIOAttempt>(new TIOAttempt(std::move(spec), std::move(latch), limits));
}

std::error_code TIOAttempt::FailLocked(std::string_view path, std::error_code error)
{
    if (!error) {
        return FirstError_;
    }
    if (!FirstError_) {
        FirstError_ = error;
    }
    if (auto found = Files_.find(path); found != Files_.end()) {
        auto& record = found->second.Record;
        if (!record.FirstError) {
            record.FirstError = error;
        }
        record.State = EFileState::Failed;
        record.Receipt.reset();
        record.FormatVerified = false;
    }
    return FirstError_;
}

std::error_code TIOAttempt::AdmitLocked(std::string_view path)
{
    if (FirstError_) {
        return FailLocked(path, FirstError_);
    }
    if (!Active_) {
        return FailLocked(path, std::make_error_code(std::errc::operation_canceled));
    }
    if (Sealed_) {
        return FailLocked(path, std::make_error_code(std::errc::operation_not_permitted));
    }
    if (auto error = Latch_->Error()) {
        return FailLocked(path, error);
    }
    return {};
}

std::error_code TIOAttempt::DeclareDataFile(std::string_view path)
{
    std::lock_guard lock(Mutex_);
    if (auto error = AdmitLocked(path)) {
        return error;
    }
    const auto uri = TStoreUri::Parse(path, Spec_.StoreId);
    if (!uri) {
        return FailLocked(path, uri.Error());
    }
    if (Spec_.Kind != EAttemptKind::Append || !Spec_.OperationId || uri.Value().Kind() != EFileKind::Data ||
        uri.Value().CollectionId() != Spec_.CollectionId)
    {
        return FailLocked(path, std::make_error_code(std::errc::invalid_argument));
    }
    const auto prefix = *Spec_.OperationId + "-" + Spec_.AttemptId + "-";
    const auto leaf = uri.Value().Leaf();
    if (leaf.size() != prefix.size() + 5 + 8 || !leaf.starts_with(prefix) ||
        !std::ranges::all_of(leaf.substr(prefix.size(), 5), [](char c) {
            return c >= '0' && c <= '9';
        }))
    {
        return FailLocked(path, std::make_error_code(std::errc::invalid_argument));
    }
    if (DeclaredData_) {
        return FailLocked(path, std::make_error_code(std::errc::file_exists));
    }
    DeclaredData_.emplace(path);
    return {};
}

TResult<EFileKind> TIOAttempt::RegisterFile(std::string_view path)
{
    std::lock_guard lock(Mutex_);
    if (auto error = AdmitLocked(path)) {
        return error;
    }
    const auto uri = TStoreUri::Parse(path, Spec_.StoreId);
    if (!uri) {
        return FailLocked(path, uri.Error());
    }
    const auto kind = uri.Value().Kind();
    if (uri.Value().CollectionId() != Spec_.CollectionId ||
        (Spec_.Kind == EAttemptKind::CreateCollection && kind != EFileKind::Metadata) ||
        (kind == EFileKind::Data && (!DeclaredData_ || *DeclaredData_ != path)))
    {
        return FailLocked(path, std::make_error_code(std::errc::invalid_argument));
    }
    if (Files_.contains(path)) {
        return FailLocked(path, std::make_error_code(std::errc::file_exists));
    }
    const auto count = std::ranges::count_if(Files_, [kind](const auto& entry) {
        return entry.second.Record.Kind == kind;
    });
    if (static_cast<std::size_t>(count) >= FileLimit(kind)) {
        return FailLocked(path, std::make_error_code(std::errc::no_buffer_space));
    }
    Files_.emplace(std::string(path), TEntry{TFileRecord{std::string(path), kind}, 0});
    return kind;
}

std::error_code TIOAttempt::CheckWrite(std::string_view path)
{
    std::lock_guard lock(Mutex_);
    if (auto error = AdmitLocked(path)) {
        return error;
    }
    if (!Files_.contains(path)) {
        return FailLocked(path, std::make_error_code(std::errc::no_such_file_or_directory));
    }
    return {};
}

void TIOAttempt::FailFile(std::string_view path, std::error_code error)
{
    std::lock_guard lock(Mutex_);
    [[maybe_unused]] const auto recordedError = FailLocked(path, error);
}

std::error_code TIOAttempt::Advance(std::string_view path, EFileState next)
{
    std::lock_guard lock(Mutex_);
    if (auto error = AdmitLocked(path)) {
        return error;
    }
    const auto found = Files_.find(path);
    if (found == Files_.end()) {
        return FailLocked(path, std::make_error_code(std::errc::no_such_file_or_directory));
    }
    if (!NextState(found->second.Record.State, next) || found->second.PendingWrites != 0) {
        return FailLocked(path, std::make_error_code(std::errc::operation_not_permitted));
    }
    found->second.Record.State = next;
    return {};
}

TResult<TDurableFileReceipt> TIOAttempt::CompleteFile(std::string_view path,
    std::uint64_t length,
    const TSha256Digest& digest,
    std::uint64_t allocated)
{
    std::lock_guard lock(Mutex_);
    if (auto error = AdmitLocked(path)) {
        return error;
    }
    const auto found = Files_.find(path);
    if (found == Files_.end()) {
        return FailLocked(path, std::make_error_code(std::errc::no_such_file_or_directory));
    }
    auto& record = found->second.Record;
    if (record.State != EFileState::DirSynced || record.Receipt || found->second.PendingWrites != 0 ||
        record.Length != length)
    {
        return FailLocked(path, std::make_error_code(std::errc::invalid_argument));
    }
    record.Receipt = TDurableFileReceipt{record.Path, record.Kind, length, digest, Spec_.AttemptId};
    record.Sha256 = digest;
    record.PhysicalBytes = std::max(length, allocated);
    return *record.Receipt;
}

std::error_code TIOAttempt::MarkFormatVerified(std::string_view path)
{
    std::lock_guard lock(Mutex_);
    if (FirstError_) {
        return FirstError_;
    }
    if (!Active_) {
        return std::make_error_code(std::errc::operation_canceled);
    }
    const auto found = Files_.find(path);
    if (found == Files_.end() || found->second.Record.State != EFileState::DirSynced || !found->second.Record.Receipt) {
        return FailLocked(path, std::make_error_code(std::errc::operation_not_permitted));
    }
    found->second.Record.FormatVerified = true;
    return {};
}

TResult<TWriteReservation> TIOAttempt::ReserveWrite(std::string_view path, std::uint64_t requested)
{
    std::lock_guard lock(Mutex_);
    if (auto error = AdmitLocked(path)) {
        return error;
    }
    const auto found = Files_.find(path);
    if (found == Files_.end()) {
        return FailLocked(path, std::make_error_code(std::errc::no_such_file_or_directory));
    }
    if (found->second.Record.State != EFileState::Open) {
        return FailLocked(path, std::make_error_code(std::errc::operation_not_permitted));
    }
    if (requested > Limits_.NewFileBytes - Budget_.NewFileBytes - Budget_.ReservedNewFileBytes ||
        requested > Limits_.IoBytes - Budget_.IoBytes)
    {
        return FailLocked(path, std::make_error_code(std::errc::no_buffer_space));
    }
    TWriteReservation reservation(shared_from_this(), std::string(path), requested);
    Budget_.ReservedNewFileBytes += requested;
    Budget_.IoBytes += requested;
    ++found->second.PendingWrites;
    return reservation;
}

std::error_code TIOAttempt::FinishWrite(std::string_view path,
    std::uint64_t requested,
    std::optional<std::uint64_t> accepted,
    std::optional<TSha256Digest> digest)
{
    std::lock_guard lock(Mutex_);
    Budget_.ReservedNewFileBytes -= requested;
    auto& entry = Files_.find(path)->second;
    --entry.PendingWrites;
    if (!accepted) {
        return FailLocked(path, std::make_error_code(std::errc::operation_canceled));
    }
    if (*accepted > requested) {
        return FailLocked(path, std::make_error_code(std::errc::invalid_argument));
    }
    Budget_.NewFileBytes += *accepted;
    entry.Record.Length += *accepted;
    if (*accepted != 0 || digest) {
        entry.Record.Sha256 = digest;
    }
    return FirstError_;
}

std::error_code TIOAttempt::ChargeRead(std::uint64_t requested)
{
    std::lock_guard lock(Mutex_);
    if (FirstError_) {
        return FirstError_;
    }
    if (!Active_) {
        return std::make_error_code(std::errc::operation_canceled);
    }
    if (requested > Limits_.IoBytes - Budget_.IoBytes) {
        return FailLocked({}, std::make_error_code(std::errc::no_buffer_space));
    }
    Budget_.IoBytes += requested;
    return {};
}

std::error_code TIOAttempt::CheckBinding() const
{
    std::lock_guard lock(Mutex_);
    if (FirstError_) {
        return FirstError_;
    }
    if (!Active_) {
        return std::make_error_code(std::errc::operation_canceled);
    }
    if (Sealed_) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    return Latch_->Error();
}

std::error_code TIOAttempt::CanRead(std::string_view path) const
{
    std::lock_guard lock(Mutex_);
    const auto found = Files_.find(path);
    if (found == Files_.end()) {
        return {};
    }
    if (found->second.Record.FirstError) {
        return found->second.Record.FirstError;
    }
    return found->second.Record.State == EFileState::DirSynced && found->second.Record.Receipt
        ? std::error_code{}
        : std::make_error_code(std::errc::operation_in_progress);
}

TResult<TFileRecord> TIOAttempt::Record(std::string_view path) const
{
    std::lock_guard lock(Mutex_);
    const auto found = Files_.find(path);
    if (found == Files_.end()) {
        return std::errc::no_such_file_or_directory;
    }
    return found->second.Record;
}

std::vector<TFileRecord> TIOAttempt::Records() const
{
    std::lock_guard lock(Mutex_);
    std::vector<TFileRecord> result;
    result.reserve(Files_.size());
    for (const auto& [path, entry] : Files_) {
        result.push_back(entry.Record);
    }
    return result;
}

std::vector<TDurableFileReceipt> TIOAttempt::Receipts() const
{
    std::lock_guard lock(Mutex_);
    std::vector<TDurableFileReceipt> result;
    for (const auto& [path, entry] : Files_) {
        if (entry.Record.Receipt) {
            result.push_back(*entry.Record.Receipt);
        }
    }
    return result;
}

TAttemptBudget TIOAttempt::Budget() const
{
    std::lock_guard lock(Mutex_);
    return Budget_;
}

std::error_code TIOAttempt::FirstError() const
{
    std::lock_guard lock(Mutex_);
    return FirstError_;
}

std::error_code TIOAttempt::Seal()
{
    std::lock_guard lock(Mutex_);
    if (FirstError_) {
        return FirstError_;
    }
    if (!Active_) {
        return FailLocked({}, std::make_error_code(std::errc::operation_canceled));
    }
    if (auto error = Latch_->Error()) {
        return FailLocked({}, error);
    }
    Sealed_ = true;
    for (const auto& [path, entry] : Files_) {
        if (entry.Record.State != EFileState::DirSynced || !entry.Record.Receipt || entry.PendingWrites != 0) {
            return FailLocked(path, std::make_error_code(std::errc::operation_in_progress));
        }
    }
    return {};
}

void TIOAttempt::Deactivate()
{
    std::lock_guard lock(Mutex_);
    Active_ = false;
    for (auto& [path, entry] : Files_) {
        if (entry.Record.State != EFileState::DirSynced || !entry.Record.Receipt || entry.PendingWrites != 0) {
            [[maybe_unused]] const auto recordedError =
                FailLocked(path, std::make_error_code(std::errc::operation_canceled));
        }
    }
}

const std::string& TIOAttempt::Id() const noexcept
{
    return Spec_.AttemptId;
}

const std::string& TIOAttempt::StoreId() const noexcept
{
    return Spec_.StoreId;
}

const std::string& TIOAttempt::CollectionId() const noexcept
{
    return Spec_.CollectionId;
}

EAttemptKind TIOAttempt::Kind() const noexcept
{
    return Spec_.Kind;
}

const std::shared_ptr<TWriteLatch>& TIOAttempt::Latch() const noexcept
{
    return Latch_;
}

}
