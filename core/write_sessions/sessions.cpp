#include "storage/write_session.h"

#include "core/api/bulk_input.h"
#include "core/api/kv_input.h"
#include "core/engine/engine_factory.h"
#include "core/write_sessions/journal.h"

#include <algorithm>
#include <arrow/record_batch.h>

namespace glamdb {
namespace {
using namespace detail;
template <class F>
auto Guard(F action) -> decltype(action())
{
    try {
        return action();
    } catch (const Failure& e) {
        return std::unexpected(e.detail);
    } catch (const std::bad_alloc&) {
        return std::unexpected(ErrorDetail{"RESOURCE_EXHAUSTED", "staging allocation failed"});
    } catch (const std::exception&) {
        return std::unexpected(ErrorDetail{"IO_ERROR", "staging operation failed"});
    }
}
std::string SelectionBytes(const std::vector<TWritePart>& selected)
{
    Encoder out;
    out.Number<uint32_t>(static_cast<uint32_t>(selected.size()));
    for (const auto p : selected) {
        out.Number(p.Task);
        out.Number(p.Attempt);
        out.Number(p.Part);
    }
    return out.bytes;
}
std::vector<TWritePart> ReadSelection(const std::string& bytes)
{
    if (bytes.empty()) {
        return {};
    }
    Decoder in{bytes};
    const auto count = in.Number<uint32_t>();
    Require(count <= 4096, "CORRUPTION", "invalid selection count");
    std::vector<TWritePart> result;
    result.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        result.push_back({in.Number<uint32_t>(), in.Number<uint32_t>(), in.Number<uint32_t>()});
    }
    in.End();
    return result;
}
struct TSession
{
    SchemaSpec Schema;
    MutationKind Mutation;
    SnapshotPrecondition Precondition;
    int64_t Lifetime;
    TWriteStatus Status;
};

class TStagedSource final : public BulkAppendSource
{
public:
    TStagedSource(Database& db, std::string id, std::vector<TWritePart> selected, SchemaSpec schema)
        : DB_(db)
        , ID_(std::move(id))
        , Selected_(std::move(selected))
        , Schema_(std::move(schema))
    { }
    Status Rewind() override
    {
        Part_ = 0;
        Ordinal_ = 0;
        Rows_ = 0;
        Hash_ = {};
        return {};
    }
    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        return Guard([&]() -> Result<std::shared_ptr<arrow::RecordBatch>> {
            while (Part_ < Selected_.size()) {
                auto data = DB_.Query("SELECT payload FROM batches WHERE session=? AND part=? AND ordinal=?",
                    {ID_, PartKey(Selected_[Part_]), Ordinal_});
                if (data.empty()) {
                    auto part = DB_.Query("SELECT rows,hash FROM parts WHERE session=? AND part=?",
                        {ID_, PartKey(Selected_[Part_])});
                    Hash_.AddNumber(Rows_);
                    Require(part.size() == 1 && ParseInteger(Required(part[0], 0)) == Rows_ &&
                            Required(part[0], 1) == Hash_.ComputeDigest(),
                        "CORRUPTION",
                        "acknowledged part content changed");
                    ++Part_;
                    Ordinal_ = 0;
                    Rows_ = 0;
                    Hash_ = {};
                    continue;
                }
                ++Ordinal_;
                auto batch = DecodeSessionBatch(Required(data[0], 0));
                SessionTake(UpdateFingerprintBatches(Schema_, {batch}, Hash_));
                Rows_ += batch->num_rows();
                return batch;
            }
            return nullptr;
        });
    }

private:
    Database& DB_;
    std::string ID_;
    std::vector<TWritePart> Selected_;
    size_t Part_ = 0;
    int64_t Ordinal_ = 0;
    SchemaSpec Schema_;
    int64_t Rows_ = 0;
    TCanonicalHasher Hash_;
};
}

struct TWriteSessions::TImpl
{
    std::shared_ptr<Engine> EngineOwner;
    TWriteJournal Journal;
    std::mutex Mutex;
    TImpl(std::shared_ptr<Engine> engine, const std::string& path, const std::string& scope, TWriteSessionLimits limits)
        : EngineOwner(std::move(engine))
        , Journal(path, EngineOwner->GetResourceOwner(), EngineOwner->GetStoreInfo().store_id, scope, limits)
    { }
    TSession Get(const TWriteIdentity& id, bool retain = false)
    {
        std::shared_ptr<ResourceReservation> lease;
        if (retain) {
            lease = SessionTake(EngineOwner->GetResourceOwner()->Reserve(ResourceClass::Metadata, 128ULL << 10));
        }
        auto rows =
            Journal.DB.Query("SELECT schema,mutation,precondition,snapshot,lifetime,expires,state,rows,hash,selection "
                             "FROM sessions WHERE id=?",
                {WriteKey(id)});
        Require(rows.size() == 1, "NOT_FOUND", "write session not found");
        const auto& row = rows[0];
        TSession s;
        s.Schema = SessionTake(DecodeSchema(Required(row, 0)));
        s.Mutation = static_cast<MutationKind>(ParseInteger(Required(row, 1)));
        s.Precondition = {static_cast<SnapshotPreconditionKind>(ParseInteger(Required(row, 2))),
            ParseInteger(Required(row, 3))};
        s.Lifetime = ParseInteger(Required(row, 4));
        s.Status = {std::move(lease),
            static_cast<EWriteState>(ParseInteger(Required(row, 6))),
            ParseInteger(Required(row, 5)),
            ParseInteger(Required(row, 7)),
            Required(row, 8),
            ReadSelection(Required(row, 9))};
        Require(s.Status.State <= EWriteState::Reclaimed &&
                (s.Mutation == MutationKind::None || s.Mutation == MutationKind::Upsert),
            "CORRUPTION",
            "invalid stored write state");
        return s;
    }
    std::shared_ptr<ResourceReservation> Workspace()
    {
        return SessionTake(EngineOwner->GetResourceOwner()->Reserve(ResourceClass::Data, 256ULL << 20));
    }
    BulkAppendRequest Request(const TWriteIdentity& id, const TSession& s)
    {
        return {id.CollectionID,
            id.OperationID,
            s.Precondition,
            s.Status.Rows,
            std::make_shared<TStagedSource>(Journal.DB, WriteKey(id), s.Status.Selected, s.Schema)};
    }
    std::string Hash(const TWriteIdentity& id, const TSession& s)
    {
        auto request = Request(id, s);
        auto hash = SessionTake(
            FingerprintBulkSource(s.Schema, request, std::chrono::steady_clock::now() + std::chrono::minutes(10)));
        if (s.Mutation == MutationKind::Upsert) {
            hash = SessionTake(KvPayloadHash(s.Schema, hash, TKvInput{}));
        }
        return hash;
    }
    std::optional<AppendOutcome> Outcome(const TWriteIdentity& id, const TSession& s)
    {
        return SessionTake(
            EngineFactory::FindWriteOutcome(*EngineOwner, id.CollectionID, id.OperationID, s.Status.PayloadHash));
    }
    void Mark(const TWriteIdentity& id, EWriteState state)
    {
        Journal.Transaction([&] {
            Journal.DB.Execute("UPDATE sessions SET state=? WHERE id=?", {static_cast<int64_t>(state), WriteKey(id)});
        });
    }
    void Abort(const TWriteIdentity& id, const TSession& s)
    {
        if (s.Status.State == EWriteState::Aborted) {
            return;
        }
        Require(s.Status.State != EWriteState::Reclaimed, "INVALID_STATE", "published write cannot be aborted");
        auto outcome = Outcome(id, s);
        Require(!outcome || outcome->disposition == CommitDisposition::DefinitelyNotCommitted,
            "INVALID_STATE",
            "publication is committed or unresolved");
        Require(s.Status.State != EWriteState::CommitStarted || outcome.has_value(),
            "OPERATION_IN_PROGRESS",
            "commit started; retain payload until authoritative resolution");
        Mark(id, EWriteState::Aborted);
    }
};

TWriteSessions::TWriteSessions(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{ }
TWriteSessions::~TWriteSessions() = default;
Result<std::shared_ptr<TWriteSessions>> TWriteSessions::Open(std::shared_ptr<Engine> engine,
    const std::string& path,
    const std::string& scope,
    TWriteSessionLimits limits)
{
    return Guard([&]() -> Result<std::shared_ptr<TWriteSessions>> {
        Require(engine != nullptr && !scope.empty() && scope.size() <= 256 && ValidUtf8(scope),
            "INVALID_ARGUMENT",
            "write sessions require an engine and bounded scope");
        Require(engine->GetCapabilities().backend == BackendKind::LocalStorage,
            "UNSUPPORTED_OPERATION",
            "write sessions require native local storage");
        SessionTake(EngineFactory::ValidateStagingDirectory(*engine, path));
        return std::shared_ptr<TWriteSessions>(
            new TWriteSessions(std::make_unique<TImpl>(std::move(engine), path, scope, limits)));
    });
}
Result<TWriteStatus> TWriteSessions::BeginWrite(const TBeginWrite& request)
{
    return Guard([&]() -> Result<TWriteStatus> {
        std::unique_lock lock(Impl_->Mutex, std::try_to_lock);
        Require(lock.owns_lock(), "OPERATION_IN_PROGRESS", "another staging call is active");
        const auto id = WriteKey(request.Identity);
        Require(request.Mutation == MutationKind::None || request.Mutation == MutationKind::Upsert,
            "UNSUPPORTED_OPERATION",
            "only append and last-wins upsert can be staged");
        Require(request.Lifetime.count() > 0 && request.Lifetime <= std::chrono::hours(24) &&
                request.Precondition.kind <= SnapshotPreconditionKind::Equals &&
                (request.Precondition.kind == SnapshotPreconditionKind::Equals ||
                    request.Precondition.snapshot_id == 0),
            "INVALID_ARGUMENT",
            "invalid write lifetime or precondition");
        auto& journal = Impl_->Journal;
        if (!journal.DB.Query("SELECT id FROM sessions WHERE id=?", {id}).empty()) {
            auto old = Impl_->Get(request.Identity, true);
            Require(old.Mutation == request.Mutation && old.Precondition.kind == request.Precondition.kind &&
                    old.Precondition.snapshot_id == request.Precondition.snapshot_id &&
                    old.Lifetime == request.Lifetime.count(),
                "IDEMPOTENCY_CONFLICT",
                "write identity already has different begin parameters");
            return old.Status;
        }
        journal.RequireUploadCapacity();
        Require(journal.DB.Integer("SELECT count(*) FROM sessions") < journal.Limits.MaxSessions,
            "RESOURCE_EXHAUSTED",
            "session inventory full");
        const auto collection = SessionTake(Impl_->EngineOwner->GetCollection(request.Identity.CollectionID));
        Require(collection.mode ==
                (request.Mutation == MutationKind::None ? CollectionMode::Append : CollectionMode::KeyValue),
            "INVALID_ARGUMENT",
            "mutation does not match collection mode");
        const auto schema = SessionTake(EncodeSchema(collection.schema));
        journal.Transaction(
            [&] {
                journal.DB.Execute("INSERT INTO sessions(id,schema,mutation,precondition,snapshot,lifetime,expires) "
                                   "VALUES(?,?,?,?,?,?,?)",
                    {id,
                        Blob{schema},
                        static_cast<int64_t>(request.Mutation),
                        static_cast<int64_t>(request.Precondition.kind),
                        request.Precondition.snapshot_id,
                        request.Lifetime.count(),
                        SessionNow() + request.Lifetime.count()});
            },
            false);
        return Impl_->Get(request.Identity, true).Status;
    });
}
Result<TWritePartReceipt> TWriteSessions::PutPart(const TWriteIdentity& identity,
    TWritePart part,
    TNext next,
    std::shared_ptr<const std::atomic<bool>> cancellation)
{
    return Guard([&]() -> Result<TWritePartReceipt> {
        std::unique_lock lock(Impl_->Mutex, std::try_to_lock);
        Require(lock.owns_lock(), "OPERATION_IN_PROGRESS", "another staging call is active");
        auto s = Impl_->Get(identity);
        const auto id = WriteKey(identity), key = PartKey(part);
        auto& journal = Impl_->Journal;
        auto existing = journal.DB.Query("SELECT rows,hash FROM parts WHERE session=? AND part=?", {id, key});
        Require(s.Status.State != EWriteState::Aborted && s.Status.State != EWriteState::Reclaimed,
            "INVALID_STATE",
            "session no longer accepts parts");
        Require(!existing.empty() ||
                (s.Status.State == EWriteState::Uploading && SessionNow() < s.Status.ExpiresAtSeconds),
            "INVALID_STATE",
            "session is sealed or expired");
        Require(static_cast<bool>(next), "INVALID_ARGUMENT", "missing one-pass source");
        journal.RequireUploadCapacity();
        auto workspace = Impl_->Workspace();
        TWritePartReceipt receipt;
        receipt.ResourceLease =
            SessionTake(Impl_->EngineOwner->GetResourceOwner()->Reserve(ResourceClass::Control, 512));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(10);
        journal.Transaction(
            [&] {
                TCanonicalHasher hash;
                int64_t batches = journal.DB.Integer("SELECT count(*) FROM batches");
                int64_t bytes = journal.DB.Integer("SELECT coalesce(sum(length(payload)),0) FROM batches");
                int64_t ordinal = 0;
                if (existing.empty()) {
                    Require(journal.DB.Integer("SELECT count(*) FROM parts") < journal.Limits.MaxParts,
                        "RESOURCE_EXHAUSTED",
                        "part inventory full");
                }
                for (;;) {
                    Require(std::chrono::steady_clock::now() < deadline, "DEADLINE_EXCEEDED", "part deadline expired");
                    Require(!cancellation || !cancellation->load(), "CANCELLED", "part upload cancelled");
                    auto batch = SessionTake(next());
                    Require(std::chrono::steady_clock::now() < deadline, "DEADLINE_EXCEEDED", "part deadline expired");
                    Require(!cancellation || !cancellation->load(), "CANCELLED", "part upload cancelled");
                    if (!batch) {
                        break;
                    }
                    Require(++ordinal <= journal.Limits.MaxBatches && batch->num_rows() > 0 &&
                            batch->num_rows() <= BulkAppendLimits::MaxRows - receipt.Rows,
                        "RESOURCE_EXHAUSTED",
                        "part row or batch limit exceeded");
                    auto normalized = SessionTake(NormalizeBatches(s.Schema, identity.CollectionID, {batch}));
                    SessionTake(UpdateFingerprintBatches(s.Schema, {normalized.Columns}, hash));
                    receipt.Rows += batch->num_rows();
                    if (existing.empty()) {
                        auto ipc = EncodeSessionBatch(normalized.Columns);
                        Require(++batches <= journal.Limits.MaxBatches &&
                                ipc.size() <= journal.Limits.MaxSpoolBytes - bytes,
                            "RESOURCE_EXHAUSTED",
                            "staging spool quota full");
                        bytes += static_cast<int64_t>(ipc.size());
                        journal.DB.Execute("INSERT INTO batches VALUES(?,?,?,?)",
                            {id, key, ordinal - 1, Blob{std::move(ipc)}});
                    }
                }
                Require(!existing.empty() || SessionNow() < s.Status.ExpiresAtSeconds,
                    "INVALID_STATE",
                    "session expired during upload");
                Require(receipt.Rows > 0, "INVALID_ARGUMENT", "empty part");
                hash.AddNumber(receipt.Rows);
                receipt.CanonicalHash = hash.ComputeDigest();
                if (existing.empty()) {
                    journal.DB.Execute("INSERT INTO parts VALUES(?,?,?,?)",
                        {id, key, receipt.Rows, receipt.CanonicalHash});
                } else {
                    Require(ParseInteger(Required(existing[0], 0)) == receipt.Rows &&
                            Required(existing[0], 1) == receipt.CanonicalHash,
                        "IDEMPOTENCY_CONFLICT",
                        "part identity already has different canonical content");
                }
            },
            false);
        return receipt;
    });
}
Result<std::optional<TWritePartReceipt>> TWriteSessions::GetPartReceipt(const TWriteIdentity& identity, TWritePart part)
{
    return Guard([&]() -> Result<std::optional<TWritePartReceipt>> {
        std::unique_lock lock(Impl_->Mutex, std::try_to_lock);
        Require(lock.owns_lock(), "OPERATION_IN_PROGRESS", "another staging call is active");
        (void)Impl_->Get(identity);
        auto lease = SessionTake(Impl_->EngineOwner->GetResourceOwner()->Reserve(ResourceClass::Control, 512));
        auto row = Impl_->Journal.DB.Query("SELECT rows,hash FROM parts WHERE session=? AND part=?",
            {WriteKey(identity), PartKey(part)});
        if (row.empty()) {
            return std::nullopt;
        }
        return TWritePartReceipt{std::move(lease), ParseInteger(Required(row[0], 0)), Required(row[0], 1)};
    });
}
Result<TWriteStatus> TWriteSessions::SealWrite(const TWriteIdentity& identity, std::vector<TWritePart> selected)
{
    return Guard([&]() -> Result<TWriteStatus> {
        std::unique_lock lock(Impl_->Mutex, std::try_to_lock);
        Require(lock.owns_lock(), "OPERATION_IN_PROGRESS", "another staging call is active");
        auto s = Impl_->Get(identity, true);
        Require(!selected.empty() && selected.size() <= Impl_->Journal.Limits.MaxParts,
            "INVALID_ARGUMENT",
            "seal requires a bounded nonempty selection");
        std::ranges::sort(selected);
        for (size_t i = 1; i < selected.size(); ++i) {
            Require(selected[i] != selected[i - 1] &&
                    (selected[i].Task != selected[i - 1].Task || selected[i].Attempt == selected[i - 1].Attempt),
                "INVALID_ARGUMENT",
                "select one attempt per task and unique parts");
        }
        if (s.Status.State != EWriteState::Uploading) {
            Require(s.Status.State != EWriteState::Aborted && s.Status.Selected == selected,
                "IDEMPOTENCY_CONFLICT",
                "selection differs from immutable seal");
            return s.Status;
        }
        Require(SessionNow() < s.Status.ExpiresAtSeconds, "INVALID_STATE", "session expired");
        s.Status.Selected = std::move(selected);
        for (const auto part : s.Status.Selected) {
            auto rows = Impl_->Journal.DB.Query("SELECT rows FROM parts WHERE session=? AND part=?",
                {WriteKey(identity), PartKey(part)});
            Require(rows.size() == 1, "NOT_FOUND", "selected part not acknowledged");
            auto count = ParseInteger(Required(rows[0], 0));
            Require(count > 0 && count <= BulkAppendLimits::MaxRows - s.Status.Rows,
                "RESOURCE_EXHAUSTED",
                "sealed row limit exceeded");
            s.Status.Rows += count;
        }
        auto workspace = Impl_->Workspace();
        s.Status.PayloadHash = Impl_->Hash(identity, s);
        Require(SessionNow() < s.Status.ExpiresAtSeconds, "INVALID_STATE", "session expired during seal");
        Impl_->Journal.Transaction([&] {
            Impl_->Journal.DB.Execute("UPDATE sessions SET state=1,rows=?,hash=?,selection=? WHERE id=?",
                {s.Status.Rows, s.Status.PayloadHash, Blob{SelectionBytes(s.Status.Selected)}, WriteKey(identity)});
        });
        s.Status.State = EWriteState::Sealed;
        return s.Status;
    });
}
Result<AppendOutcome> TWriteSessions::CommitWrite(const TWriteIdentity& identity,
    std::shared_ptr<const std::atomic<bool>> cancellation)
{
    return Guard([&]() -> Result<AppendOutcome> {
        std::unique_lock lock(Impl_->Mutex, std::try_to_lock);
        Require(lock.owns_lock(), "OPERATION_IN_PROGRESS", "another staging call is active");
        auto s = Impl_->Get(identity);
        Require(s.Status.State == EWriteState::Sealed || s.Status.State == EWriteState::CommitStarted ||
                s.Status.State == EWriteState::Reclaimed,
            "INVALID_STATE",
            "commit requires a sealed session");
        // Even after payload cleanup, compare the immutable original hash against
        // the catalog binding, including permanent failure receipts without hashes.
        if (auto outcome = Impl_->Outcome(identity, s)) {
            return *outcome;
        }
        Require(s.Status.State != EWriteState::Reclaimed, "CORRUPTION", "reclaimed write has no terminal receipt");
        auto workspace = Impl_->Workspace();
        Require(Impl_->Hash(identity, s) == s.Status.PayloadHash, "CORRUPTION", "sealed payload changed");
        Impl_->Mark(identity, EWriteState::CommitStarted);
        auto request = Impl_->Request(identity, s);
        request.cancellation = std::move(cancellation);
        return s.Mutation == MutationKind::None ? Impl_->EngineOwner->BulkAppendDurable(request)
                                                : Impl_->EngineOwner->BulkUpsertDurable(request);
    });
}
Result<TWriteStatus> TWriteSessions::GetWriteStatus(const TWriteIdentity& identity)
{
    return Guard([&]() -> Result<TWriteStatus> {
        std::unique_lock lock(Impl_->Mutex, std::try_to_lock);
        Require(lock.owns_lock(), "OPERATION_IN_PROGRESS", "another staging call is active");
        return Impl_->Get(identity, true).Status;
    });
}
Result<std::optional<AppendOutcome>> TWriteSessions::GetWriteOutcome(const TWriteIdentity& identity)
{
    return Guard([&]() -> Result<std::optional<AppendOutcome>> {
        std::unique_lock lock(Impl_->Mutex, std::try_to_lock);
        Require(lock.owns_lock(), "OPERATION_IN_PROGRESS", "another staging call is active");
        auto s = Impl_->Get(identity);
        Require(!s.Status.PayloadHash.empty(),
            "INVALID_STATE",
            "unsealed session has no authoritative output identity");
        return Impl_->Outcome(identity, s);
    });
}
Status TWriteSessions::AbortWrite(const TWriteIdentity& identity)
{
    return Guard([&]() -> Status {
        std::unique_lock lock(Impl_->Mutex, std::try_to_lock);
        Require(lock.owns_lock(), "OPERATION_IN_PROGRESS", "another staging call is active");
        Impl_->Abort(identity, Impl_->Get(identity));
        return {};
    });
}
Status TWriteSessions::Cleanup()
{
    return Guard([&]() -> Status {
        std::unique_lock lock(Impl_->Mutex, std::try_to_lock);
        Require(lock.owns_lock(), "OPERATION_IN_PROGRESS", "another staging call is active");
        auto& journal = Impl_->Journal;
        for (const auto& row : journal.DB.Query("SELECT id FROM sessions")) {
            const auto key = Required(row, 0);
            Require(key.size() == 73 && key[36] == '/', "CORRUPTION", "invalid stored write identity");
            const TWriteIdentity id{key.substr(0, 36), key.substr(37)};
            auto s = Impl_->Get(id);
            bool reclaim = s.Status.State == EWriteState::Aborted || s.Status.State == EWriteState::Reclaimed;
            if (!reclaim && (s.Status.State == EWriteState::Uploading || s.Status.State == EWriteState::Sealed) &&
                SessionNow() >= s.Status.ExpiresAtSeconds)
            {
                Impl_->Abort(id, s);
                reclaim = true;
            }
            if (!reclaim && !s.Status.PayloadHash.empty()) {
                auto outcome = EngineFactory::FindWriteOutcome(*Impl_->EngineOwner,
                    id.CollectionID,
                    id.OperationID,
                    s.Status.PayloadHash);
                if (outcome && *outcome && (*outcome)->disposition != CommitDisposition::CommitUnknown) {
                    Impl_->Mark(id, EWriteState::Reclaimed);
                    reclaim = true;
                }
            }
            if (!reclaim) {
                continue;
            }
            // One bounded batch per transaction: emergency space also covers cleanup.
            for (;;) {
                const auto batch = journal.DB.Query("SELECT part,ordinal FROM batches WHERE session=? LIMIT 1", {key});
                if (batch.empty()) {
                    break;
                }
                journal.Transaction([&] {
                    journal.DB.Execute("DELETE FROM batches WHERE session=? AND part=? AND ordinal=?",
                        {key, Required(batch[0], 0), ParseInteger(Required(batch[0], 1))});
                });
            }
            journal.Transaction([&] {
                journal.DB.Execute("DELETE FROM parts WHERE session=?", {key});
            });
        }
        return {};
    });
}
} // namespace glamdb
