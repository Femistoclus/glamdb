#include "apps/server/scans.h"
#include "apps/transport/arrow_ipc.h"

#include "storage/resources.h"

#include <arrow/ipc/api.h>
#include <arrow/util/byte_size.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>

namespace glamdb::NServer {
using namespace NTransport;
namespace {
#if defined(__cpp_lib_atomic_shared_ptr) && __cpp_lib_atomic_shared_ptr >= 201711L
template <class T>
using TAtomicSharedPtr = std::atomic<std::shared_ptr<T>>;
#else
// libc++ 18 supports shared_ptr atomic free functions without the specialization.
template <class T>
using TAtomicSharedPtr = std::shared_ptr<T>;
#endif

constexpr uint64_t ReplayMemoryBytes = 2 * MaxResponseBytes + (64ULL << 10);
constexpr std::array RejectionNames{"global_live_limit",
    "database_live_limit",
    "global_replay_limit",
    "database_replay_limit",
    "shared_data_memory",
    "shared_control_memory",
    "shared_planning_memory",
    "shared_decode_memory",
    "session_busy"};
constexpr size_t PageRows = 10000;
constexpr size_t PageBytes = size_t{4} * 1024 * 1024;
int64_t Now()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
std::string Token()
{
    std::random_device random;
    std::string value;
    constexpr char Hex[] = "0123456789abcdef";
    for (size_t i = 0; i < 32; ++i) {
        value += Hex[random() & 15];
    }
    return value;
}
TJson Snapshot(const ScanInfo& info)
{
    return info.selector_resolved.kind == SnapshotSelectorKind::Id
        ? TJson(std::to_string(info.selector_resolved.snapshot_id))
        : TJson(nullptr);
}
template <class T>
T ArrowValue(arrow::Result<T> value)
{
    Require(value.ok(), "cannot assemble Arrow scan page", "RESOURCE_EXHAUSTED");
    return std::move(value).ValueUnsafe();
}
int64_t IpcSize(const arrow::RecordBatch& batch)
{
    int64_t size = 0;
    Require(arrow::ipc::GetRecordBatchSize(batch, &size).ok(), "cannot size Arrow scan page", "INTERNAL");
    return size;
}
} // namespace

TScanAdmissionFailure::TScanAdmissionFailure(EScanRejection reason)
    : TFailure({"RESOURCE_EXHAUSTED",
          std::string("scan admission refused: ") + RejectionNames.at(static_cast<size_t>(reason)) +
              "; cancel unused cursors, wait for expiry or increase the shared profile",
          RetryHint::SameIdAfterReconcile})
    , Reason(reason)
{ }
TResponse ScanAdmissionResponse(const TScanAdmissionFailure& failure)
{
    auto body = EncodeError(failure.Detail);
    body["error"]["reason"] = RejectionNames.at(static_cast<size_t>(failure.Reason));
    body["error"]["retry_after_ms"] = 1000;
    return {429, body.dump(), "application/json", {{"Retry-After", "1"}}};
}
void ValidateScanLimits(const TScanLimits& limits, const ResourceLimits* resources)
{
    Require(limits.Live > 0 && limits.Live <= 4096 && limits.LivePerDatabase > 0 &&
            limits.LivePerDatabase <= limits.Live,
        "scan live limits must be 1..4096 with per-database <= global");
    Require(limits.Completed <= 4096 && limits.CompletedPerDatabase <= limits.Completed,
        "scan completed limits must be 0..4096 with per-database <= global");
    Require(limits.ReplayBytes >= MaxResponseBytes && limits.ReplayBytes <= (1ULL << 40) &&
            limits.ReplayBytesPerDatabase >= MaxResponseBytes && limits.ReplayBytesPerDatabase <= limits.ReplayBytes,
        "scan replay limits must be 16 MiB..1 TiB with per-database <= global");
    Require(limits.Idle.count() > 0 && limits.Lifetime.count() >= 0 && limits.CompletedTtl.count() > 0 &&
            limits.Idle <= std::chrono::hours(24) && limits.Lifetime <= std::chrono::hours(24 * 365) &&
            limits.CompletedTtl <= std::chrono::hours(24),
        "scan idle/completed TTL must be positive and <= 24h; lifetime must be 0..365d");
    if (resources) {
        const auto readers =
            std::min<uint64_t>(limits.LivePerDatabase, limits.ReplayBytesPerDatabase / MaxResponseBytes);
        const auto& memory = resources->memory_bytes;
        Require(readers * ReplayMemoryBytes + (320ULL << 20) <= memory[0] &&
                readers * (272ULL << 20) + (768ULL << 20) <= memory[1] && readers * (32ULL << 10) <= memory[2],
            "scan per-database profile exceeds shared data, metadata or control memory; lower quotas or increase "
            "resources");
    }
}

struct TScans::TImpl
{
    struct TSession
    {
        std::shared_ptr<ResourceOwner> Resources;
        std::shared_ptr<ResourceReservation> DescriptorMemory;
        std::shared_ptr<ResourceReservation> ReplayMemory;
        std::mutex Mutex;
        std::string Database, Collection, NextToken, ReplayToken;
        bool Arrow = false, Ended = false;
        std::atomic<bool> Cancelled{false}, Complete{false}, Busy{false};
        const int64_t Created = Now();
        std::atomic<int64_t> Touched{Created};
        TAtomicSharedPtr<ScanCursor> Cursor;
        std::deque<std::shared_ptr<arrow::RecordBatch>> Pending;
        std::deque<std::shared_ptr<arrow::RecordBatch>>* Rollback = nullptr;
        ScanInfo Info;
        TJson Schema;
        TResponse Replay;
        size_t ChargedBytes = 0;
        std::atomic<size_t> BodyBytes{0};
        ~TSession()
        {
            if (const auto cursor = std::atomic_load(&Cursor)) {
                cursor->Cancel();
            }
        }
    };
    using TPtr = std::shared_ptr<TSession>;
    // Keep every batch fetched from core until page advancement commits. A
    // retryable decoder refusal can then restore the whole unconsumed page.
    struct TPageTransaction
    {
        TSession& Session;
        std::deque<std::shared_ptr<arrow::RecordBatch>> Saved;
        bool Committed = false;
        explicit TPageTransaction(TSession& session)
            : Session(session)
            , Saved(session.Pending)
        {
            Session.Rollback = &Saved;
        }
        ~TPageTransaction()
        {
            Session.Rollback = nullptr;
            if (!Committed) {
                Session.Pending.swap(Saved);
            }
        }
    };
    // Registry protection never covers planning, cursor I/O, encoding, hashing,
    // response copying or waiting for a session mutex.
    std::mutex Mutex;
    std::condition_variable Wake;
    bool Stopping = false, Accepting = true;
    std::vector<TPtr> Sessions;
    std::unordered_map<std::string, TPtr> Tokens;
    const TScanLimits Limits;
    const std::shared_ptr<ResourceOwner> Resources;
    std::array<uint64_t, static_cast<size_t>(EScanRejection::Count)> Rejections{};
    uint64_t CompletedEvictions = 0, Expirations = 0, Cancellations = 0;
    std::thread Reaper;

    struct TLookup
    {
        const std::string& Database;
        const std::string& Collection;
        const std::string& Token;
    };
    explicit TImpl(TScanLimits limits, std::shared_ptr<ResourceOwner> resources)
        : Limits(limits)
        , Resources(std::move(resources))
        , Reaper([this]() noexcept {
            for (;;) {
                try {
                    std::unique_lock lock(Mutex);
                    if (Wake.wait_for(lock,
                            std::min({Limits.Idle, Limits.CompletedTtl, std::chrono::milliseconds(1000)}),
                            [this] {
                                return Stopping;
                            }))
                    {
                        return;
                    }
                    lock.unlock();
                    Sweep();
                } catch (...) {
                    // Allocation or backend cleanup failures must not escape a
                    // background thread. The next bounded wait retries cleanup
                    // and observes shutdown with fresh, well-defined lock ownership.
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
        })
    { }
    ~TImpl()
    {
        {
            std::lock_guard lock(Mutex);
            Stopping = true;
        }
        Wake.notify_all();
        Reaper.join();
        // Member-owned sessions cancel through RAII after the reaper joins.
    }
    bool Expired(const TSession& session) const
    {
        const auto now = Now();
        const auto idle = session.Complete.load() ? Limits.CompletedTtl : Limits.Idle;
        return (Limits.Lifetime.count() > 0 && now - session.Created >= Limits.Lifetime.count()) ||
            (!session.Busy.load() && now - session.Touched.load() >= idle.count());
    }
    void CheckActive(const TSession& session) const
    {
        Require(!session.Cancelled.load() && !Expired(session), "scan cursor not found or expired", "NOT_FOUND");
    }
    void MarkLocked(const TPtr& session)
    {
        session->Cancelled = true;
        std::erase_if(Tokens, [&](const auto& item) {
            return item.second == session;
        });
        session->ChargedBytes = 0;
        std::erase(Sessions, session);
    }
    // Caller owns the session mutex. Keep a shared reference until after the
    // registry lock is released, so buffer/cursor destruction cannot hold it.
    void Release(const TPtr& session)
    {
        session->Cancelled = true;
        if (const auto cursor = std::atomic_exchange(&session->Cursor, std::shared_ptr<ScanCursor>{})) {
            cursor->Cancel();
        }
        session->Pending.clear();
        session->Replay = {};
        session->BodyBytes = 0;
        session->Info = {};
        session->Schema = {};
        session->ReplayMemory.reset();
        std::lock_guard lock(Mutex);
        MarkLocked(session);
    }
    void Sweep()
    {
        std::vector<TPtr> expired;
        {
            std::lock_guard lock(Mutex);
            for (const auto& session : Sessions) {
                if (session->Cancelled.load() || Expired(*session)) {
                    expired.push_back(session);
                }
            }
            for (const auto& session : expired) {
                ++Expirations;
                MarkLocked(session);
            }
        }
        for (const auto& session : expired) {
            if (const auto cursor = std::atomic_load(&session->Cursor)) {
                cursor->Cancel();
            }
            std::unique_lock lock(session->Mutex, std::try_to_lock);
            if (lock.owns_lock()) {
                Release(session);
            }
        }
    }
    void Clear(const std::string* database)
    {
        std::vector<TPtr> removed;
        {
            std::lock_guard lock(Mutex);
            for (const auto& session : Sessions) {
                if (!database || session->Database == *database) {
                    removed.push_back(session);
                }
            }
            for (const auto& session : removed) {
                MarkLocked(session);
            }
        }
        for (const auto& session : removed) {
            if (const auto cursor = std::atomic_load(&session->Cursor)) {
                cursor->Cancel();
            }
            std::lock_guard lock(session->Mutex);
            Release(session);
        }
    }
    TPtr Find(const TLookup& key)
    {
        Require(key.Token.size() == 32, "scan cursor not found or expired", "NOT_FOUND");
        std::lock_guard lock(Mutex);
        const auto found = Tokens.find(key.Token);
        Require(found != Tokens.end() && found->second->Database == key.Database &&
                found->second->Collection == key.Collection,
            "scan cursor not found or expired",
            "NOT_FOUND");
        return found->second;
    }
    struct TOccupancy
    {
        size_t Live = 0, Completed = 0, Executing = 0, Reserved = 0, Body = 0;
        TJson Encode() const
        {
            return {{"live_cursors", Live},
                {"completed_replays", Completed},
                {"executing_calls", Executing},
                {"replay_reserved_bytes", Reserved},
                {"replay_body_bytes", Body}};
        }
    };
    TOccupancy CountLocked(const std::string* database = nullptr) const
    {
        TOccupancy count;
        for (const auto& session : Sessions) {
            if (!database || session->Database == *database) {
                if (session->Complete.load()) {
                    ++count.Completed;
                } else {
                    ++count.Live;
                }
                count.Executing += session->Busy.load() ? 1 : 0;
                count.Reserved += session->ChargedBytes;
                count.Body += session->BodyBytes.load();
            }
        }
        return count;
    }
    [[noreturn]] void RefuseLocked(EScanRejection reason)
    {
        ++Rejections.at(static_cast<size_t>(reason));
        throw TScanAdmissionFailure(reason);
    }
    [[noreturn]] void Refuse(EScanRejection reason)
    {
        std::lock_guard lock(Mutex);
        RefuseLocked(reason);
    }
    std::shared_ptr<ResourceReservation> Reserve(const std::shared_ptr<ResourceOwner>& resources,
        ResourceClass resource,
        uint64_t bytes,
        EScanRejection reason,
        bool reclaim = false)
    {
        for (;;) {
            auto reservation = resources->Reserve(resource, bytes);
            if (reservation) {
                return std::move(*reservation);
            }
            if (reclaim && EvictCompleted(nullptr)) {
                continue;
            }
            Refuse(reason);
        }
    }
    // Remove only the oldest completed entry in the pressured scope. Never
    // evict a live cursor. Destruction and cancellation stay outside the lock.
    bool EvictCompleted(const std::string* database, const TPtr& except = {})
    {
        TPtr candidate;
        {
            std::lock_guard lock(Mutex);
            for (const auto& session : Sessions) {
                if (session != except && session->Complete.load() && !session->Busy.load() &&
                    (!database || session->Database == *database) &&
                    (!candidate || session->Touched.load() < candidate->Touched.load()))
                {
                    candidate = session;
                }
            }
            if (!candidate) {
                return false;
            }
            ++CompletedEvictions;
            MarkLocked(candidate);
        }
        std::unique_lock lock(candidate->Mutex, std::try_to_lock);
        if (lock.owns_lock()) {
            Release(candidate);
        }
        return true;
    }
    void Register(const TPtr& session)
    {
        Sweep();
        for (;;) {
            std::optional<EScanRejection> reason;
            {
                std::lock_guard lock(Mutex);
                Require(Accepting, "server is stopping", "CANCELLED");
                const auto global = CountLocked();
                const auto local = CountLocked(&session->Database);
                if (global.Live >= Limits.Live) {
                    RefuseLocked(EScanRejection::GlobalLive);
                }
                if (local.Live >= Limits.LivePerDatabase) {
                    RefuseLocked(EScanRejection::DatabaseLive);
                }
                if (MaxResponseBytes > Limits.ReplayBytesPerDatabase - local.Reserved) {
                    reason = EScanRejection::DatabaseReplay;
                } else if (MaxResponseBytes > Limits.ReplayBytes - global.Reserved) {
                    reason = EScanRejection::GlobalReplay;
                } else {
                    auto memory = session->Resources->Reserve(ResourceClass::Data, ReplayMemoryBytes);
                    if (memory) {
                        session->ReplayMemory = std::move(*memory);
                        // All potentially allocating registry mutations precede
                        // the nonthrowing logical charge; failure is rolled back.
                        Sessions.push_back(session);
                        try {
                            Tokens.emplace(session->NextToken, session);
                        } catch (...) {
                            Sessions.pop_back();
                            throw;
                        }
                        session->ChargedBytes = MaxResponseBytes;
                        return;
                    }
                    reason = EScanRejection::SharedData;
                }
            }
            if (!EvictCompleted(*reason == EScanRejection::DatabaseReplay ? &session->Database : nullptr)) {
                Refuse(*reason);
            }
        }
    }
    void RetainCompleted(const TPtr& session)
    {
        for (;;) {
            bool localFull, globalFull;
            {
                std::lock_guard lock(Mutex);
                globalFull = CountLocked().Completed > Limits.Completed;
                localFull = CountLocked(&session->Database).Completed > Limits.CompletedPerDatabase;
            }
            if (!localFull && !globalFull) {
                return;
            }
            if (!EvictCompleted(localFull ? &session->Database : nullptr, session)) {
                {
                    std::lock_guard lock(Mutex);
                    ++CompletedEvictions;
                }
                Release(session); // A zero retention limit explicitly disables replay.
                return;
            }
        }
    }
    bool Peek(TSession& session, bool allowPartial = false)
    {
        CheckActive(session);
        while (session.Pending.empty() && !session.Ended) {
            auto result = std::atomic_load(&session.Cursor)->Next();
            CheckActive(session);
            if (!result && result.error().code == "RESOURCE_EXHAUSTED" &&
                result.error().retry_hint == RetryHint::SameIdAfterReconcile)
            {
                // Commit a nonempty page before retaining another decoder
                // output. Otherwise rollback can preserve its own admission
                // pressure indefinitely when core returns many tiny batches.
                if (allowPartial) {
                    return false;
                }
                Refuse(EScanRejection::SharedDecode);
            }
            auto batch = Unwrap(std::move(result));
            if (!batch) {
                session.Ended = true;
            } else if (batch->num_rows() != 0) {
                if (session.Rollback) {
                    session.Rollback->push_back(batch);
                }
                session.Pending.push_back(std::move(batch));
            }
        }
        return !session.Pending.empty();
    }
    void Consume(TSession& session, int64_t count)
    {
        auto& batch = session.Pending.front();
        if (count == batch->num_rows()) {
            session.Pending.pop_front();
        } else {
            batch = batch->Slice(count);
        }
    }
    TJson Envelope(const TSession& session, const std::string& next, size_t count)
    {
        return {{"schema", session.Schema},
            {"snapshot_id", Snapshot(session.Info)},
            {"identity", EncodeIdentity(session.Info.identity)},
            {"next_cursor", next.empty() ? TJson(nullptr) : TJson(next)},
            {"profile", {{"rows_returned", std::to_string(count)}}}};
    }
    std::string JsonRows(TSession& session, size_t& count)
    {
        std::string rows = "[";
        const auto overhead = Envelope(session, std::string(32, '0'), PageRows).dump().size() + 9;
        Require(overhead < MaxResponseBytes, "scan schema exceeds response byte limit");
        while (count < PageRows && rows.size() < PageBytes && Peek(session, count != 0)) {
            const auto take = AppendRowsJson(*session.Pending.front(),
                rows,
                {.MaxRows = PageRows - count, .TargetBytes = PageBytes, .HardBytes = MaxResponseBytes - overhead});
            if (take == 0) {
                break;
            }
            Consume(session, static_cast<int64_t>(take));
            count += take;
        }
        rows += ']';
        return rows;
    }
    TResponse ArrowPage(TSession& session, size_t& count)
    {
        std::vector<std::shared_ptr<arrow::RecordBatch>> pieces, groups;
        size_t bytes = 0;
        while (count < PageRows && bytes < PageBytes && Peek(session, count != 0)) {
            const auto& pending = session.Pending.front();
            int64_t take = std::min(pending->num_rows(), static_cast<int64_t>(PageRows - count));
            auto slice = pending->Slice(0, take);
            auto size = ArrowValue(arrow::util::ReferencedBufferSize(*slice));
            if (static_cast<uint64_t>(size) > PageBytes - bytes) {
                int64_t low = 0, high = take;
                while (low < high) {
                    const auto middle = low + (high - low + 1) / 2;
                    const auto candidate = ArrowValue(arrow::util::ReferencedBufferSize(*pending->Slice(0, middle)));
                    if (static_cast<uint64_t>(candidate) <= PageBytes - bytes) {
                        low = middle;
                    } else {
                        high = middle - 1;
                    }
                }
                take = count == 0 ? std::max<int64_t>(1, low) : low;
                if (take == 0) {
                    break;
                }
                slice = pending->Slice(0, take);
                size = ArrowValue(arrow::util::ReferencedBufferSize(*slice));
            }
            pieces.push_back(std::move(slice));
            count += static_cast<size_t>(take);
            bytes += static_cast<size_t>(size);
            Consume(session, take);
            // Bound object/control memory when core returns one filtered row
            // per batch, without repeatedly copying the whole growing page.
            if (pieces.size() == 128) {
                groups.push_back(ArrowValue(arrow::ConcatenateRecordBatches(pieces)));
                pieces.clear();
            }
        }
        if (!pieces.empty()) {
            groups.push_back(pieces.size() == 1 ? pieces.front() : ArrowValue(arrow::ConcatenateRecordBatches(pieces)));
        }
        std::shared_ptr<arrow::RecordBatch> batch;
        if (!groups.empty()) {
            batch = groups.size() == 1 ? groups.front() : ArrowValue(arrow::ConcatenateRecordBatches(groups));
            const auto prefix =
                static_cast<size_t>(ArrowValue(arrow::ipc::SerializeSchema(*session.Info.output_schema))->size()) +
                8; // schema message plus explicit EOS
            Require(prefix < MaxResponseBytes, "Arrow schema exceeds response byte limit");
            const auto target = PageBytes > prefix ? PageBytes - prefix : 0;
            int64_t take = batch->num_rows();
            if (static_cast<uint64_t>(IpcSize(*batch)) > target) {
                int64_t low = 0, high = take;
                while (low < high) {
                    const auto middle = low + (high - low + 1) / 2;
                    if (static_cast<uint64_t>(IpcSize(*batch->Slice(0, middle))) <= target) {
                        low = middle;
                    } else {
                        high = middle - 1;
                    }
                }
                take = std::max<int64_t>(1, low);
            }
            if (take < batch->num_rows()) {
                session.Pending.push_front(batch->Slice(take));
            }
            batch = batch->Slice(0, take);
            Require(static_cast<uint64_t>(IpcSize(*batch)) <= MaxResponseBytes - prefix,
                "individual Arrow row exceeds response byte limit");
            count = static_cast<size_t>(take);
        }
        return EncodeArrowPage(std::const_pointer_cast<arrow::Schema>(session.Info.output_schema),
            batch,
            session.Info.selector_resolved);
    }
    TResponse Page(const TPtr& session, const std::string& token, const std::shared_ptr<ResourceReservation>& outgoing)
    {
        CheckActive(*session);
        Require(token == session->ReplayToken || token == session->NextToken,
            "scan cursor not found or expired",
            "NOT_FOUND");
        if (token == session->ReplayToken) {
            auto response = session->Replay;
            response.ResourceLease = outgoing;
            CheckActive(*session);
            return response; // Replay does not extend idle or absolute lifetime.
        }
        TPageTransaction transaction(*session);
        size_t count = 0;
        auto response = session->Arrow ? ArrowPage(*session, count) : TResponse{200, JsonRows(*session, count)};
        const auto next = (Peek(*session, count != 0) || !session->Ended) ? Token() : std::string{};
        if (session->Arrow) {
            if (!next.empty()) {
                response.Headers["X-GlamDB-Next-Cursor"] = next;
            }
        } else {
            auto body = Envelope(*session, next, count).dump();
            body.pop_back();
            body += ",\"rows\":";
            body += response.Body;
            body += '}';
            response = {200, std::move(body)};
        }
        Require(response.Body.size() <= MaxResponseBytes, "scan response exceeds response byte limit");
        response.ResourceLease = outgoing;
        session->Replay = response;
        session->Replay.ResourceLease = session->ReplayMemory;
        session->BodyBytes = response.Body.size();
        CheckActive(*session);
        session->ReplayToken = token;
        session->NextToken = next;
        session->Touched = Now();
        if (next.empty()) {
            if (const auto cursor = std::atomic_exchange(&session->Cursor, std::shared_ptr<ScanCursor>{})) {
                cursor->Cancel();
            }
            session->Complete = true;
            // Replays use the encoded schema, not the cursor's charged schema.
            session->Info.output_schema.reset();
        }
        {
            std::lock_guard lock(Mutex);
            CheckActive(*session);
            std::erase_if(Tokens, [&](const auto& item) {
                return item.second == session;
            });
            Tokens.emplace(token, session);
            if (!next.empty()) {
                Tokens.emplace(next, session);
            }
        }
        transaction.Committed = true;
        if (next.empty()) {
            RetainCompleted(session);
        }
        return response;
    }
};
TScans::TScans(TScanLimits limits, std::shared_ptr<ResourceOwner> resources)
{
    ValidateScanLimits(limits, resources ? &resources->GetLimits() : nullptr);
    Impl_ = std::make_unique<TImpl>(limits, std::move(resources));
}
TScans::TScans(std::chrono::milliseconds idle, std::chrono::milliseconds lifetime)
    : TScans(TScanLimits{.Idle = idle, .Lifetime = lifetime, .CompletedTtl = idle})
{ }
TScans::~TScans() = default;
const TScanLimits& TScans::GetLimits() const
{
    return Impl_->Limits;
}
TJson TScans::GetStatistics(const std::string& database) const
{
    TImpl::TOccupancy global, local;
    decltype(Impl_->Rejections) rejections;
    uint64_t evictions, expirations, cancellations;
    {
        std::lock_guard lock(Impl_->Mutex);
        global = Impl_->CountLocked();
        local = Impl_->CountLocked(&database);
        rejections = Impl_->Rejections;
        evictions = Impl_->CompletedEvictions;
        expirations = Impl_->Expirations;
        cancellations = Impl_->Cancellations;
    }
    TJson value{{"global", global.Encode()},
        {"completed_evictions", evictions},
        {"expirations", expirations},
        {"cancellations", cancellations},
        {"completed_eviction_policy", "oldest_completed_first"}};
    if (!database.empty()) {
        value["database"] = local.Encode();
    }
    for (size_t index = 0; index < rejections.size(); ++index) {
        value["rejections"][RejectionNames[index]] = rejections[index];
    }
    return value;
}
TResponse TScans::Open(const std::shared_ptr<Engine>& engine,
    const std::string& database,
    const std::string& collection,
    const TJson& body,
    bool arrow,
    bool openOnly)
{
    TImpl::TPtr session;
    std::string token;
    if (body.contains("cursor")) {
        Require(!openOnly, "open does not accept a cursor");
        CheckKeys(body, {"cursor"});
        token = body.at("cursor").get<std::string>();
        session = Impl_->Find({.Database = database, .Collection = collection, .Token = token});
    } else {
        session = std::make_shared<TImpl::TSession>();
        session->Resources = engine->GetResourceOwner();
        Require(!Impl_->Resources || session->Resources == Impl_->Resources,
            "scan engine must share the service resource owner");
        session->DescriptorMemory = Impl_->Reserve(session->Resources,
            ResourceClass::Control,
            16ULL << 10,
            EScanRejection::SharedControl,
            true);
        session->Database = database;
        session->Collection = collection;
        session->Arrow = arrow;
        session->NextToken = Token();
        token = session->NextToken;
    }
    std::unique_lock lock(session->Mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        Impl_->Refuse(EScanRejection::SessionBusy);
    }
    // A representation mismatch does not invalidate the owner's valid cursor.
    Require(session->Arrow == arrow, "cursor representation cannot change");
    Require(token == session->NextToken || token == session->ReplayToken,
        "scan cursor not found or expired",
        "NOT_FOUND");
    {
        // Serialize admission with the reaper's expiry decision. Busy pages
        // remain active until completion; replay still does not refresh idle.
        std::lock_guard activityLock(Impl_->Mutex);
        Impl_->CheckActive(*session);
        session->Busy = true;
    }
    struct TActivity
    {
        TImpl::TSession& Session;
        ~TActivity()
        {
            Session.Busy = false;
        }
    } activity{*session};
    // Reserve all new copies before consuming the cursor. A parent refusal
    // preserves an existing cursor and its previous replay.
    auto work = token == session->ReplayToken ? nullptr
                                              : Impl_->Reserve(session->Resources,
                                                    ResourceClass::Data,
                                                    128ULL << 20,
                                                    EScanRejection::SharedData,
                                                    !body.contains("cursor"));
    auto outgoing = Impl_->Reserve(session->Resources,
        ResourceClass::Data,
        ReplayMemoryBytes,
        EScanRejection::SharedData,
        !body.contains("cursor"));
    try {
        if (!body.contains("cursor")) {
            Impl_->Register(session);
            const auto info = Unwrap(engine->GetCollection(collection));
            auto result = engine->Scan(ParseScan(collection, info.schema, body));
            if (!result && result.error().code == "RESOURCE_EXHAUSTED") {
                Impl_->Refuse(EScanRejection::SharedPlanning);
            }
            const auto cursor = Unwrap(std::move(result));
            std::atomic_store(&session->Cursor, cursor);
            // Stop may have run while planning, before the cursor was published.
            if (session->Cancelled.load()) {
                cursor->Cancel();
            }
            session->Info = cursor->Info();
            session->Schema = EncodeSchema(*session->Info.output_schema);
            for (auto& field : session->Schema["fields"]) {
                for (size_t i = 0; i < info.schema.fields.size(); ++i) {
                    if (field["name"] == info.schema.fields[i].name) {
                        field["field_id"] = i + 1;
                    }
                }
            }
        }
        Impl_->CheckActive(*session);
        if (openOnly) {
            session->Touched = Now();
            return {200,
                TJson{{"cursor", token},
                    {"snapshot_id", Snapshot(session->Info)},
                    {"schema", session->Schema},
                    {"identity", EncodeIdentity(session->Info.identity)}}
                    .dump(),
                "application/json",
                {},
                outgoing};
        }
        return Impl_->Page(session, token, outgoing);
    } catch (const TScanAdmissionFailure&) {
        if (!body.contains("cursor") || session->Cancelled.load() || Impl_->Expired(*session)) {
            Impl_->Release(session);
        }
        throw;
    } catch (...) {
        Impl_->Release(session);
        throw;
    }
}
TResponse TScans::Cancel(const std::string& database, const std::string& collection, const TJson& body)
{
    CheckKeys(body, {"cursor"});
    const auto token = body.at("cursor").get<std::string>();
    const auto session = Impl_->Find({.Database = database, .Collection = collection, .Token = token});
    {
        std::lock_guard lock(Impl_->Mutex);
        Impl_->CheckActive(*session);
        ++Impl_->Cancellations;
        Impl_->MarkLocked(session);
    }
    if (const auto cursor = std::atomic_load(&session->Cursor)) {
        cursor->Cancel();
    }
    std::unique_lock lock(session->Mutex, std::try_to_lock);
    if (lock.owns_lock()) {
        Impl_->Release(session);
    }
    return {200, R"({"cancelled":true})"};
}
void TScans::Stop()
{
    std::vector<TImpl::TPtr> sessions;
    {
        std::lock_guard lock(Impl_->Mutex);
        Impl_->Accepting = false;
        sessions.swap(Impl_->Sessions);
        Impl_->Tokens.clear();
        for (const auto& session : sessions) {
            session->Cancelled = true;
            session->ChargedBytes = 0;
        }
    }
    // Cursor cancellation is thread-safe; destruction stays outside registry locks.
    for (const auto& session : sessions) {
        if (const auto cursor = std::atomic_load(&session->Cursor)) {
            cursor->Cancel();
        }
    }
}
void TScans::Clear(const std::string& database)
{
    Impl_->Clear(&database);
}
void TScans::Clear()
{
    Impl_->Clear(nullptr);
}
} // namespace glamdb::NServer
