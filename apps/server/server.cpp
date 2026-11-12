#include "apps/server/server.h"
#include "apps/server/admission.h"
#include "apps/server/upload_job.h"
#include "apps/transport/bulk_upload.h"

#include "storage/resources.h"

#include "apps/server/metrics.h"

#include "core/telemetry/structured_log.h"

#include <asio.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <deque>
#include <iostream>
#include <set>
#include <sys/socket.h>
#include <thread>

namespace glamdb::NServer {
using namespace NTransport;
using namespace NGlamDB::NTelemetry;
namespace {
using TTcp = asio::ip::tcp;
bool IsControlBody(const TRequest& request)
{
    const auto [path, query] = ParseTarget(request.Target);
    (void)query;
    const auto split = path.starts_with("/databases/") ? path.find('/', 11) : std::string::npos;
    const auto suffix = split == std::string::npos ? path : path.substr(split);
    return path == "/databases" ||
        (split != std::string::npos &&
            (suffix == "/open" || suffix == "/close" || suffix == "/reconcile" || suffix == "/discard" ||
                suffix == "/drop")) ||
        (suffix.starts_with("/collections/") && suffix.ends_with("/scan/cancel"));
}
ELogOperation ClassifyOperation(const TRequest& request) noexcept
{
    try {
        const auto [path, query] = ParseTarget(request.Target);
        (void)query;
        const auto split = path.starts_with("/databases/") ? path.find('/', 11) : std::string::npos;
        const auto suffix = split == std::string::npos ? path : path.substr(split);
        if (path == "/healthz" || suffix == "/readyz" || suffix == "/capabilities") {
            return ELogOperation::Health;
        }
        if (path == "/databases" && request.Method == "POST") {
            return ELogOperation::CreateStore;
        }
        if (suffix.starts_with("/collections/")) {
            if (suffix.find("/operations/") != std::string::npos) {
                return ELogOperation::Operation;
            }
            if (suffix.ends_with("/append") || suffix.ends_with("/bulk-append")) {
                return ELogOperation::Append;
            }
            if (suffix.ends_with("/scan/open")) {
                return ELogOperation::ScanOpen;
            }
            if (suffix.ends_with("/scan/cancel")) {
                return ELogOperation::ScanCancel;
            }
            if (suffix.find("/scan") != std::string::npos || suffix.ends_with("/explain")) {
                return ELogOperation::Scan;
            }
            if (suffix.find("/snapshots") != std::string::npos) {
                return ELogOperation::Snapshots;
            }
            return ELogOperation::Collections;
        }
        if (suffix == "/collections") {
            return request.Method == "POST" ? ELogOperation::CreateCollection : ELogOperation::Collections;
        }
        if (path == "/databases" || path.starts_with("/databases/")) {
            return ELogOperation::Database;
        }
    } catch (...) {
        return ELogOperation::Other;
    }
    return ELogOperation::Other;
}
class TSession;
struct TCallQueue
{
    TCallQueue(size_t workers, size_t perDatabase)
        : Pool(workers)
        , Calls({.Active = workers, .PerDatabase = perDatabase})
    { }
    void Pump();
    asio::thread_pool Pool;
    TFairQueue<std::shared_ptr<TSession>> Calls;
    bool Pumping = false, Stopping = false;
};
struct TState
{
    explicit TState(TRoutes& routes,
        std::chrono::milliseconds timeout,
        TStructuredLogger* logger,
        TReaderLimits readers)
        : Readers(readers.Active, readers.PerDatabase)
        , Routes(routes)
        , Timeout(timeout)
        , Logger(logger)
    { }
    asio::io_context IO;
    void PumpBodies();
    void ReapWorkers();
    void ObserveActivity();
    TCallQueue Readers;
    TCallQueue Writers{4, 1};
    TCallQueue Control{2, 1};
    TCallQueue Lifecycle{4, 1};
    asio::thread_pool Uploads{2};
    std::set<std::string> UploadDatabases;
    TRoutes& Routes;
    std::set<std::shared_ptr<TSession>> Sessions;
    std::map<std::string, size_t> HeaderPeers;
    std::deque<std::shared_ptr<TSession>> BodyWaiters;
    std::chrono::milliseconds Timeout;
    TStructuredLogger* Logger;
    size_t Headers = 0, Bodies = 0, OrdinaryBodies = 0;
    bool BodyPumping = false;
};

class TSession : public std::enable_shared_from_this<TSession>
{
    friend struct TState;

public:
    TSession(TTcp::socket socket, TState& state)
        : DescriptorMemory_(Unwrap(state.Routes.GetResourceOwner()->Reserve(ResourceClass::Completion, 256ULL << 10)))
        , Socket_(std::move(socket))
        , Timer_(state.IO)
        , SpillTimer_(state.IO)
        , State_(state)
        , Metrics_(nullptr)
        , RequestID_(state.Logger ? state.Logger->NewRequestID() : 0)
    { }
    template <class F>
    void Guard(F&& action) noexcept
    {
        try {
            action();
        } catch (const TFailure& error) {
            try {
                Write(ErrorResponse(ErrorStatus(error.Detail),
                    error.Detail.code,
                    error.Detail.message,
                    error.Detail.retry_hint));
            } catch (...) {
                Finish();
            }
        } catch (...) {
            Finish();
        }
    }
    void Start()
    {
        Peer_ = Socket_.remote_endpoint().address().to_string();
        // Keep response slots usable by complete authenticated requests even
        // when unauthenticated peers exhaust their ordinary header allowance.
        HeaderProbe_ = State_.Headers >= 64 || State_.HeaderPeers[Peer_] >= 32;
        ReadingHeaders_ = true;
        ++State_.Headers;
        ++State_.HeaderPeers[Peer_];
        ReadExpires_ = std::chrono::steady_clock::now() + State_.Timeout;
        if (HeaderProbe_) {
            Deadline(std::min(State_.Timeout, std::chrono::milliseconds(100)), true);
        } else {
            ReadDeadline();
        }
        ReadHeaders();
    }
    void StopIdle()
    {
        if (!Active_) {
            Finish();
        }
    }
    void StartCall()
    {
        Queued_ = false;
        Active_ = true;
        if (std::chrono::steady_clock::now() >= QueueExpires_) {
            Write(Refusal(408, "REQUEST_TIMEOUT", "request queue deadline exceeded"));
            return;
        }
        // The cancellation callback may still be queued when a worker frees.
        // Peek without Asio's synchronous retry loop before entering core.
        if (PeerAborted()) {
            Finish();
            return;
        }
        std::error_code ignored;
        Socket_.cancel(ignored);
        WatchWorker();
        asio::post(Queue_->Pool, [self = shared_from_this(), guard = asio::make_work_guard(State_.IO)]() mutable {
            try {
                // First use initializes this stable worker before entering the engine.
                if (self->State_.Logger) {
                    self->State_.Logger->RegisterProducerThread();
                }
                TLogScope scope(self->State_.Logger, self->Operation_, self->RequestID_);
                auto response = self->State_.Routes.Handle(self->Request_);
                self->WorkerDone_.store(true);
                asio::post(self->State_.IO, [self, response = std::move(response), guard = std::move(guard)]() mutable {
                    self->Guard([&] {
                        self->CompleteCall();
                        self->Write(std::move(response));
                    });
                });
            } catch (...) {
                self->WorkerDone_.store(true);
                self->WorkerFailed_.store(true);
            }
        });
        WorkerStarted_ = true;
    }

private:
    void ReleaseHeaders()
    {
        if (ReadingHeaders_) {
            ReadingHeaders_ = false;
            --State_.Headers;
            const auto found = State_.HeaderPeers.find(Peer_);
            if (found != State_.HeaderPeers.end() && --found->second == 0) {
                State_.HeaderPeers.erase(found);
            }
        }
    }
    void ReadHeaders()
    {
        Socket_.async_read_some(asio::buffer(ReadBuffer_),
            [self = shared_from_this()](const std::error_code& error, size_t size) {
                self->Guard([&] {
                    if (self->Processing_) {
                        return;
                    }
                    if (error || self->Closed_) {
                        self->Finish();
                        return;
                    }
                    self->Input_.append(self->ReadBuffer_.data(), size);
                    const auto end = self->Input_.find("\r\n\r\n");
                    const auto headerSize = end == std::string::npos ? self->Input_.size() : end + 4;
                    Require(headerSize <= 16384, "HTTP headers exceed 16 KiB", "HEADERS_TOO_LARGE");
                    for (size_t i = self->Checked_; i < headerSize; ++i) {
                        Require(self->Input_[i] != '\n' || (i > 0 && self->Input_[i - 1] == '\r'),
                            "bare LF in HTTP headers");
                        Require(i == 0 || self->Input_[i - 1] != '\r' || self->Input_[i] == '\n',
                            "bare CR in HTTP headers");
                    }
                    self->Checked_ = headerSize;
                    if (end != std::string::npos) {
                        self->Headers(headerSize);
                    } else {
                        self->ReadHeaders();
                    }
                });
            });
    }
    void CancelDeadline()
    {
        ++Generation_;
        Timer_.cancel();
    }
    TResponse Refusal(int status, std::string_view code, std::string_view message)
    {
        auto response = ErrorResponse(status, code, message);
        auto body = TJson::parse(response.Body);
        body["execution_started"] = false;
        response.Body = body.dump();
        response.Headers["Retry-After"] = "1";
        return response;
    }
    void ReadDeadline()
    {
        Deadline(std::chrono::duration_cast<std::chrono::milliseconds>(ReadExpires_ - std::chrono::steady_clock::now()),
            true);
    }
    void Deadline(std::chrono::milliseconds timeout, bool reading = false)
    {
        const auto generation = ++Generation_;
        Timer_.expires_after(timeout);
        Timer_.async_wait([self = shared_from_this(), generation, reading](const std::error_code& error) {
            self->Guard([&] {
                if (error || self->Closed_ || generation != self->Generation_) {
                    return;
                }
                if (reading) {
                    if (self->HeaderProbe_) {
                        self->Busy();
                    } else {
                        self->Write(self->Refusal(408, "REQUEST_TIMEOUT", "request read or queue deadline exceeded"));
                    }
                } else {
                    self->Finish();
                }
            });
        });
    }
    void Finish() noexcept
    {
        if (Closed_) {
            if (Active_ && WorkerDone_.load()) {
                CompleteCall();
            }
            if (!Active_ && CleanupUpload()) {
                State_.Sessions.erase(shared_from_this());
            }
            return;
        }
        Closed_ = true;
        LogCompletion();
        ++Generation_;
        std::error_code ignored;
        try {
            Timer_.cancel();
        } catch (...) {
            Socket_.cancel(ignored);
        }
        Socket_.close(ignored);
        ReleaseHeaders();
        HeaderProbe_ = false;
        ReleaseBody();
        if (Active_ && WorkerStarted_ && !WorkerDone_.load()) {
            // Socket/watchdog failure cannot release an executing core call's
            // permit. The worker acknowledgement owns its remaining lifetime.
            try {
                WatchWorker();
            } catch (...) {
                // A later accept or shutdown also reaps finished workers.
                std::fputs("worker watchdog rearm failed\n", stderr);
            }
            return;
        }
        ReleaseCall();
        if (CleanupUpload()) {
            State_.Sessions.erase(shared_from_this());
        }
    }
    void Busy()
    {
        Write(Refusal(503, "SERVER_BUSY", "server admission limit reached; retry with original operation ID"));
    }
    void Headers(size_t size)
    {
        if (Closed_ || Processing_) {
            return;
        }
        Request_ = ParseHeaders(std::string_view(Input_).substr(0, size));
        Operation_ = ClassifyOperation(Request_);
        Metrics_.SetRequest(Request_.Method, Request_.Target);
        if (!State_.Routes.IsAuthorized(Request_)) {
            Write(ErrorResponse(401, "UNAUTHORIZED", "bearer token required"));
            return;
        }
        Bulk_ = IsBulkUpload(Request_);
        if (Bulk_) {
            Require(HasBulkContentType(Request_),
                "Content-Type must be application/x-ndjson",
                "UNSUPPORTED_MEDIA_TYPE");
            Require(Request_.ContentLength > 0, "bulk upload requires a nonempty body");
            const auto [path, query] = ParseTarget(Request_.Target);
            (void)query;
            const auto split = path.starts_with("/databases/") ? path.find('/', 11) : std::string::npos;
            Database_ = split == std::string::npos ? std::string("legacy") : path.substr(11, split - 11);
            if (State_.UploadDatabases.size() >= 2 || State_.UploadDatabases.contains(Database_)) {
                Busy();
                return;
            }
            State_.UploadDatabases.insert(Database_);
            BulkAdmitted_ = true;
            ReadExpires_ = std::chrono::steady_clock::now() + std::chrono::minutes(10);
        }
        const bool protectedCall = UsesProtectedAdmission(Request_) && Request_.ContentLength <= (64ULL << 10);
        BodyMemory_ = Unwrap(
            State_.Routes.GetResourceOwner()->Reserve(protectedCall ? ResourceClass::Completion : ResourceClass::Data,
                Bulk_ ? 256ULL << 10 : static_cast<uint64_t>(Request_.ContentLength) * 2 + (64ULL << 10)));
        ReleaseHeaders();
        HeaderProbe_ = false;
        Input_.erase(0, size);
        Require(Input_.size() <= Request_.ContentLength, "pipelining is unsupported");
        if (Request_.ContentLength != 0) {
            ControlBody_ = IsControlBody(Request_);
            if (State_.Bodies >= 16 || (!ControlBody_ && State_.OrdinaryBodies >= 12)) {
                State_.BodyWaiters.push_back(shared_from_this());
                WaitingBody_ = true;
                ReadDeadline();
                return;
            }
            ++State_.Bodies;
            if (!ControlBody_) {
                ++State_.OrdinaryBodies;
            }
            BodyAdmitted_ = true;
        }
        BeginBody();
    }
    void BeginBody()
    {
        ReadDeadline();
        if (Request_.Headers.contains("expect")) {
            // A stalled interim write must close, not start an overlapping 408
            // write. The body read budget begins after Continue is delivered.
            Deadline(std::chrono::seconds(5));
            static constexpr std::string_view Continue = "HTTP/1.1 100 Continue\r\n\r\n";
            asio::async_write(Socket_,
                asio::buffer(Continue),
                [self = shared_from_this()](const std::error_code& error, size_t) {
                    self->Guard([&] {
                        if (error || self->Closed_) {
                            self->Finish();
                        } else {
                            self->ReadDeadline();
                            self->ReadBody();
                        }
                    });
                });
        } else {
            ReadBody();
        }
    }
    void ReleaseBody() noexcept
    {
        if (WaitingBody_) {
            std::erase(State_.BodyWaiters, shared_from_this());
            WaitingBody_ = false;
        }
        if (BodyAdmitted_) {
            --State_.Bodies;
            if (!ControlBody_) {
                --State_.OrdinaryBodies;
            }
            BodyAdmitted_ = false;
        }
        try {
            State_.PumpBodies();
        } catch (...) {
            // Waiting headers retain bounded buffers and their read deadlines.
            std::fputs("body queue dispatch failed\n", stderr);
        }
    }
    void ReadBody()
    {
        if (Closed_ || Processing_) {
            return;
        }
        if (Bulk_) {
            ReadBulk();
            return;
        }
        const auto remaining = Request_.ContentLength - Input_.size();
        if (remaining == 0) {
            Dispatch();
            return;
        }
        asio::async_read(Socket_,
            asio::dynamic_buffer(Input_, MaxRequestBytes),
            asio::transfer_exactly(remaining),
            [self = shared_from_this()](const std::error_code& error, size_t) {
                self->Guard([&] {
                    if (self->Processing_ || self->Closed_) {
                        return;
                    }
                    if (error) {
                        self->Finish();
                    } else {
                        self->Dispatch();
                    }
                });
            });
    }
    void ReleaseBulk() noexcept
    {
        if (BulkAdmitted_) {
            State_.UploadDatabases.erase(Database_);
            BulkAdmitted_ = false;
        }
    }
    void WatchSpill(const std::shared_ptr<TUploadJob>& job)
    {
        SpillTimer_.expires_after(std::chrono::milliseconds(100));
        SpillTimer_.async_wait([self = shared_from_this(), job](const std::error_code& error) {
            self->Guard([&] {
                if (error || self->SpillJob_ != job) {
                    return;
                }
                if (job->Done.load()) {
                    self->CompleteSpill(job);
                } else {
                    self->WatchSpill(job);
                }
            });
        });
    }
    void StartSpill(bool cleanup = false)
    {
        auto job = std::make_shared<TUploadJob>();
        job->Resources = State_.Routes.GetResourceOwner();
        job->Length = Request_.ContentLength;
        job->Cleanup = cleanup;
        if (!cleanup) {
            job->Bytes = std::move(Input_);
            job->Complete = BulkReceived_ + job->Bytes.size() == Request_.ContentLength;
        }
        QueueUpload(SpillJob_, Request_.BulkUpload, job, [&] {
            WatchSpill(job);
            asio::post(State_.Uploads, [self = shared_from_this(), guard = asio::make_work_guard(State_.IO), job] {
                job->Run([&] {
                    asio::post(self->State_.IO, [self, job] {
                        self->Guard([&] {
                            self->CompleteSpill(job);
                        });
                    });
                });
            });
        });
    }
    bool CleanupUpload() noexcept
    {
        if (SpillJob_) {
            return false;
        }
        if (!Request_.BulkUpload) {
            ReleaseBulk();
            return true;
        }
        try {
            StartSpill(true);
        } catch (...) {
            // Retain scratch and its permit if scheduling fails. New accepts,
            // shutdown reaping and this independent timer retry without closing
            // a file on the listener or allowing the exception to escape.
            try {
                SpillTimer_.expires_after(std::chrono::milliseconds(100));
                SpillTimer_.async_wait([self = shared_from_this()](const std::error_code& error) {
                    if (!error) {
                        self->Guard([&] {
                            if (self->CleanupUpload() && self->Closed_) {
                                self->Finish();
                            }
                        });
                    }
                });
            } catch (...) {
                std::fputs("upload cleanup dispatch failed\n", stderr);
            }
        }
        return false;
    }
    void CompleteSpill(const std::shared_ptr<TUploadJob>& job)
    {
        if (SpillJob_ != job || !job->Done.load()) {
            return;
        }
        Request_.BulkUpload = std::move(job->Upload);
        SpillJob_.reset();
        SpillTimer_.cancel();
        if (job->Cleanup) {
            ReleaseBulk();
            if (Closed_) {
                Finish();
            }
            return;
        }
        if (Closed_) {
            Finish();
            return;
        }
        if (Writing_) {
            CleanupUpload();
            return;
        }
        if (job->Failure) {
            std::rethrow_exception(job->Failure);
        }
        BulkReceived_ += job->Bytes.size();
        if (job->Complete) {
            Dispatch();
        } else {
            ReadBulk();
        }
    }
    void ReadBulk()
    {
        if (!Request_.BulkUpload || !Input_.empty()) {
            StartSpill();
            return;
        }
        Socket_.async_read_some(
            asio::buffer(ReadBuffer_.data(), std::min(ReadBuffer_.size(), Request_.ContentLength - BulkReceived_)),
            [self = shared_from_this()](const std::error_code& error, size_t count) {
                self->Guard([&] {
                    if (self->Closed_ || self->Writing_) {
                        return;
                    }
                    if (error) {
                        self->Finish();
                        return;
                    }
                    self->Input_.assign(self->ReadBuffer_.data(), count);
                    self->ReadBulk();
                });
            });
    }
    void ReleaseCall() noexcept
    {
        if (!Queue_) {
            return;
        }
        auto* queue = std::exchange(Queue_, nullptr);
        if (Queued_) {
            queue->Calls.Remove(Database_, shared_from_this());
            Queued_ = false;
        } else if (Active_) {
            queue->Calls.Release(Database_);
            Active_ = false;
        }
        try {
            queue->Pump();
        } catch (...) {
            // Pending calls retain their own deadline and cancellation handlers.
            std::fputs("call queue dispatch failed\n", stderr);
        }
    }
    void CompleteCall() noexcept
    {
        ReleaseCall();
        if (CleanupUpload() && Closed_) {
            State_.Sessions.erase(shared_from_this());
        }
    }
    // A worker cannot touch socket state. If allocating its completion fails,
    // the I/O thread observes this flag and closes only the affected session.
    void WatchWorker()
    {
        const auto generation = ++Generation_;
        Timer_.expires_after(std::chrono::milliseconds(100));
        Timer_.async_wait([self = shared_from_this(), generation](const std::error_code& error) {
            self->Guard([&] {
                if (error || (self->Closed_ && !self->Active_) || generation != self->Generation_) {
                    return;
                }
                if (self->WorkerFailed_.load() || (!self->Closed_ && self->PeerAborted())) {
                    self->Finish();
                } else if (self->Closed_ && self->WorkerDone_.load()) {
                    self->CompleteCall();
                } else {
                    self->WatchWorker();
                }
            });
        });
    }
    bool PeerAborted()
    {
        // Read EOF closes only the peer's send side; it can still read our reply.
        // Extra input is unsupported pipelining in this one-request session.
        char byte = 0;
        const auto received = recv(Socket_.native_handle(), &byte, 1, MSG_PEEK | MSG_DONTWAIT);
        return received > 0 || (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR);
    }
    void Dispatch()
    {
        if (Closed_ || Processing_) {
            return;
        }
        if (std::chrono::steady_clock::now() >= ReadExpires_) {
            Write(Refusal(408, "REQUEST_TIMEOUT", "request read deadline exceeded"));
            return;
        }
        CancelDeadline();
        BodyComplete_ = true;
        ReleaseBody();
        Request_.Body = std::move(Input_);
        const auto [path, query] = ParseTarget(Request_.Target);
        (void)query;
        const auto split = path.starts_with("/databases/") ? path.find('/', 11) : std::string::npos;
        const auto suffix = split == std::string::npos ? path : path.substr(split);
        // Health observation is bounded and does no I/O. Keep it available even
        // when operation lookups occupy every control worker behind a writer.
        if (Request_.Method == "GET" && (path == "/healthz" || suffix == "/capabilities" || suffix == "/readyz")) {
            State_.ObserveActivity();
            TLogScope scope(State_.Logger, Operation_, RequestID_);
            Write(State_.Routes.Handle(Request_));
            return;
        }
        const bool lifecycle = Request_.Method == "POST" &&
            (path == "/databases" || suffix == "/open" || suffix == "/close" || suffix == "/reconcile" ||
                suffix == "/discard" || suffix == "/drop");
        const bool control = !lifecycle &&
            (path == "/databases" || (path.starts_with("/databases/") && split == std::string::npos) ||
                !suffix.starts_with("/collections") || suffix.find("/operations/") != std::string::npos ||
                suffix.ends_with("/scan/cancel"));
        const bool writer =
            Request_.Method == "POST" && suffix.find("/scan") == std::string::npos && !suffix.ends_with("/explain");
        auto& queue = lifecycle ? State_.Lifecycle
            : control           ? State_.Control
            : writer            ? State_.Writers
                                : State_.Readers;
        Database_ = split == std::string::npos ? std::string("legacy") : path.substr(11, split - 11);
        if (!queue.Calls.Enqueue(Database_, Request_.Body.size(), shared_from_this())) {
            Busy();
            return;
        }
        Queue_ = &queue;
        Queued_ = true;
        Processing_ = true;
        QueueExpires_ = std::chrono::steady_clock::now() + State_.Timeout;
        Deadline(State_.Timeout, true);
        Socket_.async_wait(TTcp::socket::wait_read, [self = shared_from_this()](const std::error_code& error) {
            self->Guard([&] {
                if (self->Queued_ && error != asio::error::operation_aborted && (error || self->PeerAborted())) {
                    self->Finish();
                }
                // Do not rearm a readable EOF: complete half-closed requests
                // retain their queue deadline and can still receive a response.
            });
        });
        queue.Pump();
    }
    void Write(TResponse response)
    {
        if (Closed_ || Writing_) {
            return;
        }
        StatusCode_ = static_cast<std::uint32_t>(response.Status);
        ReleaseCall();
        ReleaseBody();
        ReleaseHeaders();
        Processing_ = true;
        Writing_ = true;
        std::error_code ignored;
        Socket_.cancel(ignored);
        Header_ = SerializeResponseHead(response);
        OutputMemory_ = std::move(response.ResourceLease);
        Output_ = std::move(response.Body);
        Deadline(std::chrono::seconds(30));
        const std::array buffers{asio::buffer(Header_), asio::buffer(Output_)};
        asio::async_write(Socket_, buffers, [self = shared_from_this()](const std::error_code& error, size_t) {
            self->Guard([&] {
                self->ResponseSent_ = !error;
                self->LogCompletion();
                if (error || self->BodyComplete_) {
                    self->Finish();
                } else {
                    // Half-close the reply, then drain bounded eager request data
                    // so closing a rejected body does not reset the response.
                    std::error_code ignored;
                    self->Socket_.shutdown(TTcp::socket::shutdown_send, ignored);
                    self->Deadline(std::chrono::seconds(2));
                    self->Drain();
                }
            });
        });
    }
    void LogCompletion() noexcept
    {
        Metrics_.Finish(static_cast<int>(StatusCode_), ResponseSent_, ResponseSent_ ? Output_.size() : 0);
        if (Logged_ || !State_.Logger) {
            return;
        }
        Logged_ = true;
        const auto outcome = !ResponseSent_            ? ELogOutcome::Cancelled
            : StatusCode_ == 401 || StatusCode_ == 403 ? ELogOutcome::Denied
            : StatusCode_ >= 400                       ? ELogOutcome::Error
                                                       : ELogOutcome::Ok;
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - Started_).count();
        State_.Logger->TryLog({.Event = ELogEvent::RequestCompleted,
            .Operation = StatusCode_ == 401 || StatusCode_ == 403 ? ELogOperation::Auth : Operation_,
            .Outcome = outcome,
            .RequestID = RequestID_,
            .DurationUs = static_cast<std::uint64_t>(elapsed),
            .StatusCode = ResponseSent_ ? StatusCode_ : 0,
            .ByteCount = ResponseSent_ ? static_cast<std::uint64_t>(Output_.size()) : 0});
    }
    void Drain()
    {
        Socket_.async_read_some(asio::buffer(ReadBuffer_),
            [self = shared_from_this()](const std::error_code& error, size_t size) {
                self->Guard([&] {
                    self->Drained_ += size;
                    if (error || self->Closed_ || self->Drained_ >= (size_t{32} << 20)) {
                        self->Finish();
                    } else {
                        self->Drain();
                    }
                });
            });
    }
    std::shared_ptr<ResourceReservation> DescriptorMemory_, BodyMemory_;
    std::shared_ptr<const void> OutputMemory_;
    TTcp::socket Socket_;
    asio::steady_timer Timer_, SpillTimer_;
    TState& State_;
    THttpScope Metrics_;
    TRequest Request_;
    std::array<char, 65536> ReadBuffer_{};
    std::string Input_, Header_, Output_, Peer_, Database_;
    size_t Checked_ = 0, Generation_ = 0, Drained_ = 0;
    size_t BulkReceived_ = 0;
    bool Bulk_ = false, BulkAdmitted_ = false;
    std::shared_ptr<TUploadJob> SpillJob_;
    TCallQueue* Queue_ = nullptr;
    std::chrono::steady_clock::time_point ReadExpires_, QueueExpires_;
    std::atomic<bool> WorkerFailed_{false}, WorkerDone_{false};
    bool ReadingHeaders_ = false, BodyAdmitted_ = false, ControlBody_ = false;
    bool Processing_ = false, Writing_ = false, Closed_ = false;
    bool Queued_ = false, Active_ = false, BodyComplete_ = false, HeaderProbe_ = false, WaitingBody_ = false;
    bool WorkerStarted_ = false;
    const std::uint64_t RequestID_;
    const std::chrono::steady_clock::time_point Started_ = std::chrono::steady_clock::now();
    ELogOperation Operation_ = ELogOperation::Other;
    std::uint32_t StatusCode_ = 0;
    bool ResponseSent_ = false, Logged_ = false;
};

void TState::ReapWorkers()
{
    std::array<std::shared_ptr<TSession>, 96> finished;
    size_t count = 0;
    for (const auto& session : Sessions) {
        if ((session->Active_ && session->WorkerDone_.load() && (session->Closed_ || session->WorkerFailed_.load())) ||
            (session->SpillJob_ && session->SpillJob_->Done.load()) ||
            (session->Closed_ && !session->Active_ && !session->SpillJob_ && session->Request_.BulkUpload))
        {
            finished[count++] = session;
        }
    }
    for (size_t i = 0; i < count; ++i) {
        finished[i]->Guard([&] {
            if (finished[i]->SpillJob_ && finished[i]->SpillJob_->Done.load()) {
                const auto job = finished[i]->SpillJob_;
                finished[i]->CompleteSpill(job);
            } else {
                finished[i]->CompleteCall();
                finished[i]->Finish();
            }
        });
    }
}

void TState::ObserveActivity()
{
    TRoutes::THttpActivity activity;
    activity.BodyWaiters = BodyWaiters.size();
    const auto now = std::chrono::steady_clock::now();
    for (const auto& session : Sessions) {
        activity.Connected += session->Closed_ ? 0 : 1;
        activity.Executing += session->Active_ ? 1 : 0;
        if (session->Queued_) {
            ++activity.Pending;
            const auto age =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - (session->QueueExpires_ - Timeout)).count();
            activity.OldestQueueAgeMs =
                std::max(activity.OldestQueueAgeMs, static_cast<uint64_t>(std::max<int64_t>(0, age)));
        }
    }
    Routes.SetHttpActivity(activity);
}

void TState::PumpBodies()
{
    if (BodyPumping || Readers.Stopping) {
        return;
    }
    BodyPumping = true;
    struct TReset
    {
        bool& Flag;
        ~TReset()
        {
            Flag = false;
        }
    } reset{BodyPumping};
    while (Bodies < 16 && !BodyWaiters.empty()) {
        // Ordinary uploads cannot hold up controls when only reserved readers remain.
        const auto eligible = std::find_if(BodyWaiters.begin(), BodyWaiters.end(), [&](const auto& session) {
            return session->ControlBody_ || OrdinaryBodies < 12;
        });
        if (eligible == BodyWaiters.end()) {
            break;
        }
        const auto session = *eligible;
        BodyWaiters.erase(eligible);
        session->WaitingBody_ = false;
        if (!session->Closed_ && !session->Writing_) {
            ++Bodies;
            if (!session->ControlBody_) {
                ++OrdinaryBodies;
            }
            session->BodyAdmitted_ = true;
            session->Guard([&] {
                session->BeginBody();
            });
        }
    }
}

void TCallQueue::Pump()
{
    if (Pumping || Stopping) {
        return;
    }
    Pumping = true;
    struct TReset
    {
        bool& Flag;
        ~TReset()
        {
            Flag = false;
        }
    } reset{Pumping};
    while (const auto session = Calls.Take()) {
        (*session)->Guard([&] {
            (*session)->StartCall();
        });
    }
}
} // namespace

void ValidateReaderLimits(TReaderLimits limits)
{
    Require(limits.Active >= 1 && limits.Active <= 4 && limits.PerDatabase >= 1 && limits.PerDatabase <= limits.Active,
        "HTTP reader limits must be 1..4 with per-database <= global");
}

void RunServer(TRoutes& routes,
    std::string_view address,
    uint16_t port,
    std::chrono::milliseconds timeout,
    size_t maxConnections,
    TStructuredLogger* logger,
    TReaderLimits readers)
{
    ValidateReaderLimits(readers);
    Require(maxConnections > 0 && maxConnections <= 96, "connection ceiling must be 1..96");
    std::unique_ptr<TLogRuntime> ownedRuntime;
    if (!logger) {
        ownedRuntime = std::make_unique<TLogRuntime>();
        logger = ownedRuntime->Get();
    }
    if (logger) {
        logger->RegisterProducerThread();
    }
    TState state(routes, timeout, logger, readers);
    TTcp::acceptor acceptor(state.IO, TTcp::endpoint(asio::ip::make_address(address), port));
    asio::steady_timer retry(state.IO);
    asio::signal_set signals(state.IO, SIGINT, SIGTERM);
    signals.async_wait([&](const std::error_code& error, int) {
        if (error) {
            return;
        }
        std::error_code ignored;
        acceptor.close(ignored);
        retry.cancel();
        state.Readers.Stopping = state.Writers.Stopping = state.Control.Stopping = state.Lifecycle.Stopping = true;
        routes.CancelScans();
        // StopIdle can erase itself; advancing before it avoids allocating a copy.
        for (auto it = state.Sessions.begin(); it != state.Sessions.end();) {
            (*it++)->StopIdle();
        }
    });
    std::function<void()> accept;
    bool accepting = false, waiting = false;
    const auto backoff = [&] {
        retry.expires_after(std::chrono::milliseconds(100));
        retry.async_wait([&](const std::error_code& error) {
            waiting = false;
            if (!error && acceptor.is_open()) {
                accept();
            }
        });
        waiting = true;
    };
    accept = [&] {
        state.ReapWorkers();
        if (!acceptor.is_open() || accepting || waiting) {
            return;
        }
        // Leave excess connections in the kernel backlog until a response slot
        // is available. Every accepted socket can then receive an HTTP response.
        if (state.Sessions.size() >= maxConnections) {
            backoff();
            return;
        }
        try {
            accepting = true;
            acceptor.async_accept([&](const std::error_code& error, TTcp::socket socket) {
                accepting = false;
                if (!acceptor.is_open()) {
                    return;
                }
                if (error) {
                    std::cerr << "accept: " << error.value() << '\n';
                    backoff();
                    return;
                }
                try {
                    auto session = std::make_shared<TSession>(std::move(socket), state);
                    state.Sessions.insert(session);
                    session->Guard([&] {
                        session->Start();
                    });
                } catch (...) {
                    std::error_code ignored;
                    socket.close(ignored);
                    backoff();
                    return;
                }
                accept();
            });
        } catch (...) {
            accepting = false;
            backoff();
        }
    };
    std::cout << TJson({{"address", address}, {"port", acceptor.local_endpoint().port()}, {"backend", "local"}}).dump()
              << '\n'
              << std::flush;
    // Individual callbacks contain exceptions. An allocation failure while
    // scheduling accept backoff must not permanently abandon the listener.
    for (;;) {
        try {
            accept();
            state.IO.run();
            break;
        } catch (...) {
            accepting = waiting = false;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    state.Readers.Pool.join();
    state.Writers.Pool.join();
    state.Control.Pool.join();
    state.Lifecycle.Pool.join();
    state.Uploads.join();
    state.ReapWorkers();
    if (ownedRuntime) {
        ownedRuntime->SetResult(true);
    }
}
} // namespace glamdb::NServer
