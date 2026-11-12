#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <system_error>

namespace NGlamDB::NTelemetry {

enum class ELogEvent : std::uint8_t {
    RuntimeStarted,
    RuntimeStopped,
    RequestCompleted,
    StorageFailed,
    RecoveryRequired,
    LoggerHealth
};
enum class ELogOperation : std::uint8_t {
    Other,
    CreateStore,
    InspectStore,
    QuiesceStore,
    Database,
    Health,
    Collections,
    Snapshots,
    Append,
    Scan,
    Operation,
    Auth
};
enum class ELogOutcome : std::uint8_t { Ok, Error, Unknown, Denied, Cancelled };

// No free text, URL, path, identity or payload may cross this boundary.
struct TLogEvent
{
    ELogEvent Event = ELogEvent::RequestCompleted;
    ELogOperation Operation = ELogOperation::Other;
    ELogOutcome Outcome = ELogOutcome::Ok;
    std::uint64_t RequestID = 0;
    std::uint64_t DurationUs = 0;
    std::uint64_t RowCount = 0;
    std::uint32_t StatusCode = 0;
};

struct TLogOptions
{
    std::uint64_t SuccessSampleEvery = 100;
    std::uint32_t MaxSuccessPerSecond = 20;
};

struct TLogStats
{
    std::uint64_t Submitted = 0;
    std::uint64_t Accepted = 0;
    std::uint64_t Sampled = 0;
    std::uint64_t RateLimited = 0;
    std::uint64_t DroppedFull = 0;
    std::uint64_t DroppedUnregistered = 0;
    std::uint64_t DroppedStopped = 0;
    std::uint64_t AdapterErrors = 0;
    std::uint64_t BackendNotifications = 0;
    // Delayed callback observation of DroppedFull, never an additional loss count.
    std::uint64_t QuillReportedDropped = 0;
    std::uint64_t SinkErrors = 0;
    std::size_t ProducerThreads = 0;
    std::size_t QueueCapacityBytes = 0;
    std::size_t MaxProducerThreads = 0;
};

// Cumulative process counters outlive the logger and its metrics consumers.
TLogStats GetProcessLogStats() noexcept;

class TStructuredLogger
{
public:
    static constexpr std::size_t QueueCapacityBytes = 64ULL * 1024;
    static constexpr std::size_t MaxProducerThreads = 16;
    static constexpr std::size_t MaxRecordBytes = 1024;

    // One active logger per process. Open preallocates the calling producer.
    static std::expected<std::unique_ptr<TStructuredLogger>, std::error_code> Open(const TLogOptions& options = {});
    ~TStructuredLogger();
    TStructuredLogger(const TStructuredLogger&) = delete;
    TStructuredLogger& operator=(const TStructuredLogger&) = delete;

    // Call during worker initialization, before handling operations. The process
    // admits at most sixteen producer threads over its lifetime, including Open.
    // Slots are not reused: a blocked backend can retain exited threads' queues.
    bool RegisterProducerThread() noexcept;
    // No file/network IO, flush, queue allocation or waiting for the sink here.
    bool TryLog(const TLogEvent& event) noexcept;
    std::uint64_t NewRequestID() noexcept;
    TLogStats GetStats() const noexcept;
    // Close admission and drain in-flight callers before removing Quill objects.
    // OS stderr IO can delay shutdown. Join producers before destroying this object.
    void Shutdown() noexcept;

private:
    struct TImpl;
    explicit TStructuredLogger(std::unique_ptr<TImpl> impl);
    std::unique_ptr<TImpl> Impl_;
};

// Explicit synchronous call context. No process-global logger pointer and no
// dependency on HTTP types. The owner must outlive this scope and its workers.
class TLogScope
{
public:
    TLogScope(TStructuredLogger* logger, ELogOperation operation, std::uint64_t requestID) noexcept;
    ~TLogScope();
    TLogScope(const TLogScope&) = delete;
    TLogScope& operator=(const TLogScope&) = delete;
    static void Emit(ELogEvent event, ELogOutcome outcome = ELogOutcome::Error) noexcept;

private:
    TStructuredLogger* Logger_;
    ELogOperation Operation_;
    std::uint64_t RequestID_;
    TLogScope* Previous_;
    static thread_local TLogScope* Current_;
};

// Best-effort process lifecycle; never makes startup fail if the sink cannot open.
class TLogRuntime
{
public:
    explicit TLogRuntime(const TLogOptions& options = {}) noexcept;
    ~TLogRuntime();
    TStructuredLogger* Get() const noexcept;
    void SetResult(bool success) noexcept;

private:
    std::unique_ptr<TStructuredLogger> Logger_;
    ELogOutcome Outcome_ = ELogOutcome::Error;
};

} // namespace NGlamDB::NTelemetry
