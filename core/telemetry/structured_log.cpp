#include "structured_log.h"

#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/Logger.h>
#include <quill/sinks/JsonSink.h>
#include <quill/sinks/RotatingJsonFileSink.h>

#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <pthread.h>
#include <signal.h>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>

namespace NGlamDB::NTelemetry {
namespace {

struct TFrontendOptions : quill::FrontendOptions
{
    static constexpr quill::QueueType queue_type = quill::QueueType::BoundedDropping;
    static constexpr std::size_t initial_queue_capacity = TStructuredLogger::QueueCapacityBytes;
};
using TFrontend = quill::FrontendImpl<TFrontendOptions>;

// Quill 13's public JsonConsoleSink selects stdout. Only redirect its inherited
// stream; Quill still owns JSON formatting, escaping, writing and flushing.
class TStderrJsonSink : public quill::JsonConsoleSink
{
public:
    TStderrJsonSink()
    {
        const auto fd = ::fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0);
        if (fd < 0) {
            throw std::system_error(errno, std::generic_category());
        }
#if defined(__APPLE__)
        // Darwin can deliver a pipe-generated SIGPIPE to another unblocked
        // process thread. Suppress it only on our duplicate, not process-wide.
        if (::fcntl(fd, F_SETNOSIGPIPE, 1) != 0) {
            const auto error = errno;
            ::close(fd);
            throw std::system_error(error, std::generic_category());
        }
#endif
        _file = ::fdopen(fd, "w");
        if (_file == nullptr) {
            const auto error = errno;
            ::close(fd);
            throw std::system_error(error, std::generic_category());
        }
        if (std::setvbuf(_file, nullptr, _IONBF, 0) != 0) {
            std::fclose(_file);
            throw std::system_error(std::make_error_code(std::errc::io_error));
        }
        _filename = "stderr";
    }

    ~TStderrJsonSink() override
    {
        std::fclose(_file);
    }
};

struct TAtomicStats
{
    std::atomic<std::uint64_t> Submitted{0};
    std::atomic<std::uint64_t> Accepted{0};
    std::atomic<std::uint64_t> Sampled{0};
    std::atomic<std::uint64_t> RateLimited{0};
    std::atomic<std::uint64_t> DroppedFull{0};
    std::atomic<std::uint64_t> DroppedUnregistered{0};
    std::atomic<std::uint64_t> DroppedStopped{0};
    std::atomic<std::uint64_t> AdapterErrors{0};
    std::atomic<std::uint64_t> BackendNotifications{0};
    std::atomic<std::uint64_t> QuillReportedDropped{0};
    std::atomic<std::uint64_t> SinkErrors{0};
};

TAtomicStats ProcessStats;
std::atomic<std::size_t> ProducerThreads{0};
std::mutex LifecycleMutex;
bool HasLogger = false;
thread_local bool IsRegistered = false;

void Count(TAtomicStats& local, std::atomic<std::uint64_t> TAtomicStats::*field, std::uint64_t amount = 1) noexcept
{
    (local.*field).fetch_add(amount, std::memory_order_relaxed);
    (ProcessStats.*field).fetch_add(amount, std::memory_order_relaxed);
}

TLogStats ReadStats(const TAtomicStats& stats) noexcept
{
    return {.Submitted = stats.Submitted.load(std::memory_order_relaxed),
        .Accepted = stats.Accepted.load(std::memory_order_relaxed),
        .Sampled = stats.Sampled.load(std::memory_order_relaxed),
        .RateLimited = stats.RateLimited.load(std::memory_order_relaxed),
        .DroppedFull = stats.DroppedFull.load(std::memory_order_relaxed),
        .DroppedUnregistered = stats.DroppedUnregistered.load(std::memory_order_relaxed),
        .DroppedStopped = stats.DroppedStopped.load(std::memory_order_relaxed),
        .AdapterErrors = stats.AdapterErrors.load(std::memory_order_relaxed),
        .BackendNotifications = stats.BackendNotifications.load(std::memory_order_relaxed),
        .QuillReportedDropped = stats.QuillReportedDropped.load(std::memory_order_relaxed),
        .SinkErrors = stats.SinkErrors.load(std::memory_order_relaxed),
        .ProducerThreads = ProducerThreads.load(std::memory_order_relaxed),
        .QueueCapacityBytes = TStructuredLogger::QueueCapacityBytes,
        .MaxProducerThreads = TStructuredLogger::MaxProducerThreads};
}

bool RegisterThread() noexcept
{
    if (IsRegistered) {
        return true;
    }
    auto count = ProducerThreads.load(std::memory_order_relaxed);
    while (count < TStructuredLogger::MaxProducerThreads) {
        if (ProducerThreads.compare_exchange_weak(count, count + 1, std::memory_order_relaxed)) {
            try {
                TFrontend::preallocate();
                IsRegistered = true;
                return true;
            } catch (...) {
                // A failed allocation still consumes the lifetime slot.
                return false;
            }
        }
    }
    return false;
}

void OnBackendNotification(TAtomicStats& stats, const std::string& message) noexcept
{
    Count(stats, &TAtomicStats::BackendNotifications);
    // This is the pinned Quill 13 bounded-dropping notification, not a delivery
    // estimate. Other messages are counted without publishing their free text.
    constexpr std::string_view prefix = " Quill INFO: Dropped ";
    const auto position = message.find(prefix);
    if (position != std::string::npos) {
        std::uint64_t dropped = 0;
        const auto* first = message.data() + position + prefix.size();
        const auto [end, error] = std::from_chars(first, message.data() + message.size(), dropped);
        if (error == std::errc{} &&
            std::string_view(end, message.data() + message.size()).starts_with(" events from thread "))
        {
            Count(stats, &TAtomicStats::QuillReportedDropped, dropped);
        }
    } else if (message.starts_with("fwrite failed errno:") || message.starts_with("fflush failed errno:") ||
        message.starts_with("fwrite returned 0 bytes"))
    {
        Count(stats, &TAtomicStats::SinkErrors);
    }
}

const char* GetEventName(ELogEvent event) noexcept
{
    switch (event) {
        case ELogEvent::RuntimeStarted:
            return "runtime_started";
        case ELogEvent::RuntimeStopped:
            return "runtime_stopped";
        case ELogEvent::RequestCompleted:
            return "request_completed";
        case ELogEvent::StorageFailed:
            return "storage_failed";
        case ELogEvent::RecoveryRequired:
            return "recovery_required";
        case ELogEvent::LoggerHealth:
            return "logger_health";
    }
    return "request_completed";
}

const char* GetOperationName(ELogOperation operation) noexcept
{
    switch (operation) {
        case ELogOperation::CreateStore:
            return "create_store";
        case ELogOperation::InspectStore:
            return "inspect_store";
        case ELogOperation::QuiesceStore:
            return "quiesce_store";
        case ELogOperation::Database:
            return "database";
        case ELogOperation::Health:
            return "health";
        case ELogOperation::Collections:
            return "collections";
        case ELogOperation::Snapshots:
            return "snapshots";
        case ELogOperation::Append:
            return "append";
        case ELogOperation::Scan:
            return "scan";
        case ELogOperation::Operation:
            return "operation";
        case ELogOperation::Auth:
            return "auth";
        case ELogOperation::Other:
            return "other";
    }
    return "other";
}

const char* GetOutcomeName(ELogOutcome outcome) noexcept
{
    switch (outcome) {
        case ELogOutcome::Ok:
            return "ok";
        case ELogOutcome::Error:
            return "error";
        case ELogOutcome::Unknown:
            return "unknown";
        case ELogOutcome::Denied:
            return "denied";
        case ELogOutcome::Cancelled:
            return "cancelled";
    }
    return "error";
}

const char* GetLevel(const TLogEvent& event) noexcept
{
    if (event.Event == ELogEvent::StorageFailed || event.Event == ELogEvent::RecoveryRequired ||
        event.Outcome == ELogOutcome::Error || event.Outcome == ELogOutcome::Unknown)
    {
        return "error";
    }
    return event.Outcome == ELogOutcome::Ok ? "info" : "warn";
}

} // namespace

struct TStructuredLogger::TImpl
{
    const TLogOptions Options;
    const std::shared_ptr<TAtomicStats> Stats = std::make_shared<TAtomicStats>();
    quill::LoggerImpl<TFrontendOptions>* Logger = nullptr;
    std::atomic<bool> Stopped{false};
    std::atomic<std::uint64_t> InFlight{0};
    std::atomic<std::uint64_t> Sequence{0};
    std::atomic<std::uint64_t> RequestID{0};
    // One packed monotonic second/count permits a bounded CAS admission attempt.
    std::atomic<std::uint64_t> SuccessWindow{0};
    const std::uint64_t RequestPrefix =
        static_cast<std::uint64_t>(std::chrono::system_clock::now().time_since_epoch().count());

    explicit TImpl(TLogOptions options)
        : Options(std::move(options))
    { }

    bool AdmitSuccess() noexcept
    {
        if (Sequence.fetch_add(1, std::memory_order_relaxed) % Options.SuccessSampleEvery != 0) {
            Count(*Stats, &TAtomicStats::Sampled);
            return false;
        }
        const auto second = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count());
        auto window = SuccessWindow.load(std::memory_order_relaxed);
        const auto sameSecond = (window >> 32U) == second;
        if (sameSecond && (window & 0xffffffffU) >= Options.MaxSuccessPerSecond) {
            Count(*Stats, &TAtomicStats::RateLimited);
            return false;
        }
        const auto next = (second << 32U) | (sameSecond ? (window & 0xffffffffU) + 1 : 1);
        if (!SuccessWindow.compare_exchange_strong(window, next, std::memory_order_relaxed)) {
            Count(*Stats, &TAtomicStats::RateLimited);
            return false;
        }
        return true;
    }
};

TLogStats GetProcessLogStats() noexcept
{
    return ReadStats(ProcessStats);
}

std::expected<std::unique_ptr<TStructuredLogger>, std::error_code> TStructuredLogger::Open(const TLogOptions& options)
{
    if (options.SuccessSampleEvery == 0 || options.MaxSuccessPerSecond == 0 || options.MaxSuccessPerSecond > 1000000) {
        return std::unexpected(std::make_error_code(std::errc::invalid_argument));
    }
    std::lock_guard lock(LifecycleMutex);
    if (HasLogger || !RegisterThread()) {
        return std::unexpected(std::make_error_code(std::errc::resource_unavailable_try_again));
    }
    try {
        auto impl = std::make_unique<TImpl>(options);
        quill::BackendOptions backendOptions;
        backendOptions.transit_event_buffer_initial_capacity = 16;
        backendOptions.transit_events_soft_limit = 16;
        backendOptions.transit_events_hard_limit = 16;
        backendOptions.error_notifier = [stats = impl->Stats](const std::string& message) {
            OnBackendNotification(*stats, message);
        };
        // A closed runtime pipe must report an error instead of terminating the
        // application. Only the backend inherits the temporary SIGPIPE mask.
        sigset_t mask;
        sigset_t previous;
        sigemptyset(&mask);
        sigaddset(&mask, SIGPIPE);
        const auto masked = ::pthread_sigmask(SIG_BLOCK, &mask, &previous) == 0;
        try {
            quill::Backend::start(backendOptions);
        } catch (...) {
            if (masked) {
                ::pthread_sigmask(SIG_SETMASK, &previous, nullptr);
            }
            throw;
        }
        if (masked) {
            ::pthread_sigmask(SIG_SETMASK, &previous, nullptr);
        }
        std::shared_ptr<quill::Sink> sink;
        if (options.Directory.empty()) {
            sink = TFrontend::create_or_get_sink<TStderrJsonSink>("glamdb.stderr");
        } else {
            const std::filesystem::path directory(options.Directory);
            if (std::filesystem::create_directories(directory)) {
                std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
            }
            quill::RotatingFileSinkConfig config;
            config.set_open_mode('a');
            config.set_rotation_max_file_size(MaxFileBytes);
            config.set_max_backup_files(MaxBackupFiles);
            config.set_overwrite_rolled_files(true);
            config.set_write_buffer_size(0);
            sink = TFrontend::create_or_get_sink<quill::RotatingJsonFileSink>((directory / "glamdb.jsonl").string(),
                config);
        }
        impl->Logger = TFrontend::create_or_get_logger("glamdb",
            std::move(sink),
            quill::PatternFormatterOptions{"", "%H:%M:%S.%Qns", quill::Timezone::GmtTime},
            quill::ClockSourceType::System);
        auto logger = std::unique_ptr<TStructuredLogger>(new TStructuredLogger(std::move(impl)));
        HasLogger = true;
        return logger;
    } catch (...) {
        quill::Backend::stop();
        return std::unexpected(std::make_error_code(std::errc::io_error));
    }
}

TStructuredLogger::TStructuredLogger(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{ }

TStructuredLogger::~TStructuredLogger()
{
    Shutdown();
}

bool TStructuredLogger::RegisterProducerThread() noexcept
{
    return !Impl_->Stopped.load() && RegisterThread();
}

bool TStructuredLogger::TryLog(const TLogEvent& event) noexcept
{
    auto& impl = *Impl_;
    impl.InFlight.fetch_add(1);
    struct TAdmission
    {
        std::atomic<std::uint64_t>& InFlight;
        ~TAdmission()
        {
            InFlight.fetch_sub(1);
        }
    } admission{impl.InFlight};
    Count(*impl.Stats, &TAtomicStats::Submitted);
    if (impl.Stopped.load()) {
        Count(*impl.Stats, &TAtomicStats::DroppedStopped);
        return false;
    }
    if (!IsRegistered) {
        Count(*impl.Stats, &TAtomicStats::DroppedUnregistered);
        return false;
    }
    if (event.Event == ELogEvent::RequestCompleted && event.Outcome == ELogOutcome::Ok && !impl.AdmitSuccess()) {
        return false;
    }
    try {
        char requestID[33];
        std::snprintf(requestID,
            sizeof(requestID),
            "%016llx%016llx",
            static_cast<unsigned long long>(impl.RequestPrefix),
            static_cast<unsigned long long>(event.RequestID == 0 ? NewRequestID() : event.RequestID));
        static constexpr quill::MacroMetadata Metadata{"structured_log:0",
            "TryLog",
            "{schema} {service} {event} {level} {operation} {outcome} {request_id} {duration_us} {row_count} "
            "{status_code}",
            nullptr,
            quill::LogLevel::Info,
            quill::MacroMetadata::Event::Log};
        const auto admitted = impl.Logger->log_statement<false>(&Metadata,
            1,
            "glamdb",
            GetEventName(event.Event),
            GetLevel(event),
            GetOperationName(event.Operation),
            GetOutcomeName(event.Outcome),
            requestID,
            event.DurationUs,
            event.RowCount,
            event.StatusCode);
        Count(*impl.Stats, admitted ? &TAtomicStats::Accepted : &TAtomicStats::DroppedFull);
        return admitted;
    } catch (...) {
        Count(*impl.Stats, &TAtomicStats::AdapterErrors);
        return false;
    }
}

std::uint64_t TStructuredLogger::NewRequestID() noexcept
{
    return Impl_->RequestID.fetch_add(1, std::memory_order_relaxed) + 1;
}

TLogStats TStructuredLogger::GetStats() const noexcept
{
    return ReadStats(*Impl_->Stats);
}

void TStructuredLogger::Shutdown() noexcept
{
    if (Impl_->Stopped.exchange(true)) {
        return;
    }
    while (Impl_->InFlight.load() != 0) {
        std::this_thread::yield();
    }
    try {
        std::lock_guard lock(LifecycleMutex);
        TFrontend::remove_logger(Impl_->Logger);
        quill::Backend::stop();
        HasLogger = false;
    } catch (...) {
        Count(*Impl_->Stats, &TAtomicStats::AdapterErrors);
        // Attempt the join even after a removal/locking error. A rejected
        // backend-thread stop remains observable; shared callback state stays
        // owned by Quill until it actually stops.
        try {
            quill::Backend::stop();
        } catch (...) {
            Count(*Impl_->Stats, &TAtomicStats::AdapterErrors);
        }
    }
}

thread_local TLogScope* TLogScope::Current_ = nullptr;

TLogScope::TLogScope(TStructuredLogger* logger, ELogOperation operation, std::uint64_t requestID) noexcept
    : Logger_(logger)
    , Operation_(operation)
    , RequestID_(requestID)
    , Previous_(Current_)
{
    Current_ = this;
}

TLogScope::~TLogScope()
{
    Current_ = Previous_;
}

void TLogScope::Emit(ELogEvent event, ELogOutcome outcome) noexcept
{
    if (Current_ && Current_->Logger_) {
        Current_->Logger_->TryLog(
            {.Event = event, .Operation = Current_->Operation_, .Outcome = outcome, .RequestID = Current_->RequestID_});
    }
}

TLogRuntime::TLogRuntime(const TLogOptions& options) noexcept
{
    try {
        auto opened = TStructuredLogger::Open(options);
        if (opened) {
            Logger_ = std::move(*opened);
            Logger_->TryLog({.Event = ELogEvent::RuntimeStarted});
        }
    } catch (...) {
        // Operational telemetry never prevents the application from running.
        ProcessStats.AdapterErrors.fetch_add(1, std::memory_order_relaxed);
    }
}

TLogRuntime::~TLogRuntime()
{
    if (Logger_) {
        Logger_->TryLog({.Event = ELogEvent::RuntimeStopped, .Outcome = Outcome_});
    }
}

TStructuredLogger* TLogRuntime::Get() const noexcept
{
    return Logger_.get();
}

void TLogRuntime::SetResult(bool success) noexcept
{
    Outcome_ = success ? ELogOutcome::Ok : ELogOutcome::Error;
}

} // namespace NGlamDB::NTelemetry
