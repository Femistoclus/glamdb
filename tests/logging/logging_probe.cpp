#include "core/telemetry/structured_log.h"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

using namespace NGlamDB::NTelemetry;

int main(int argc, char** argv)
try {
    if (argc != 4) {
        return 2;
    }
    const std::string mode = argv[1];
    TLogOptions options;
    options.SuccessSampleEvery = mode == "sample" ? 10 : mode == "default" ? 100 : 1;
    options.MaxSuccessPerSecond = mode == "rate" ? 2 : mode == "default" ? 20 : 1000000;
    auto opened = TStructuredLogger::Open(options);
    if (!opened) {
        return 3;
    }
    auto& logger = **opened;
    const auto before = std::chrono::steady_clock::now();
    logger.TryLog({.Event = ELogEvent::RuntimeStarted});
    const auto count = std::stoull(argv[3]);
    const auto produce = [&logger, count, &mode]() {
        if (!logger.RegisterProducerThread()) {
            logger.TryLog({.Event = ELogEvent::StorageFailed});
            return;
        }
        for (std::uint64_t index = 0; index < count; ++index) {
            const auto error = mode != "sample" && mode != "rate" && mode != "default";
            logger.TryLog({.Event = ELogEvent::RequestCompleted,
                .Operation = ELogOperation::Append,
                .Outcome = error ? ELogOutcome::Error : ELogOutcome::Ok,
                .RequestID = logger.NewRequestID(),
                .DurationUs = mode == "maximum" ? std::numeric_limits<std::uint64_t>::max() : 123,
                .RowCount = mode == "maximum" ? std::numeric_limits<std::uint64_t>::max() : 7,
                .StatusCode = error ? 503U : 200U});
        }
    };
    if (mode == "stress" || mode == "threads" || mode == "shutdown-race") {
        std::vector<std::thread> threads;
        for (unsigned index = 0; index < (mode == "threads" ? 64U : 8U); ++index) {
            threads.emplace_back(produce);
        }
        if (mode == "shutdown-race") {
            logger.Shutdown();
        }
        for (auto& thread : threads) {
            thread.join();
        }
    } else {
        produce();
    }
    logger.TryLog({.Event = ELogEvent::RuntimeStopped});
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - before).count();
    if (mode != "blocked") {
        logger.Shutdown();
    }
    const auto stats = logger.GetStats();
    if (mode == "reopen") {
        opened->reset();
        auto reopened = TStructuredLogger::Open(options);
        if (!reopened) {
            return 4;
        }
        (**reopened).TryLog({.Event = ELogEvent::RuntimeStarted});
        reopened->reset();
    }
    const auto processStats = GetProcessLogStats();
    std::cout << "{\"submitted\":" << stats.Submitted << ",\"accepted\":" << stats.Accepted
              << ",\"process_accepted\":" << processStats.Accepted << ",\"sampled\":" << stats.Sampled
              << ",\"rate_limited\":" << stats.RateLimited << ",\"dropped_full\":" << stats.DroppedFull
              << ",\"dropped_unregistered\":" << stats.DroppedUnregistered
              << ",\"dropped_stopped\":" << stats.DroppedStopped
              << ",\"backend_notifications\":" << stats.BackendNotifications
              << ",\"quill_reported_dropped\":" << stats.QuillReportedDropped << ",\"sink_errors\":" << stats.SinkErrors
              << ",\"producer_threads\":" << stats.ProducerThreads
              << ",\"queue_capacity_bytes\":" << stats.QueueCapacityBytes
              << ",\"max_producer_threads\":" << stats.MaxProducerThreads << ",\"elapsed_us\":" << elapsed << "}\n"
              << std::flush;
    if (mode == "blocked") {
        logger.Shutdown();
    }
    return 0;
} catch (...) {
    return 100;
}
