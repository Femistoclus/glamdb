#include "core/telemetry/metrics.h"
#include "core/telemetry/structured_log.h"

int main()
{
    using namespace NGlamDB::NTelemetry;
    TLogOptions options;
    options.SuccessSampleEvery = 100;
    options.MaxSuccessPerSecond = 1000;
    auto opened = TStructuredLogger::Open(options);
    if (!opened) {
        return 1;
    }
    auto& logger = **opened;
    {
        const TScopedLogContext outer(&logger, 11);
        if (outer.GetID() != 11) {
            return 3;
        }
        {
            const TScopedLogContext generated(&logger);
            const TScopedLogContext another(&logger);
            if (generated.GetID() == 0 || generated.GetID() == another.GetID()) {
                return 3;
            }
        }
        {
            const TScopedLogContext disabled(nullptr);
            LogCurrent({.Event = ELogEvent::StorageFailed, .Operation = ELogOperation::Other});
        }
        RecordFinished(EOperation::Append,
            glamdb::CommitDisposition::Committed,
            glamdb::CommitPhase::Complete,
            {},
            .001,
            2,
            32);
        {
            const TScopedLogContext inner(&logger, 22);
            RecordFinished(EOperation::Append,
                glamdb::CommitDisposition::CommitUnknown,
                glamdb::CommitPhase::CatalogCommit,
                "IO_ERROR",
                .002);
        }
        RecordReadFinished(EReadOperation::ScanNext, "CORRUPTION", .003);
        RecordReadFinished(EReadOperation::Reconcile, "private-diagnostic-canary", .004);
    }
    logger.TryLog({.Event = ELogEvent::RequestCompleted, .RequestID = 11});
    const auto stats = logger.GetStats();
    logger.Shutdown();
    return stats.Accepted == 5 && stats.DroppedUnregistered == 0 ? 0 : 2;
}
