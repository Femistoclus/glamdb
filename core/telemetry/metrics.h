#pragma once

#include "storage/types.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string_view>

namespace prometheus {
class Collectable;
}

namespace NGlamDB::NTelemetry {

enum class EOperation : std::uint8_t { Append, CreateCollection };

// Internal process-wide accounting. All label domains and storage are fixed.
void RecordPhase(EOperation operation, glamdb::CommitPhase phase, double seconds) noexcept;
void RecordFinished(EOperation operation,
    glamdb::CommitDisposition outcome,
    glamdb::CommitPhase phase,
    std::string_view error,
    double seconds,
    std::uint64_t rows = 0,
    std::uint64_t bytes = 0) noexcept;
void RecordIncomplete(EOperation operation, double seconds) noexcept;
void AdjustInFlight(EOperation operation, int64_t delta) noexcept;
void AdjustAdmission(int64_t pendingDelta, int64_t bytesDelta) noexcept;
void RecordPublished(std::uint64_t rows, std::uint64_t bytes) noexcept;

enum class EReadOperation : std::uint8_t {
    CreateStore,
    OpenStore,
    CloseStore,
    Reconcile,
    ScanOpen,
    ScanNext,
    ScanCancel
};
void RecordReadFinished(EReadOperation operation,
    std::string_view error,
    double seconds,
    std::uint64_t rows = 0,
    std::uint64_t bytes = 0) noexcept;
void AdjustReadInFlight(EReadOperation operation, int64_t delta) noexcept;
void AdjustRetainedCursors(int64_t delta) noexcept;

class TReadOperationScope
{
public:
    explicit TReadOperationScope(EReadOperation operation) noexcept
        : Operation_(operation)
    {
        AdjustReadInFlight(Operation_, 1);
    }
    ~TReadOperationScope()
    {
        if (!Finished_) {
            Error("INCOMPLETE");
        }
        AdjustReadInFlight(Operation_, -1);
    }
    TReadOperationScope(const TReadOperationScope&) = delete;
    TReadOperationScope& operator=(const TReadOperationScope&) = delete;

    template <class TResult>
    TResult Finish(TResult result, std::uint64_t rows = 0, std::uint64_t bytes = 0) noexcept
    {
        End(result ? std::string_view{} : result.error().code, rows, bytes);
        return result;
    }
    void Finish() noexcept
    {
        End({}, 0, 0);
    }
    void Error(std::string_view code) noexcept
    {
        End(code, 0, 0);
    }

private:
    void End(std::string_view code, std::uint64_t rows, std::uint64_t bytes) noexcept
    {
        if (!Finished_) {
            Finished_ = true;
            RecordReadFinished(Operation_,
                code,
                std::chrono::duration<double>(std::chrono::steady_clock::now() - Start_).count(),
                rows,
                bytes);
        }
    }
    EReadOperation Operation_;
    std::chrono::steady_clock::time_point Start_ = std::chrono::steady_clock::now();
    bool Finished_ = false;
};
// Private application seam: register with an application-owned prometheus::Exposer.
// No listener, thread, textfile or public engine API is created here.
std::shared_ptr<prometheus::Collectable> GetMetricsCollectable();

template <class TOutcome>
class TOperationScope
{
public:
    explicit TOperationScope(EOperation operation) noexcept
        : Operation_(operation)
    {
        AdjustInFlight(Operation_, 1);
    }

    ~TOperationScope()
    {
        if (!Finished_) {
            OnPhaseEnd();
            RecordIncomplete(Operation_, GetSeconds(Start_));
        }
        AdjustInFlight(Operation_, -1);
    }

    TOperationScope(const TOperationScope&) = delete;
    TOperationScope& operator=(const TOperationScope&) = delete;

    void EnterPhase(glamdb::CommitPhase phase) noexcept
    {
        OnPhaseEnd();
        Phase_ = phase;
        PhaseStart_ = TClock::now();
    }

    TOutcome Finish(TOutcome outcome) noexcept
    {
        OnPhaseEnd();
        std::uint64_t rows = 0, bytes = 0;
        if constexpr (requires {
                          outcome.result->rows_appended;
                          outcome.result->compressed_data_bytes_added;
                      })
        {
            if (outcome.result) {
                rows =
                    outcome.result->rows_appended > 0 ? static_cast<std::uint64_t>(outcome.result->rows_appended) : 0;
                bytes = outcome.result->compressed_data_bytes_added > 0
                    ? static_cast<std::uint64_t>(outcome.result->compressed_data_bytes_added)
                    : 0;
            }
        }
        RecordFinished(Operation_,
            outcome.disposition,
            outcome.phase,
            outcome.error ? outcome.error->code : std::string_view{},
            GetSeconds(Start_),
            rows,
            bytes);
        Finished_ = true;
        return outcome;
    }

private:
    using TClock = std::chrono::steady_clock;

    static double GetSeconds(TClock::time_point start) noexcept
    {
        return std::chrono::duration<double>(TClock::now() - start).count();
    }

    void OnPhaseEnd() noexcept
    {
        RecordPhase(Operation_, Phase_, GetSeconds(PhaseStart_));
    }

    EOperation Operation_;
    TClock::time_point Start_ = TClock::now();
    TClock::time_point PhaseStart_ = Start_;
    glamdb::CommitPhase Phase_ = glamdb::CommitPhase::Validation;
    bool Finished_ = false;
};

} // namespace NGlamDB::NTelemetry
