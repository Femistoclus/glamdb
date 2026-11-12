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
    double seconds) noexcept;
void RecordIncomplete(EOperation operation, double seconds) noexcept;
void AdjustInFlight(EOperation operation, int64_t delta) noexcept;
void AdjustAdmission(int64_t pendingDelta, int64_t bytesDelta) noexcept;
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
        RecordFinished(Operation_,
            outcome.disposition,
            outcome.phase,
            outcome.error ? outcome.error->code : std::string_view{},
            GetSeconds(Start_));
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
