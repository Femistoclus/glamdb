#pragma once

#include "storage/read_view.h"

namespace glamdb {
namespace detail {
struct TScanPlanState;
}

struct TScanSplitLimits
{
    size_t MaxSplitsPerPage = 256;
    size_t MaxFilesExaminedPerPage = 4096;
    uint64_t MaxTargetBytes = 1ULL << 30;
    uint64_t MaxPredicateBytes = 16ULL << 20;
};

struct TScanPlanOptions
{
    size_t SplitsPerPage = 64;
    // Soft target of committed whole-file sizes, before pruning/projection.
    // A file (including all its row groups) is indivisible and may exceed it.
    uint64_t TargetBytes = 128ULL << 20;
};

// Native process-local descriptor version 1, not serializable. Retains the
// original view pin; explicit release/cancel/expiry/close stop new admissions.
class TScanSplit final
{
public:
    static constexpr uint32_t Version = 1;
    uint64_t EstimatedBytes() const;
    size_t FileCount() const;

private:
    TScanSplit() = default;
    std::shared_ptr<const void> Memory_;
    std::shared_ptr<detail::TScanPlanState> State_;
    size_t Begin_ = 0;
    size_t End_ = 0;
    size_t FileCount_ = 0;
    uint64_t Bytes_ = 0;
    friend class Engine;
};

class TScanSplitPlan final
{
public:
    // Schema available even when there are no splits. No global output order.
    const ScanInfo& Info() const;

private:
    TScanSplitPlan() = default;
    std::shared_ptr<detail::TScanPlanState> State_;
    friend class Engine;
};

struct TScanSplitPage
{
    // Empty nonterminal pages are possible when a bounded KV inventory window
    // contains no live files. Call again until Done. Successful pages advance;
    // failed pages leave the position unchanged. Retrying a split replays it.
    std::vector<std::shared_ptr<const TScanSplit>> Splits;
    bool Done = false;
};

} // namespace glamdb
