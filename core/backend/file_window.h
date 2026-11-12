#pragma once

#include "core/api/binary.h"

#include <arrow/record_batch.h>
#include <arrow/util/byte_size.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

namespace glamdb::detail {

// Both append and KV producers use the same bounded batch window before the
// shared Parquet writer. Caller chunk boundaries do not force file boundaries.
class TBoundedFileWindow
{
public:
    struct TLimits
    {
        int64_t MaxRows;
        int64_t MaxInputBytes;
        int64_t TargetBytes;
    };

    using TBatches = std::vector<std::shared_ptr<arrow::RecordBatch>>;
    using TFlush = std::function<void(const TBatches&, int64_t)>;

    TBoundedFileWindow(TLimits limits, arrow::MemoryPool* pool, TFlush flush, std::function<void()> check)
        : Limits_(limits)
        , Pool_(pool)
        , Flush_(std::move(flush))
        , Check_(std::move(check))
    {
        Require(Limits_.MaxRows > 0 && Limits_.MaxInputBytes > 0 && Limits_.TargetBytes > 0,
            "INTERNAL",
            "invalid bounded file window limits");
        Batches_.reserve(128);
    }

    void Add(const std::shared_ptr<arrow::RecordBatch>& batch)
    {
        Require(batch != nullptr, "INTERNAL", "missing bounded file window batch");
        const auto bytes = arrow::util::TotalBufferSize(*batch);
        Require(bytes >= 0 && bytes <= Limits_.MaxInputBytes,
            "RESOURCE_EXHAUSTED",
            "source chunk exceeds the file input byte limit");
        for (int64_t offset = 0; offset < batch->num_rows();) {
            Check_();
            const auto count = std::min(batch->num_rows() - offset, Limits_.MaxRows - Rows_);
            auto piece = offset == 0 && count == batch->num_rows() ? batch : batch->Slice(offset, count);
            const auto retained = arrow::util::TotalBufferSize(*piece);
            Require(retained >= 0 && retained <= Limits_.MaxInputBytes,
                "RESOURCE_EXHAUSTED",
                "source slice exceeds the file input byte limit");
            if (retained > Limits_.MaxInputBytes - Bytes_) {
                Flush();
                continue;
            }
            Batches_.push_back(std::move(piece));
            Rows_ += count;
            Bytes_ += retained;
            offset += count;
            // Bound descriptors independently of caller chunk size. Coalescing
            // preserves the old bulk producer's row and byte boundaries.
            if (Batches_.size() >= 128) {
                auto combined = arrow::ConcatenateRecordBatches(Batches_, Pool_);
                if (!combined.ok()) {
                    throw Failure({combined.status().IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : "INTERNAL",
                        combined.status().ToString(),
                        RetryHint::OperatorAction});
                }
                Batches_.assign(1, *combined);
                Bytes_ = arrow::util::TotalBufferSize(*Batches_.front());
                Require(Bytes_ >= 0 && Bytes_ <= Limits_.MaxInputBytes,
                    "RESOURCE_EXHAUSTED",
                    "coalesced buffers exceed the file input byte limit");
            }
            if (Rows_ == Limits_.MaxRows || Bytes_ >= Limits_.TargetBytes) {
                Flush();
            }
        }
    }

    void Flush()
    {
        if (Batches_.empty()) {
            return;
        }
        Check_();
        Flush_(Batches_, Rows_);
        Batches_.clear();
        Rows_ = 0;
        Bytes_ = 0;
    }

private:
    const TLimits Limits_;
    arrow::MemoryPool* const Pool_;
    const TFlush Flush_;
    const std::function<void()> Check_;
    TBatches Batches_;
    int64_t Rows_ = 0;
    int64_t Bytes_ = 0;
};

} // namespace glamdb::detail
