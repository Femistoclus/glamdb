#include "core/api/bulk_input.h"

#include "core/api/binary.h"

#include <arrow/record_batch.h>

namespace glamdb::detail {
namespace {

template <typename T>
T TakeBulk(Result<T> result)
{
    if (!result) {
        throw Failure(result.error());
    }
    return std::move(*result);
}

class TBulkReader
{
public:
    TBulkReader(SchemaSpec schema,
        BulkAppendRequest request,
        std::chrono::steady_clock::time_point deadline,
        std::function<Status()> checkHealth = {})
        : Schema_(std::move(schema))
        , Request_(std::move(request))
        , Deadline_(deadline)
        , CheckHealth_(std::move(checkHealth))
    {
        Check();
        Hash_.AddBytes(TakeBulk(EncodeBulkAppendHeader(Schema_, Request_.collection_id, Request_.rows)));
        const auto rewound = Request_.source->Rewind();
        if (!rewound) {
            throw Failure(rewound.error());
        }
        Check();
    }

    std::shared_ptr<arrow::RecordBatch> Next()
    {
        Check();
        auto batch = TakeBulk(Request_.source->Next());
        Check();
        if (!batch) {
            Require(Rows_ == Request_.rows, "INVALID_ARGUMENT", "bulk source row count differs from its declaration");
            return nullptr;
        }
        Require(batch->num_rows() > 0 && batch->num_rows() <= Request_.rows - Rows_,
            "INVALID_ARGUMENT",
            "bulk source is empty or exceeds its declared row count");
        const auto bytes = TakeBulk(UpdateFingerprintBatches(Schema_, {batch}, Hash_));
        Require(bytes <= static_cast<size_t>(BulkAppendLimits::MaxDecodedBytes) - Bytes_,
            "RESOURCE_EXHAUSTED",
            "bulk source exceeds its canonical byte limit");
        Bytes_ += bytes;
        Rows_ += batch->num_rows();
        return batch;
    }

    std::string Digest()
    {
        return Hash_.ComputeDigest();
    }

    const SchemaSpec& Schema() const
    {
        return Schema_;
    }

    const std::string& CollectionID() const
    {
        return Request_.collection_id;
    }

private:
    SchemaSpec Schema_;
    BulkAppendRequest Request_;
    std::chrono::steady_clock::time_point Deadline_;
    std::function<Status()> CheckHealth_;
    TCanonicalHasher Hash_;
    int64_t Rows_ = 0;
    size_t Bytes_ = 0;

    void Check() const
    {
        Require(!Request_.cancellation || !Request_.cancellation->load(), "CANCELLED", "bulk append was cancelled");
        Require(std::chrono::steady_clock::now() < Deadline_, "DEADLINE_EXCEEDED", "bulk append deadline expired");
        if (CheckHealth_) {
            const auto healthy = CheckHealth_();
            if (!healthy) {
                throw Failure(healthy.error());
            }
        }
    }
};

} // namespace

Result<std::string> FingerprintBulkSource(const SchemaSpec& schema,
    const BulkAppendRequest& request,
    std::chrono::steady_clock::time_point deadline)
{
    try {
        TBulkReader reader(schema, request, deadline);
        while (reader.Next()) {
        }
        return reader.Digest();
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<std::shared_ptr<TBulkInput>> PrepareBulkInput(const SchemaSpec& schema,
    const BulkAppendRequest& request,
    std::string expectedHash,
    std::chrono::steady_clock::time_point deadline,
    std::function<Status()> checkHealth)
{
    try {
        auto reader = std::make_shared<TBulkReader>(schema, request, deadline, std::move(checkHealth));
        auto input = std::make_shared<TBulkInput>();
        input->Rows = request.rows;
        input->Next = [reader = std::move(reader),
                          expectedHash = std::move(expectedHash)]() -> Result<NormalizedInput> {
            try {
                auto batch = reader->Next();
                if (!batch) {
                    Require(reader->Digest() == expectedHash,
                        "INVALID_ARGUMENT",
                        "bulk source changed between validation and production");
                    return NormalizedInput{};
                }
                return NormalizeBatches(reader->Schema(), reader->CollectionID(), {std::move(batch)});
            } catch (const Failure& error) {
                return std::unexpected(error.detail);
            }
        };
        return input;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

} // namespace glamdb::detail
