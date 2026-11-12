// Memory of native append input preparation with many Arrow batches: the
// C++ scratch of inspection, fingerprinting and the owned copy must not grow
// with the batch count, and C++ plus owned Arrow bytes must fit the owned-input
// reservation. C++ heap use is measured by replacing the global allocation
// functions of this test binary with counting wrappers around malloc.

#include "core/api/canonical.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <new>

#include <malloc.h>

namespace {

std::atomic<int64_t> LiveBytes{0};
std::atomic<int64_t> PeakBytes{0};

void Count(int64_t bytes) noexcept
{
    const auto live = LiveBytes.fetch_add(bytes) + bytes;
    auto peak = PeakBytes.load();
    while (live > peak && !PeakBytes.compare_exchange_weak(peak, live)) {
    }
}

// Counts usable block sizes, so a block that some library releases with
// free() only skews the counters instead of corrupting the heap.
void* AllocateCounted(std::size_t size, std::size_t alignment) noexcept
{
    void* pointer = alignment <= __STDCPP_DEFAULT_NEW_ALIGNMENT__
        ? std::malloc(size == 0 ? 1 : size)
        : std::aligned_alloc(alignment, (std::max<std::size_t>(size, 1) + alignment - 1) / alignment * alignment);
    if (pointer != nullptr) {
        Count(static_cast<int64_t>(malloc_usable_size(pointer)));
    }
    return pointer;
}

void ReleaseCounted(void* pointer) noexcept
{
    if (pointer == nullptr) {
        return;
    }
    LiveBytes.fetch_sub(static_cast<int64_t>(malloc_usable_size(pointer)));
    std::free(pointer);
}

void* AllocateOrThrow(std::size_t size, std::size_t alignment)
{
    if (auto* pointer = AllocateCounted(size, alignment)) {
        return pointer;
    }
    throw std::bad_alloc();
}

} // namespace

void* operator new(std::size_t size)
{
    return AllocateOrThrow(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
}
void* operator new[](std::size_t size)
{
    return AllocateOrThrow(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
}
void* operator new(std::size_t size, std::align_val_t alignment)
{
    return AllocateOrThrow(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment)
{
    return AllocateOrThrow(size, static_cast<std::size_t>(alignment));
}
void* operator new(std::size_t size, const std::nothrow_t&) noexcept
{
    return AllocateCounted(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept
{
    return AllocateCounted(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
}
void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept
{
    return AllocateCounted(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept
{
    return AllocateCounted(size, static_cast<std::size_t>(alignment));
}
void operator delete(void* pointer) noexcept
{
    // The analyzer marks delete arguments freed before entering this counting
    // replacement. Only ReleaseCounted actually frees the malloc-backed block.
    // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDelete)
    ReleaseCounted(pointer);
}
void operator delete[](void* pointer) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete(void* pointer, std::align_val_t) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete[](void* pointer, std::align_val_t) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete(void* pointer, const std::nothrow_t&) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete[](void* pointer, const std::nothrow_t&) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete(void* pointer, std::align_val_t, const std::nothrow_t&) noexcept
{
    ReleaseCounted(pointer);
}
void operator delete[](void* pointer, std::align_val_t, const std::nothrow_t&) noexcept
{
    ReleaseCounted(pointer);
}

namespace glamdb::detail {
namespace {

constexpr char CollectionID[] = "00112233-4455-6677-8899-aabbccddeeff";
constexpr int Fields = 64;

// Schema-sized work that does not depend on rows or batches: the schema,
// its fingerprint and encoding, field mappings, one batch's column views,
// per-field owned-column state and Arrow array/batch objects. It is covered
// by the reservation's 64 KiB constant; the bound leaves the same again.
constexpr int64_t SchemaScratchBytes = 64LL * 1024;

// Peak C++ heap growth above the live bytes at the start of `work`.
template <typename TWork>
int64_t MeasureHeapPeak(TWork&& work)
{
    const auto base = LiveBytes.load();
    PeakBytes.store(base);
    work();
    return PeakBytes.load() - base;
}

SchemaSpec Integers()
{
    SchemaSpec schema;
    for (int field = 0; field < Fields; ++field) {
        schema.fields.push_back({"n" + std::to_string(field), FieldType::Int64, true});
    }
    return schema;
}

std::shared_ptr<arrow::RecordBatch> NoColumns(int64_t rows)
{
    return arrow::RecordBatch::Make(arrow::schema({}), rows, std::vector<std::shared_ptr<arrow::Array>>{});
}

std::shared_ptr<arrow::RecordBatch> OneRow(int64_t value)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (int field = 0; field < Fields; ++field) {
        arrow::Int64Builder builder;
        EXPECT_TRUE(builder.Append(value + field).ok());
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());
        fields.push_back(arrow::field("n" + std::to_string(field), arrow::int64()));
        columns.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), 1, std::move(columns));
}

struct TPreparationMemory
{
    size_t Reservation = 0;
    int64_t FingerprintHeap = 0;
    int64_t NormalizeHeap = 0;
    int64_t NormalizeArrow = 0;
};

TPreparationMemory Measure(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    const auto schema = Integers();
    TPreparationMemory memory;
    memory.FingerprintHeap = MeasureHeapPeak([&] {
        const auto fingerprinted = FingerprintInput(schema, CollectionID, batches);
        EXPECT_TRUE(fingerprinted) << fingerprinted.error().message;
        memory.Reservation = fingerprinted ? fingerprinted->Reservation : 0;
    });
    arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
    memory.NormalizeHeap = MeasureHeapPeak([&] {
        const auto normalized = NormalizeBatches(schema, CollectionID, batches, memory.Reservation, &pool);
        EXPECT_TRUE(normalized) << normalized.error().message;
    });
    memory.NormalizeArrow = pool.max_memory();
    return memory;
}

void ExpectBounded(const TPreparationMemory& many, const TPreparationMemory& single)
{
    SCOPED_TRACE(
        ::testing::Message() << "reservation=" << many.Reservation << " fingerprint heap=" << many.FingerprintHeap
                             << " normalize heap=" << many.NormalizeHeap << " normalize arrow=" << many.NormalizeArrow
                             << " single-batch heap=" << single.FingerprintHeap << "/" << single.NormalizeHeap);
    std::printf("reservation=%zu fingerprint_heap=%lld normalize_heap=%lld normalize_arrow=%lld "
                "single_batch_heap=%lld/%lld\n",
        many.Reservation,
        static_cast<long long>(many.FingerprintHeap),
        static_cast<long long>(many.NormalizeHeap),
        static_cast<long long>(many.NormalizeArrow),
        static_cast<long long>(single.FingerprintHeap),
        static_cast<long long>(single.NormalizeHeap));
    EXPECT_EQ(many.Reservation, single.Reservation);
    // C++ scratch does not depend on how the same rows are split into batches.
    EXPECT_LE(many.FingerprintHeap, single.FingerprintHeap + 16LL * 1024);
    EXPECT_LE(many.NormalizeHeap, single.NormalizeHeap + 16LL * 1024);
    // Everything normalization allocates fits the admitted reservation.
    EXPECT_LE(many.NormalizeHeap + many.NormalizeArrow, static_cast<int64_t>(many.Reservation) + SchemaScratchBytes);
    EXPECT_LE(many.FingerprintHeap, SchemaScratchBytes);
}

TEST(ArrowInputScratch, EmptyHeavyBatchesKeepScratchBounded)
{
    std::vector<std::shared_ptr<arrow::RecordBatch>> many;
    many.reserve(10000);
    // Distinct schemas ensure a cache cannot retain one mapping per schema.
    for (int index = 0; index < 9999; ++index) {
        many.push_back(NoColumns(0));
    }
    many.push_back(NoColumns(1));
    ExpectBounded(Measure(many), Measure({NoColumns(1)}));
}

// Caller-owned schema names are outside the decoded-value budget. Rejecting an
// unknown field must not copy or retain arbitrarily large names in core scratch.
TEST(ArrowInputScratch, RejectedFieldNamesKeepScratchBounded)
{
    const SchemaSpec schema{{{"n", FieldType::Int64, true}}};
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.Append(1).ok());
    const auto array = builder.Finish().ValueOrDie();
    for (const size_t length : {128ULL * 1024, 8ULL * 1024 * 1024}) {
        SCOPED_TRACE(length);
        const auto batch =
            arrow::RecordBatch::Make(arrow::schema({arrow::field(std::string(length, 'x'), arrow::int64())}),
                1,
                {array});
        const std::vector<std::shared_ptr<arrow::RecordBatch>> batches{batch};
        const auto fingerprintHeap = MeasureHeapPeak([&] {
            const auto result = FingerprintInput(schema, CollectionID, batches);
            EXPECT_FALSE(result);
            if (!result) {
                EXPECT_EQ(result.error().code, "INVALID_ARGUMENT");
                EXPECT_EQ(result.error().retry_hint, RetryHint::FixInput);
            }
        });
        const auto normalizedHeap = MeasureHeapPeak([&] {
            const auto result = NormalizeBatches(schema, CollectionID, batches);
            EXPECT_FALSE(result);
            if (!result) {
                EXPECT_EQ(result.error().code, "INVALID_ARGUMENT");
                EXPECT_EQ(result.error().retry_hint, RetryHint::FixInput);
            }
        });
        std::printf("rejected_name_bytes=%zu fingerprint_heap=%lld normalize_heap=%lld\n",
            length,
            static_cast<long long>(fingerprintHeap),
            static_cast<long long>(normalizedHeap));
        EXPECT_LE(fingerprintHeap, SchemaScratchBytes);
        EXPECT_LE(normalizedHeap, SchemaScratchBytes);
    }
}

// Arrow's full type comparison/diagnostic can materialize the entire timezone
// of an incompatible actual array, even when the batch schema itself is valid.
TEST(ArrowInputScratch, RejectedArrayTypeParametersKeepScratchBounded)
{
    const SchemaSpec schema{{{"at", FieldType::TimestampUsUtc, true}}};
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.Append(1).ok());
    auto data = builder.Finish().ValueOrDie()->data()->Copy();
    data->type = arrow::timestamp(arrow::TimeUnit::MICRO, std::string(8ULL * 1024 * 1024, 'X'));
    const auto batch =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("at", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))}),
            1,
            std::vector<std::shared_ptr<arrow::ArrayData>>{std::move(data)});
    const std::vector<std::shared_ptr<arrow::RecordBatch>> batches{batch};
    const auto fingerprintHeap = MeasureHeapPeak([&] {
        const auto result = FingerprintInput(schema, CollectionID, batches);
        EXPECT_FALSE(result);
        if (!result) {
            EXPECT_EQ(result.error().code, "INVALID_ARGUMENT");
            EXPECT_EQ(result.error().retry_hint, RetryHint::FixInput);
        }
    });
    const auto normalizedHeap = MeasureHeapPeak([&] {
        const auto result = NormalizeBatches(schema, CollectionID, batches);
        EXPECT_FALSE(result);
        if (!result) {
            EXPECT_EQ(result.error().code, "INVALID_ARGUMENT");
            EXPECT_EQ(result.error().retry_hint, RetryHint::FixInput);
        }
    });
    std::printf("rejected_timezone fingerprint_heap=%lld normalize_heap=%lld\n",
        static_cast<long long>(fingerprintHeap),
        static_cast<long long>(normalizedHeap));
    EXPECT_LE(fingerprintHeap, SchemaScratchBytes);
    EXPECT_LE(normalizedHeap, SchemaScratchBytes);
}

TEST(ArrowInputScratch, SingleRowBatchesKeepScratchBounded)
{
    // Build the values once, then slice. Constructing 640,000 separate builders
    // measures caller setup rather than preparation and exceeds sanitizer timeouts.
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (int field = 0; field < Fields; ++field) {
        arrow::Int64Builder builder;
        for (int64_t index = 0; index < 10000; ++index) {
            EXPECT_TRUE(builder.Append(index + field).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());
        columns.push_back(std::move(array));
    }
    const auto single = arrow::RecordBatch::Make(OneRow(0)->schema(), 10000, std::move(columns));
    std::vector<std::shared_ptr<arrow::RecordBatch>> many;
    many.reserve(10000);
    for (int64_t index = 0; index < 10000; ++index) {
        many.push_back(single->Slice(index, 1));
    }
    ExpectBounded(Measure(many), Measure({single}));
}

} // namespace
} // namespace glamdb::detail
