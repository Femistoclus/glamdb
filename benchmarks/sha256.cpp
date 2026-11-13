// CPU-only hashing benchmark. Native append measurements use the separate
// glamdb_bulk_ingestion_bench ordinary profile (including reopen/retry checks).
#include "core/io/sha256.h"
#include "core/api/canonical.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

#if !defined(GLAMDB_HAS_INTERCEPTING_SANITIZER)
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define GLAMDB_HAS_INTERCEPTING_SANITIZER 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer) || __has_feature(memory_sanitizer) ||          \
    __has_feature(leak_sanitizer)
#define GLAMDB_HAS_INTERCEPTING_SANITIZER 1
#endif
#endif
#endif

namespace {

#if defined(GLAMDB_HAS_INTERCEPTING_SANITIZER)
constexpr bool AllocationCountingAvailable = false;
#else
constexpr bool AllocationCountingAvailable = true;
#endif

// Count ordinary C++ allocations on the measured thread only. This is not an
// allocator/RSS profiler and does not include direct malloc or aligned new.
thread_local bool CountAllocations = false;
thread_local std::size_t Allocations = 0;

} // namespace

#if !defined(GLAMDB_HAS_INTERCEPTING_SANITIZER)

void* operator new(std::size_t size)
{
    if (auto* memory = std::malloc(std::max(size, std::size_t{1}))) {
        if (CountAllocations) {
            ++Allocations;
        }
        return memory;
    }
    throw std::bad_alloc();
}

void* operator new[](std::size_t size)
{
    return ::operator new(size);
}

void operator delete(void* memory) noexcept
{
    std::free(memory);
}

void operator delete[](void* memory) noexcept
{
    std::free(memory);
}

void operator delete(void* memory, std::size_t) noexcept
{
    std::free(memory);
}

void operator delete[](void* memory, std::size_t) noexcept
{
    std::free(memory);
}

#endif

namespace {

template <class THash>
void Measure(std::string_view name, std::string_view bytes, std::size_t chunk, THash hash)
{
    // Independent Python hashlib.sha256(bytes((i * 37 + 11) % 256 for i in range(16 << 20))).
    constexpr auto expected = "83f8f2389035d0705d74fff395a71627033e2457b9f5ddbea7b7f6fd874af66c";
    for (int trial = -1; trial < 5; ++trial) {
        Allocations = 0;
        CountAllocations = true;
        const auto cpuStart = std::clock();
        const auto start = std::chrono::steady_clock::now();
        const auto digest = hash(bytes, chunk);
        const auto end = std::chrono::steady_clock::now();
        const auto cpuEnd = std::clock();
        CountAllocations = false;
        if (digest != expected) {
            throw std::runtime_error("digest differs from independent fixture");
        }
        if (trial >= 0) {
            const auto wallMs = std::chrono::duration<double, std::milli>(end - start).count();
            const auto cpuMs = 1000.0 * static_cast<double>(cpuEnd - cpuStart) / CLOCKS_PER_SEC;
            const auto allocations = AllocationCountingAvailable ? std::to_string(Allocations) : "unavailable";
            std::printf("%.*s,%zu,%d,%zu,%.6f,%.6f,%s,%.3f,%s\n",
                static_cast<int>(name.size()),
                name.data(),
                chunk,
                trial,
                bytes.size(),
                wallMs,
                cpuMs,
                allocations.c_str(),
                static_cast<double>(bytes.size()) / (1024 * 1024) / (wallMs / 1000),
                digest.c_str());
        }
    }
}

} // namespace

int main()
try {
    if (!AllocationCountingAvailable) {
        std::fputs("C++ allocation counting is unavailable under intercepting sanitizers.\n", stderr);
    }
    // Allocate/generate once, outside all timed regions. The leading byte also
    // keeps every large direct-block Update deliberately unaligned.
    std::string input((16 << 20) + 1, '\0');
    for (std::size_t i = 1; i < input.size(); ++i) {
        input[i] = static_cast<char>(((i - 1) * 37 + 11) % 256);
    }
    const auto bytes = std::string_view(input).substr(1);
    std::puts("wrapper,chunk_bytes,trial,input_bytes,wall_ms,cpu_ms,cpp_allocations,mib_per_second,digest");
    for (const std::size_t chunk : {64, 4096, 1048576}) {
        Measure("io", bytes, chunk, [](std::string_view remaining, std::size_t count) {
            NGlamDB::NIO::TSha256 hash;
            while (!remaining.empty()) {
                const auto part = remaining.substr(0, count);
                if (hash.Update(std::as_bytes(std::span(part.data(), part.size())))) {
                    throw std::runtime_error("IO hash update refused fixture");
                }
                remaining.remove_prefix(part.size());
            }
            return NGlamDB::NIO::HexDigest(hash.Finalize());
        });
        Measure("canonical", bytes, chunk, [](std::string_view remaining, std::size_t count) {
            glamdb::detail::Sha256State hash;
            while (!remaining.empty()) {
                const auto part = remaining.substr(0, count);
                hash.Update(part);
                remaining.remove_prefix(part.size());
            }
            return hash.Digest();
        });
    }
    return 0;
} catch (const std::exception& error) {
    CountAllocations = false;
    std::fprintf(stderr, "%s\n", error.what());
    return 1;
}
