// Structural sharing of the verified scan index between generations. The
// reference below is the copying construction and the full-walk accounting
// used before generations shared their descriptors; heap allocations are
// counted by replacing the global allocation functions of this test binary.

#include "core/storage/base_state.h"
#include "core/storage/scan_index.h"

#include <gtest/gtest.h>
#include <iceberg/manifest/manifest_entry.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <iostream>
#include <mutex>
#include <new>
#include <random>
#include <set>
#include <span>
#include <thread>

namespace {

thread_local bool CountAllocations = false;
thread_local std::uint64_t Allocations = 0;

void* Allocate(std::size_t size, std::size_t alignment) noexcept
{
    if (CountAllocations) {
        ++Allocations;
    }
    size = std::max<std::size_t>(size, 1);
    return alignment <= __STDCPP_DEFAULT_NEW_ALIGNMENT__
        ? std::malloc(size)
        : std::aligned_alloc(alignment, (size + alignment - 1) / alignment * alignment);
}

void* AllocateOrThrow(std::size_t size, std::size_t alignment)
{
    if (auto* pointer = Allocate(size, alignment)) {
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
    return Allocate(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept
{
    return Allocate(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
}
void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept
{
    return Allocate(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept
{
    return Allocate(size, static_cast<std::size_t>(alignment));
}
// The analyzer marks delete arguments freed before entering these replacements.
// NOLINTBEGIN(clang-analyzer-cplusplus.NewDelete)
void operator delete(void* pointer) noexcept
{
    std::free(pointer);
}
void operator delete[](void* pointer) noexcept
{
    std::free(pointer);
}
void operator delete(void* pointer, std::size_t) noexcept
{
    std::free(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept
{
    std::free(pointer);
}
void operator delete(void* pointer, std::align_val_t) noexcept
{
    std::free(pointer);
}
void operator delete[](void* pointer, std::align_val_t) noexcept
{
    std::free(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept
{
    std::free(pointer);
}
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept
{
    std::free(pointer);
}
// NOLINTEND(clang-analyzer-cplusplus.NewDelete)

namespace NGlamDB::NStorage {
namespace {

constexpr auto Location = "store://64000000-0000-4000-8000-000000000001/warehouse/64000000-0000-4000-8000-000000000002";
constexpr auto AttemptID = "55555555-5555-7555-8555-555555555555";

class TAllocationCount
{
public:
    TAllocationCount()
    {
        Allocations = 0;
        CountAllocations = true;
    }
    ~TAllocationCount()
    {
        CountAllocations = false;
    }
    TAllocationCount(const TAllocationCount&) = delete;
    TAllocationCount& operator=(const TAllocationCount&) = delete;

    std::uint64_t Stop()
    {
        CountAllocations = false;
        return Allocations;
    }
};

TScanDataFile Descriptor(std::uint64_t serial)
{
    auto file = std::make_shared<iceberg::DataFile>();
    file->file_path = std::format("{}/data/{:012}-{}-00000.parquet", Location, serial, AttemptID);
    file->record_count = static_cast<std::int64_t>(1 + serial % 7);
    file->file_size_in_bytes = static_cast<std::int64_t>(900 + serial % 1000);
    file->column_sizes = {{1, 400}, {2, 300}};
    file->value_counts = {{1, file->record_count}, {2, file->record_count}};
    file->null_value_counts = {{1, 0}, {2, static_cast<std::int64_t>(serial % 2)}};
    file->lower_bounds = {{1, std::vector<std::uint8_t>(1 + serial % 5, 'a')}, {2, std::vector<std::uint8_t>(8, 0)}};
    file->upper_bounds = {{1, std::vector<std::uint8_t>(3, 'z')}, {2, std::vector<std::uint8_t>(8, 1)}};
    return {file->file_path, file->file_size_in_bytes, file->record_count, file};
}

std::string ManifestPath(std::uint64_t serial)
{
    // Opaque random names, as written by the pinned Iceberg writers.
    return std::format("{}/metadata/{:016x}-{:016x}-m0.avro", Location, serial * 0x9E3779B97F4A7C15ULL, serial);
}

struct TAppend
{
    std::vector<TScanDataFile> Files;
    std::int64_t SnapshotID = 0;
    std::vector<std::string> Manifests;
};

// What the publication guard and CheckAppendClosure do with a generation.
struct TGeneration
{
    std::shared_ptr<const TScanMembership> Index = std::make_shared<const TScanMembership>();
    TSortedSnapshotIDs Expired;
    TManifestPathSet Manifests;
};

TGeneration Append(const TGeneration& base, TAppend append)
{
    TGeneration next{nullptr, base.Expired, base.Manifests};
    auto index = std::make_shared<TScanMembership>(*base.Index);
    for (auto& file : append.Files) {
        index->Files.push_back(std::move(file));
    }
    for (const auto& path : append.Manifests) {
        next.Manifests.Insert(path);
    }
    const auto boundary =
        std::ranges::lower_bound(index->Snapshots, append.SnapshotID, {}, &TSnapshotBoundary::SnapshotID);
    index->Snapshots.insert(boundary, {append.SnapshotID, index->Files.size()});
    next.Index = std::move(index);
    return next;
}

TGeneration Expire(const TGeneration& base, std::span<const std::int64_t> prefix, std::vector<std::string> retained)
{
    TGeneration next{nullptr, base.Expired, base.Manifests};
    auto index = std::make_shared<TScanMembership>(*base.Index);
    next.Manifests = std::move(retained);
    std::vector<std::int64_t> ids(next.Expired.begin(), next.Expired.end());
    for (const auto id : prefix) {
        ids.push_back(id);
        std::erase_if(index->Snapshots, [&](const auto& boundary) {
            return boundary.SnapshotID == id;
        });
    }
    next.Expired = TSortedSnapshotIDs(std::move(ids));
    next.Index = std::move(index);
    return next;
}

std::uint64_t Bytes(const TGeneration& generation)
{
    return ScanIndexBytes(*generation.Index, generation.Expired, generation.Manifests);
}

// The same per-element terms, walked over every shared element.
std::uint64_t FullWalkBytes(const TGeneration& generation)
{
    const auto& index = *generation.Index;
    std::uint64_t bytes = sizeof(index) + generation.Expired.capacity() * sizeof(std::int64_t) +
        generation.Manifests.size() * ManifestPathBytes;
    generation.Manifests.ForEach([&](const std::string& path) {
        bytes += path.capacity() + 1;
    });
    bytes += index.Files.size() * 2 * sizeof(TScanDataFile) + index.Snapshots.size() * 2 * sizeof(TSnapshotBoundary);
    for (const auto& file : index.Files) {
        bytes += file.Path.capacity() + 1 + (file.DataFile ? DataFileMemory(*file.DataFile) : 0);
    }
    return bytes;
}

// The previous copying construction and its full-walk accounting.
struct TLegacyMembership
{
    std::vector<TScanDataFile> Files;
    std::vector<TSnapshotBoundary> Snapshots;
    bool KeyValue = false;
};

struct TLegacyGeneration
{
    std::shared_ptr<const TLegacyMembership> Index = std::make_shared<const TLegacyMembership>();
    std::vector<std::int64_t> Expired;
    std::vector<std::string> Manifests;
};

TLegacyGeneration LegacyAppend(const TLegacyGeneration& base, const TAppend& append)
{
    TLegacyGeneration next{nullptr, base.Expired, base.Manifests};
    auto index = std::make_shared<TLegacyMembership>(*base.Index);
    for (const auto& file : append.Files) {
        index->Files.push_back(file);
    }
    for (const auto& path : append.Manifests) {
        next.Manifests.push_back(path);
    }
    std::ranges::sort(next.Manifests);
    const auto boundary =
        std::ranges::lower_bound(index->Snapshots, append.SnapshotID, {}, &TSnapshotBoundary::SnapshotID);
    index->Snapshots.insert(boundary, {append.SnapshotID, index->Files.size()});
    next.Index = std::move(index);
    return next;
}

TLegacyGeneration
LegacyExpire(const TLegacyGeneration& base, std::span<const std::int64_t> prefix, std::vector<std::string> retained)
{
    TLegacyGeneration next{nullptr, base.Expired, base.Manifests};
    auto index = std::make_shared<TLegacyMembership>(*base.Index);
    next.Manifests = std::move(retained);
    for (const auto id : prefix) {
        next.Expired.push_back(id);
        std::erase_if(index->Snapshots, [&](const auto& boundary) {
            return boundary.SnapshotID == id;
        });
    }
    std::ranges::sort(next.Expired);
    next.Index = std::move(index);
    return next;
}

std::uint64_t LegacyBytes(const TLegacyGeneration& generation)
{
    const auto& index = *generation.Index;
    std::uint64_t bytes = sizeof(index) + generation.Expired.capacity() * sizeof(std::int64_t) +
        generation.Manifests.size() * 2 * sizeof(std::string);
    for (const auto& path : generation.Manifests) {
        bytes += path.capacity() + 1;
    }
    bytes += index.Files.size() * 2 * sizeof(TScanDataFile) + index.Snapshots.size() * 2 * sizeof(TSnapshotBoundary);
    for (const auto& file : index.Files) {
        bytes += file.Path.capacity() + 1 + (file.DataFile ? DataFileMemory(*file.DataFile) : 0);
    }
    return bytes;
}

void ExpectSame(const TGeneration& actual, const TLegacyGeneration& expected, std::mt19937_64& random)
{
    const auto& files = actual.Index->Files;
    const auto& reference = expected.Index->Files;
    ASSERT_EQ(files.size(), reference.size());
    ASSERT_EQ(files.empty(), reference.empty());
    std::size_t position = 0;
    for (const auto& file : files) {
        ASSERT_EQ(file, reference[position]) << position;
        ASSERT_EQ(file.DataFile, reference[position].DataFile) << position;
        ASSERT_EQ(file.IntroducedSequence, reference[position].IntroducedSequence) << position;
        ASSERT_EQ(file.RemovedSequence, reference[position].RemovedSequence) << position;
        ++position;
    }
    ASSERT_EQ(position, reference.size());
    const auto& boundaries = actual.Index->Snapshots;
    const auto& referenceBoundaries = expected.Index->Snapshots;
    ASSERT_EQ(boundaries.size(), referenceBoundaries.size());
    for (std::size_t boundary = 0; boundary < boundaries.size(); ++boundary) {
        ASSERT_EQ(boundaries[boundary].SnapshotID, referenceBoundaries[boundary].SnapshotID);
        ASSERT_EQ(boundaries[boundary].FileCount, referenceBoundaries[boundary].FileCount);
    }
    // Planner-style selection: a boundary, then its prefix newest first.
    for (int probe = 0; probe < 3 && !boundaries.empty(); ++probe) {
        const auto& selected = referenceBoundaries[random() % referenceBoundaries.size()];
        const auto found =
            std::ranges::lower_bound(boundaries, selected.SnapshotID, {}, &TSnapshotBoundary::SnapshotID);
        ASSERT_NE(found, boundaries.end());
        ASSERT_EQ(found->FileCount, selected.FileCount);
        for (auto next = found->FileCount; next > 0; --next) {
            ASSERT_EQ(files[next - 1], reference[next - 1]);
            ASSERT_EQ(files[next - 1].DataFile, reference[next - 1].DataFile);
        }
    }
    ASSERT_TRUE(std::ranges::equal(actual.Expired, expected.Expired));
    for (const auto id : expected.Expired) {
        ASSERT_TRUE(std::ranges::binary_search(actual.Expired, id));
    }
    const auto paths = actual.Manifests.Paths();
    ASSERT_EQ(paths.size(), expected.Manifests.size());
    for (std::size_t path = 0; path < paths.size(); ++path) {
        ASSERT_EQ(paths[path], expected.Manifests[path]);
    }
    for (int probe = 0; probe < 4 && !expected.Manifests.empty(); ++probe) {
        const auto& path = expected.Manifests[random() % expected.Manifests.size()];
        ASSERT_TRUE(actual.Manifests.Contains(path));
        ASSERT_FALSE(actual.Manifests.Contains(path + "x"));
        ASSERT_FALSE(actual.Manifests.Contains(path.substr(0, path.size() - 1)));
    }
    ASSERT_FALSE(actual.Manifests.Contains(""));
    // O(1) accounting equals its full walk and keeps every former term; it
    // adds the per-path node allowance, the larger membership object and
    // drops spare vector capacity the shared ID vector does not have.
    const auto bytes = Bytes(actual);
    ASSERT_EQ(bytes, FullWalkBytes(actual));
    ASSERT_EQ(actual.Expired.capacity(), actual.Expired.size());
    ASSERT_EQ(bytes + (expected.Expired.capacity() - expected.Expired.size()) * sizeof(std::int64_t),
        LegacyBytes(expected) + (sizeof(TScanMembership) - sizeof(TLegacyMembership)) +
            expected.Manifests.size() * (ManifestPathBytes - 2 * sizeof(std::string)));
}

struct TChain
{
    TGeneration Current;
    TLegacyGeneration Reference;
    std::vector<std::int64_t> Chronological;
};

struct TFixture
{
    std::mt19937_64 Random{0x5CA1AB1E};
    std::uint64_t Serial = 0;
    std::set<std::int64_t> UsedIDs;

    std::int64_t NewSnapshotID()
    {
        while (true) {
            const auto id = static_cast<std::int64_t>(Random() >> 1);
            if (UsedIDs.insert(id).second) {
                return id;
            }
        }
    }

    TAppend RandomAppend(const TLegacyGeneration& base, std::size_t files)
    {
        TAppend append;
        for (std::size_t file = 0; file < files; ++file) {
            append.Files.push_back(Descriptor(Serial++));
        }
        append.SnapshotID = NewSnapshotID();
        const auto manifests = 1 + Random() % 3;
        for (std::size_t manifest = 0; manifest < manifests; ++manifest) {
            // Occasionally repeat a retained path: the set keeps duplicates as the vector did.
            if (!base.Manifests.empty() && Random() % 16 == 0) {
                append.Manifests.push_back(base.Manifests[Random() % base.Manifests.size()]);
            } else {
                append.Manifests.push_back(ManifestPath(Serial++));
            }
        }
        return append;
    }
};

TGeneration BuildHistory(std::size_t files, TLegacyGeneration* reference = nullptr)
{
    TFixture fixture;
    TGeneration generation;
    TLegacyGeneration legacy;
    for (std::size_t file = 0; file < files; ++file) {
        auto append = fixture.RandomAppend(legacy, 1);
        append.Manifests.resize(1);
        if (reference) {
            legacy = LegacyAppend(legacy, append);
        }
        generation = Append(generation, std::move(append));
    }
    if (reference) {
        *reference = std::move(legacy);
    }
    return generation;
}

TEST(ScanIndexSharingTest, MatchesTheCopyingReferenceForRandomizedGenerations)
{
    TFixture fixture;
    std::array<TChain, 3> chains;
    std::vector<std::pair<TGeneration, TLegacyGeneration>> pinned;
    for (int step = 0; step < 600; ++step) {
        auto& chain = chains[fixture.Random() % chains.size()];
        const auto action = fixture.Random() % 100;
        if (action < 55 || action >= 90) {
            // Ordinary or bulk append; bulk sizes cross leaf boundaries.
            const auto bulk = action >= 90;
            const auto large = fixture.Random() % 4 == 0;
            const auto files = bulk ? 2 + fixture.Random() % (large ? 255 : 40) : 1;
            auto append = fixture.RandomAppend(chain.Reference, files);
            chain.Reference = LegacyAppend(chain.Reference, append);
            chain.Chronological.push_back(append.SnapshotID);
            chain.Current = Append(chain.Current, std::move(append));
        } else if (action < 70) {
            // A candidate that is never published: the base keeps its exact content.
            const auto before = Bytes(chain.Current);
            const auto files = chain.Current.Index->Files.size();
            {
                auto append = fixture.RandomAppend(chain.Reference, 1 + fixture.Random() % 70);
                const auto candidate = Append(chain.Current, std::move(append));
                ASSERT_GT(candidate.Index->Files.size(), files);
            }
            ASSERT_EQ(chain.Current.Index->Files.size(), files);
            ASSERT_EQ(Bytes(chain.Current), before);
        } else if (action < 75) {
            if (pinned.size() < 16) {
                pinned.emplace_back(chain.Current, chain.Reference);
            }
        } else if (action < 80 && !chain.Reference.Index->Files.empty()) {
            // KV copy-on-write closes file lifetimes in a new generation.
            const auto offset = fixture.Random() % chain.Reference.Index->Files.size();
            const auto sequence = static_cast<std::int64_t>(fixture.Serial++);
            auto copied = std::make_shared<TLegacyMembership>(*chain.Reference.Index);
            copied->Files[offset].RemovedSequence = sequence;
            chain.Reference.Index = std::move(copied);
            auto shared = std::make_shared<TScanMembership>(*chain.Current.Index);
            shared->Files.Update(offset, [&](TScanDataFile& file) {
                file.RemovedSequence = sequence;
            });
            chain.Current.Index = std::move(shared);
        } else if (chain.Chronological.size() > 1) {
            // Expiration drops the oldest boundaries and replaces the retained paths.
            const auto count = 1 + fixture.Random() % (chain.Chronological.size() - 1);
            std::vector<std::int64_t> prefix(chain.Chronological.begin(),
                chain.Chronological.begin() + static_cast<std::ptrdiff_t>(count));
            chain.Chronological.erase(chain.Chronological.begin(),
                chain.Chronological.begin() + static_cast<std::ptrdiff_t>(count));
            std::set<std::string> retained;
            for (const auto& path : chain.Reference.Manifests) {
                if (fixture.Random() % 3 != 0) {
                    retained.insert(path);
                }
            }
            chain.Reference =
                LegacyExpire(chain.Reference, prefix, std::vector<std::string>(retained.begin(), retained.end()));
            chain.Current = Expire(chain.Current, prefix, std::vector<std::string>(retained.begin(), retained.end()));
        }
        ExpectSame(chain.Current, chain.Reference, fixture.Random);
        if (HasFatalFailure()) {
            FAIL() << "step " << step;
        }
    }
    ASSERT_FALSE(pinned.empty());
    for (const auto& [generation, reference] : pinned) {
        ExpectSame(generation, reference, fixture.Random);
    }
    for (const auto& chain : chains) {
        ExpectSame(chain.Current, chain.Reference, fixture.Random);
    }
}

struct TAppendCost
{
    std::size_t SharedDescriptors = 0;
    std::size_t NewDescriptors = 0;
    std::size_t SharedPaths = 0;
    std::uint64_t IndexAllocations = 0;
    std::uint64_t PathAllocations = 0;
    std::uint64_t AccountingAllocations = 0;
    std::uint64_t LegacyAllocations = 0;
};

TAppendCost MeasureAppend(std::size_t history)
{
    TLegacyGeneration legacy;
    const auto base = BuildHistory(history, &legacy);
    TFixture fixture;
    fixture.Serial = 1ULL << 40;
    auto append = fixture.RandomAppend(legacy, 1);
    append.Manifests.resize(1);
    const auto legacyAppend = append;
    TAppendCost cost;
    // The membership step of CheckAppendClosure: share, push one file, add a boundary.
    std::shared_ptr<TScanMembership> index;
    {
        TAllocationCount count;
        index = std::make_shared<TScanMembership>(*base.Index);
        index->Files.push_back(std::move(append.Files.front()));
        const auto boundary =
            std::ranges::lower_bound(index->Snapshots, append.SnapshotID, {}, &TSnapshotBoundary::SnapshotID);
        index->Snapshots.insert(boundary, {append.SnapshotID, index->Files.size()});
        cost.IndexAllocations = count.Stop();
    }
    TGeneration next{index, base.Expired, base.Manifests};
    {
        TAllocationCount count;
        next.Manifests.Insert(std::move(append.Manifests.front()));
        cost.PathAllocations = count.Stop();
    }
    {
        TAllocationCount count;
        EXPECT_EQ(Bytes(next), FullWalkBytes(next));
        cost.AccountingAllocations = count.Stop();
    }
    for (std::size_t file = 0; file < history; ++file) {
        cost.SharedDescriptors += &next.Index->Files[file] == &base.Index->Files[file] ? 1 : 0;
    }
    std::set<const TScanDataFile*> before;
    for (const auto& file : base.Index->Files) {
        before.insert(&file);
    }
    for (const auto& file : next.Index->Files) {
        cost.NewDescriptors += before.contains(&file) ? 0 : 1;
    }
    std::set<const char*> basePaths;
    base.Manifests.ForEach([&](const std::string& path) {
        basePaths.insert(path.data());
    });
    next.Manifests.ForEach([&](const std::string& path) {
        cost.SharedPaths += basePaths.contains(path.data()) ? 1 : 0;
    });
    {
        TAllocationCount count;
        const auto copied = LegacyAppend(legacy, legacyAppend);
        EXPECT_GT(LegacyBytes(copied), 0U);
        cost.LegacyAllocations = count.Stop();
    }
    return cost;
}

TEST(ScanIndexSharingTest, AppendCopiesNoDescriptorAndAllocatesTheSameAtTwoHistorySizes)
{
    // 100 and 5,000 leave the same partial 32-file leaf phase (4 and 8 files),
    // so neither append completes a leaf.
    const auto small = MeasureAppend(100);
    const auto large = MeasureAppend(5000);
    RecordProperty("index_allocations_100", static_cast<int>(small.IndexAllocations));
    RecordProperty("index_allocations_5000", static_cast<int>(large.IndexAllocations));
    RecordProperty("path_allocations_100", static_cast<int>(small.PathAllocations));
    RecordProperty("path_allocations_5000", static_cast<int>(large.PathAllocations));
    RecordProperty("legacy_allocations_100", static_cast<int>(small.LegacyAllocations));
    RecordProperty("legacy_allocations_5000", static_cast<int>(large.LegacyAllocations));
    // Zero descriptor or path copies; one new descriptor.
    EXPECT_EQ(small.SharedDescriptors, 100U);
    EXPECT_EQ(large.SharedDescriptors, 5000U);
    EXPECT_EQ(small.NewDescriptors, 1U);
    EXPECT_EQ(large.NewDescriptors, 1U);
    EXPECT_EQ(small.SharedPaths, 100U);
    EXPECT_EQ(large.SharedPaths, 5000U);
    // The membership step allocates the same at both sizes: the membership
    // object, its boundary vector, one descriptor and one new tail leaf.
    EXPECT_EQ(small.IndexAllocations, large.IndexAllocations);
    EXPECT_LE(large.IndexAllocations, 6U);
    // Accounting reads incremental totals only.
    EXPECT_EQ(small.AccountingAllocations, 0U);
    EXPECT_EQ(large.AccountingAllocations, 0U);
    // The path set copies one root-to-leaf path: O(log n) nodes, about 3
    // allocations per level (100 paths: 2 levels; 5,000 paths: 3 levels).
    EXPECT_LE(small.PathAllocations, 16U);
    EXPECT_LE(large.PathAllocations, 20U);
    // The copying reference allocated at least one string per retained file and path.
    EXPECT_GE(small.LegacyAllocations, 200U);
    EXPECT_GE(large.LegacyAllocations, 10000U);
    std::cout << "membership allocations per append: " << small.IndexAllocations << " / " << large.IndexAllocations
              << "; path allocations: " << small.PathAllocations << " / " << large.PathAllocations
              << "; copying reference allocations: " << small.LegacyAllocations << " / " << large.LegacyAllocations
              << '\n';
}

TEST(ScanIndexSharingTest, AmortizedMembershipAllocationsPerAppendAreBoundedByTrieDepth)
{
    const auto measure = [](std::size_t history) {
        auto generation = BuildHistory(history);
        TFixture fixture;
        fixture.Serial = 1ULL << 40;
        std::vector<TAppend> appends;
        for (int append = 0; append < 64; ++append) {
            appends.push_back(fixture.RandomAppend({}, 1));
            appends.back().Manifests.clear();
        }
        TAllocationCount count;
        for (auto& append : appends) {
            generation = Append(generation, std::move(append));
        }
        return count.Stop();
    };
    const auto small = measure(96);
    const auto large = measure(4992);
    // Two leaves complete in 64 appends; each copies one branch per trie level
    // (one level below 1,024 files, two below 32,768): 2 extra allocations each.
    EXPECT_EQ(large, small + 4U);
    std::cout << "membership allocations per 64 appends: " << small << " / " << large << '\n';
}

TEST(ScanIndexSharingTest, PinnedGenerationKeepsExactlyItsOwnFilesAfterManyLaterAppends)
{
    TLegacyGeneration reference;
    const auto pinned = BuildHistory(100, &reference);
    std::vector<const TScanDataFile*> addresses;
    for (const auto& file : pinned.Index->Files) {
        addresses.push_back(&file);
    }
    const auto bytes = Bytes(pinned);
    TFixture fixture;
    fixture.Serial = 1ULL << 40;
    auto current = pinned;
    for (int append = 0; append < 5000; ++append) {
        current = Append(current, fixture.RandomAppend({}, append % 50 == 0 ? 40 : 1));
    }
    ASSERT_EQ(current.Index->Files.size(), 100U + 4900U + 100U * 40U);
    std::mt19937_64 random(7);
    ExpectSame(pinned, reference, random);
    ASSERT_EQ(pinned.Index->Files.size(), addresses.size());
    for (std::size_t file = 0; file < addresses.size(); ++file) {
        EXPECT_EQ(&pinned.Index->Files[file], addresses[file]);
        EXPECT_EQ(&current.Index->Files[file], addresses[file]);
    }
    EXPECT_EQ(Bytes(pinned), bytes);
    EXPECT_EQ(Bytes(current), FullWalkBytes(current));
}

TEST(ScanIndexSharingTest, DiscardedCandidatesOfEveryShapeLeaveTheBaseUnchanged)
{
    TLegacyGeneration reference;
    // 1,000 files leave an 8-file tail.
    const auto base = BuildHistory(1000, &reference);
    std::vector<const TScanDataFile*> addresses;
    for (const auto& file : base.Index->Files) {
        addresses.push_back(&file);
    }
    const auto paths = base.Manifests.Paths();
    const auto bytes = Bytes(base);
    TFixture fixture;
    fixture.Serial = 1ULL << 40;
    for (const std::size_t files : std::array<std::size_t, 5>{1, 24, 25, 40, 256}) {
        {
            auto append = fixture.RandomAppend(reference, files);
            const auto expected = LegacyAppend(reference, append);
            const auto candidate = Append(base, std::move(append));
            std::mt19937_64 random(files);
            ExpectSame(candidate, expected, random);
        }
        std::mt19937_64 random(files);
        ExpectSame(base, reference, random);
        for (std::size_t file = 0; file < addresses.size(); ++file) {
            ASSERT_EQ(&base.Index->Files[file], addresses[file]);
        }
        ASSERT_EQ(base.Manifests.Paths(), paths);
        ASSERT_EQ(Bytes(base), bytes);
    }
}

TEST(ScanIndexSharingTest, EmptyAndSmallStructuresBehaveLikeVectors)
{
    TScanFiles files;
    EXPECT_TRUE(files.empty());
    EXPECT_EQ(files.begin(), files.end());
    EXPECT_EQ(files.DescriptorBytes(), 0U);
    TScanFiles other;
    EXPECT_EQ(files, other);
    std::vector<TScanDataFile> expected;
    for (std::uint64_t serial = 0; serial < 1100; ++serial) {
        auto descriptor = Descriptor(serial);
        expected.push_back(descriptor);
        files.push_back(std::move(descriptor));
        ASSERT_EQ(files.size(), expected.size());
        ASSERT_EQ(files[serial], expected[serial]);
        ASSERT_EQ(files[serial / 2], expected[serial / 2]);
    }
    EXPECT_TRUE(std::ranges::equal(files, expected));
    EXPECT_NE(files, other);
    other = files;
    EXPECT_EQ(files, other);
    other.push_back(Descriptor(5000));
    EXPECT_NE(files, other);
    EXPECT_EQ(files.size(), 1100U);

    TSortedSnapshotIDs ids;
    EXPECT_TRUE(ids.empty());
    EXPECT_FALSE(std::ranges::binary_search(ids, 1));
    ids = TSortedSnapshotIDs({5, -3, 9});
    EXPECT_TRUE(std::ranges::equal(ids, std::vector<std::int64_t>{-3, 5, 9}));
    EXPECT_TRUE(std::ranges::binary_search(ids, 5));
    EXPECT_EQ(ids.capacity(), 3U);

    TManifestPathSet paths;
    EXPECT_FALSE(paths.Contains("a"));
    EXPECT_TRUE(paths.Paths().empty());
    paths = std::vector<std::string>{"c", "a", "b", "a"};
    EXPECT_EQ(paths.size(), 4U);
    EXPECT_EQ(paths.Paths(), (std::vector<std::string_view>{"a", "a", "b", "c"}));
    EXPECT_EQ(paths.StringBytes(), 4U * (std::string("a").capacity() + 1));
    paths.Insert("bb");
    paths.Insert("0");
    EXPECT_EQ(paths.Paths(), (std::vector<std::string_view>{"0", "a", "a", "b", "bb", "c"}));
    EXPECT_TRUE(paths.Contains("bb"));
    EXPECT_FALSE(paths.Contains("ba"));
    TManifestPathSet built;
    built = std::vector<std::string>{"0", "a", "a", "b", "bb", "c"};
    EXPECT_EQ(paths, built);
    std::vector<std::string> many;
    TManifestPathSet inserted;
    for (std::uint64_t serial = 0; serial < 3000; ++serial) {
        many.push_back(ManifestPath(serial % 2000));
        inserted.Insert(many.back());
    }
    TManifestPathSet bulk;
    bulk = many;
    std::ranges::sort(many);
    EXPECT_EQ(inserted, bulk);
    const auto listed = inserted.Paths();
    ASSERT_EQ(listed.size(), many.size());
    for (std::size_t path = 0; path < many.size(); ++path) {
        ASSERT_EQ(listed[path], many[path]);
        ASSERT_TRUE(bulk.Contains(many[path]));
        ASSERT_TRUE(inserted.Contains(many[path]));
    }
}

TEST(ScanIndexSharingTest, UpdatedLifetimeCopiesOnlyThatDescriptorAndKeepsTheBase)
{
    TLegacyGeneration reference;
    const auto base = BuildHistory(1000, &reference);
    for (const std::size_t offset : std::array<std::size_t, 4>{0, 517, 991, 999}) {
        auto changed = std::make_shared<TScanMembership>(*base.Index);
        changed->Files.Update(offset, [](TScanDataFile& file) {
            file.RemovedSequence = 42;
        });
        ASSERT_EQ(changed->Files.size(), base.Index->Files.size());
        EXPECT_EQ(changed->Files[offset].RemovedSequence, 42);
        EXPECT_FALSE(base.Index->Files[offset].RemovedSequence);
        EXPECT_NE(&changed->Files[offset], &base.Index->Files[offset]);
        std::size_t shared = 0;
        for (std::size_t file = 0; file < base.Index->Files.size(); ++file) {
            shared += &changed->Files[file] == &base.Index->Files[file] ? 1 : 0;
        }
        EXPECT_EQ(shared, base.Index->Files.size() - 1);
        EXPECT_EQ(changed->Files.DescriptorBytes(), base.Index->Files.DescriptorBytes());
        std::mt19937_64 random(offset);
        ExpectSame(base, reference, random);
    }
}

TEST(ScanIndexSharingTest, ReadersOfPublishedGenerationsRaceWithLaterAppends)
{
    struct TPublished
    {
        TGeneration Generation;
        std::int64_t Records = 0;
    };
    std::mutex mutex;
    auto published = std::make_shared<const TPublished>(TPublished{BuildHistory(500), 0});
    {
        std::int64_t records = 0;
        for (const auto& file : published->Generation.Index->Files) {
            records += file.RecordCount;
        }
        published = std::make_shared<const TPublished>(TPublished{published->Generation, records});
    }
    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> checked{0};
    std::vector<std::thread> readers;
    readers.reserve(3);
    for (int reader = 0; reader < 3; ++reader) {
        readers.emplace_back([&] {
            while (!stop.load()) {
                std::shared_ptr<const TPublished> pinned;
                {
                    std::lock_guard lock(mutex);
                    pinned = published;
                }
                std::int64_t records = 0;
                for (const auto& file : pinned->Generation.Index->Files) {
                    records += file.RecordCount;
                }
                EXPECT_EQ(records, pinned->Records);
                EXPECT_EQ(Bytes(pinned->Generation), FullWalkBytes(pinned->Generation));
                ++checked;
            }
        });
    }
    TFixture fixture;
    fixture.Serial = 1ULL << 40;
    auto current = published;
    for (int append = 0; append < 1500; ++append) {
        auto next = fixture.RandomAppend({}, append % 100 == 0 ? 64 : 1);
        std::int64_t added = 0;
        for (const auto& file : next.Files) {
            added += file.RecordCount;
        }
        current = std::make_shared<const TPublished>(
            TPublished{Append(current->Generation, std::move(next)), current->Records + added});
        std::lock_guard lock(mutex);
        published = current;
    }
    stop = true;
    for (auto& reader : readers) {
        reader.join();
    }
    EXPECT_GT(checked.load(), 0U);
}

} // namespace
} // namespace NGlamDB::NStorage
