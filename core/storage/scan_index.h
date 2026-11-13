#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace iceberg {
struct DataFile;
}

namespace NGlamDB::NStorage {

struct TScanDataFile
{
    std::string Path;
    std::int64_t SizeBytes = 0;
    std::int64_t RecordCount = 0;
    std::shared_ptr<const iceberg::DataFile> DataFile{};
    std::int64_t IntroducedSequence = 0;
    std::optional<std::int64_t> RemovedSequence{};

    bool operator==(const TScanDataFile& other) const
    {
        return Path == other.Path && SizeBytes == other.SizeBytes && RecordCount == other.RecordCount;
    }
};

struct TSnapshotBoundary
{
    std::int64_t SnapshotID;
    std::size_t FileCount;
    std::int64_t SequenceNumber = 0;
};

// Conservative bytes retained by one manifest DataFile descriptor: map-node and
// control-block allowances for the pinned standard library plus the actual
// capacities of every variable buffer. Saturates at UINT64_MAX.
std::uint64_t DataFileMemory(const iceberg::DataFile& file) noexcept;

// Chronological data-file descriptors of a verified generation, structurally
// shared between generations (a 32-way persistent vector). Each descriptor is
// allocated once and never copied: copying a TScanFiles shares every node, and
// push_back copies only the newest partial leaf (at most 31 handles) and, once
// per 32 files, one root-to-leaf path. Nodes reachable from another copy are
// never modified, so a published generation is immutable and readable from
// any thread while later generations are built.
class TScanFiles
{
public:
    // Implementation detail, defined in scan_index.cpp.
    struct TNode;

    class const_iterator
    {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = TScanDataFile;
        using difference_type = std::ptrdiff_t;
        using pointer = const TScanDataFile*;
        using reference = const TScanDataFile&;

        const_iterator() = default;
        reference operator*() const;
        pointer operator->() const
        {
            return &**this;
        }
        const_iterator& operator++();
        const_iterator operator++(int);
        bool operator==(const const_iterator& other) const noexcept
        {
            return Index_ == other.Index_;
        }

    private:
        friend class TScanFiles;
        const_iterator(const TScanFiles* files, std::size_t index);

        const TScanFiles* Files_ = nullptr;
        std::size_t Index_ = 0;
        const TNode* Leaf_ = nullptr;
    };
    using iterator = const_iterator;
    using value_type = TScanDataFile;
    using size_type = std::size_t;

    std::size_t size() const noexcept
    {
        return Size_;
    }
    bool empty() const noexcept
    {
        return Size_ == 0;
    }
    // O(log32 n). Requires index < size().
    const TScanDataFile& operator[](std::size_t index) const;
    // Require !empty().
    const TScanDataFile& front() const
    {
        return (*this)[0];
    }
    const TScanDataFile& back() const
    {
        return (*this)[Size_ - 1];
    }
    const_iterator begin() const;
    const_iterator end() const;
    // Strong exception guarantee.
    void push_back(TScanDataFile file);
    // Replaces descriptor `index` with a changed copy, copying only that
    // descriptor and its root-to-leaf path; references to the previous
    // descriptor stay valid while another generation shares it. Strong
    // exception guarantee.
    void Update(std::size_t index, const std::function<void(TScanDataFile&)>& change);
    // Path capacities plus DataFileMemory of every descriptor, kept incrementally.
    std::uint64_t DescriptorBytes() const noexcept
    {
        return DescriptorBytes_;
    }
    bool operator==(const TScanFiles& other) const;

private:
    std::size_t TailOffset() const noexcept;
    const TNode& LeafFor(std::size_t index) const;

    std::shared_ptr<const TNode> Root_;
    // The newest 1..32 descriptors (none only when empty).
    std::shared_ptr<const TNode> Tail_;
    std::size_t Size_ = 0;
    std::uint64_t DescriptorBytes_ = 0;
    unsigned Shift_ = 0;
};

// Built only by strict closure verification. Append snapshots select a
// chronological file prefix. KeyValue snapshots select file lifetimes by their
// sequence number. Boundaries are sorted by opaque snapshot ID for lookup only.
// Both profiles require one fixed-schema, unpartitioned, linear history; files
// have fresh paths and cannot be resurrected. Copying shares every file
// descriptor; boundaries are copied.
struct TScanMembership
{
    TScanFiles Files;
    std::vector<TSnapshotBoundary> Snapshots;
    bool KeyValue = false;
};

// Sorted snapshot IDs. Generations share one immutable vector; expiration and
// recovery build a new one.
class TSortedSnapshotIDs
{
public:
    using const_iterator = std::vector<std::int64_t>::const_iterator;
    using iterator = const_iterator;
    using value_type = std::int64_t;

    TSortedSnapshotIDs() = default;
    // Sorts `ids`. The shared vector keeps no spare capacity, so publication
    // and recovery charge the same bytes for the same IDs.
    explicit TSortedSnapshotIDs(std::vector<std::int64_t> ids);
    TSortedSnapshotIDs(std::initializer_list<std::int64_t> ids)
        : TSortedSnapshotIDs(std::vector<std::int64_t>(ids))
    { }

    const_iterator begin() const noexcept
    {
        return Values().begin();
    }
    const_iterator end() const noexcept
    {
        return Values().end();
    }
    std::size_t size() const noexcept
    {
        return Values().size();
    }
    bool empty() const noexcept
    {
        return Values().empty();
    }
    std::size_t capacity() const noexcept
    {
        return Values().capacity();
    }
    bool operator==(const TSortedSnapshotIDs& other) const;

private:
    const std::vector<std::int64_t>& Values() const noexcept;

    std::shared_ptr<const std::vector<std::int64_t>> IDs_;
};

// Sorted multiset of retained manifest paths, structurally shared between
// generations (a persistent B+-tree). Each path is allocated once; Insert
// copies one root-to-leaf path of at most 33 handles per level.
class TManifestPathSet
{
public:
    // Implementation detail, defined in scan_index.cpp.
    struct TNode;

    TManifestPathSet() = default;
    // Replaces the contents with `paths` (sorted here).
    TManifestPathSet& operator=(std::vector<std::string> paths);
    template <typename TIterator>
    void assign(TIterator first, TIterator last)
    {
        *this = std::vector<std::string>(first, last);
    }
    // Strong exception guarantee.
    void Insert(std::string path);
    // Removes one occurrence of `path`; false when absent. Copies one
    // root-to-leaf path and at most one sibling per level (an underfull node
    // merges with a neighbour), so O(log n). Strong exception guarantee.
    bool Erase(std::string_view path);
    bool Contains(std::string_view path) const;
    std::size_t size() const noexcept
    {
        return Size_;
    }
    bool empty() const noexcept
    {
        return Size_ == 0;
    }
    // Sum of capacity() + 1 over every path, kept incrementally.
    std::uint64_t StringBytes() const noexcept
    {
        return StringBytes_;
    }
    // In sorted order; the strings live as long as any generation sharing them.
    void ForEach(const std::function<void(const std::string&)>& visit) const;
    std::vector<std::string_view> Paths() const;
    bool operator==(const TManifestPathSet& other) const;

private:
    std::shared_ptr<const TNode> Root_;
    std::size_t Size_ = 0;
    std::uint64_t StringBytes_ = 0;
};

// Charged per retained manifest path in addition to its string capacity.
inline constexpr std::uint64_t ManifestPathBytes = 2 * sizeof(std::string) + 64;

// Conservative retained bytes of a generation's membership, expired IDs and
// retained manifest paths. O(1): every per-element term is kept incrementally.
// Saturates at UINT64_MAX.
std::uint64_t ScanIndexBytes(const TScanMembership& index,
    const TSortedSnapshotIDs& expired,
    const TManifestPathSet& manifests) noexcept;

} // namespace NGlamDB::NStorage
