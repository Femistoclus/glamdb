#include "core/storage/scan_index.h"

#include <iceberg/manifest/manifest_entry.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace NGlamDB::NStorage {
namespace {

constexpr unsigned Bits = 5;
constexpr std::size_t Width = std::size_t{1} << Bits;
constexpr std::size_t Mask = Width - 1;
// A path-set node splits above this many entries into two halves.
constexpr std::size_t MaxPaths = 32;

std::uint64_t Add(std::uint64_t lhs, std::uint64_t rhs) noexcept
{
    if (lhs > std::numeric_limits<std::uint64_t>::max() - rhs) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return lhs + rhs;
}

std::uint64_t Mul(std::uint64_t count, std::uint64_t width) noexcept
{
    return width != 0 && count > std::numeric_limits<std::uint64_t>::max() / width
        ? std::numeric_limits<std::uint64_t>::max()
        : count * width;
}

std::uint64_t ScanDataFileBytes(const TScanDataFile& file) noexcept
{
    return Add(file.Path.capacity() + 1, file.DataFile ? DataFileMemory(*file.DataFile) : 0);
}

template <typename T>
std::vector<T> CopyWithRoom(const std::vector<T>& values, std::size_t extra)
{
    std::vector<T> copy;
    copy.reserve(values.size() + extra);
    copy.insert(copy.end(), values.begin(), values.end());
    return copy;
}

} // namespace

std::uint64_t DataFileMemory(const iceberg::DataFile& file) noexcept
{
    // Conservative map-node/control-block allowance for the pinned standard
    // library, plus the actual capacities of every retained variable buffer.
    std::uint64_t bytes = sizeof(file) + 256;
    bytes = Add(bytes, file.file_path.capacity() + 1);
    for (const auto count : {file.column_sizes.size(),
             file.value_counts.size(),
             file.null_value_counts.size(),
             file.nan_value_counts.size()})
    {
        bytes = Add(bytes, Mul(count, 96));
    }
    for (const auto* bounds : {&file.lower_bounds, &file.upper_bounds}) {
        bytes = Add(bytes, Mul(bounds->size(), 96));
        for (const auto& [field, value] : *bounds) {
            (void)field;
            bytes = Add(bytes, value.capacity());
        }
    }
    bytes = Add(bytes, file.key_metadata.capacity());
    bytes = Add(bytes, Mul(file.split_offsets.capacity(), sizeof(std::int64_t)));
    bytes = Add(bytes, Mul(file.equality_ids.capacity(), sizeof(std::int32_t)));
    if (file.referenced_data_file) {
        bytes = Add(bytes, file.referenced_data_file->capacity() + 1);
    }
    return bytes;
}

// A branch holds up to 32 children; a leaf holds up to 32 descriptors.
struct TScanFiles::TNode
{
    std::vector<std::shared_ptr<const TNode>> Children;
    std::vector<std::shared_ptr<const TScanDataFile>> Files;
};

namespace {

using TFileNode = std::shared_ptr<const TScanFiles::TNode>;

TFileNode MakeBranch(std::vector<TFileNode> children)
{
    return std::make_shared<const TScanFiles::TNode>(TScanFiles::TNode{std::move(children), {}});
}

TFileNode NewPath(unsigned shift, const TFileNode& leaf)
{
    if (shift == 0) {
        return leaf;
    }
    std::vector<TFileNode> children;
    children.reserve(1);
    children.push_back(NewPath(shift - Bits, leaf));
    return MakeBranch(std::move(children));
}

// Returns a copy of `branch` (at `shift`) whose rightmost path also holds
// `leaf`, the full leaf that starts at element `trieSize`. Only the copied
// path is new; every other node is shared with `branch`.
TFileNode PushLeaf(const TScanFiles::TNode& branch, unsigned shift, std::size_t trieSize, const TFileNode& leaf)
{
    const auto index = (trieSize >> shift) & Mask;
    const bool append = shift == Bits || index >= branch.Children.size();
    auto children = CopyWithRoom(branch.Children, append ? 1 : 0);
    if (shift == Bits) {
        children.push_back(leaf);
    } else if (!append) {
        children[index] = PushLeaf(*branch.Children[index], shift - Bits, trieSize, leaf);
    } else {
        children.push_back(NewPath(shift - Bits, leaf));
    }
    return MakeBranch(std::move(children));
}

// Returns a copy of `node` (at `shift`) whose element `index` is `item`.
TFileNode ReplaceItem(const TScanFiles::TNode& node,
    unsigned shift,
    std::size_t index,
    const std::shared_ptr<const TScanDataFile>& item)
{
    if (shift == 0) {
        auto files = CopyWithRoom(node.Files, 0);
        files[index & Mask] = item;
        return std::make_shared<const TScanFiles::TNode>(TScanFiles::TNode{{}, std::move(files)});
    }
    auto children = CopyWithRoom(node.Children, 0);
    auto& child = children[(index >> shift) & Mask];
    child = ReplaceItem(*child, shift - Bits, index, item);
    return MakeBranch(std::move(children));
}

} // namespace

TScanFiles::const_iterator::const_iterator(const TScanFiles* files, std::size_t index)
    : Files_(files)
    , Index_(index)
    , Leaf_(index < files->Size_ ? &files->LeafFor(index) : nullptr)
{ }

TScanFiles::const_iterator::reference TScanFiles::const_iterator::operator*() const
{
    return *Leaf_->Files[Index_ & Mask];
}

TScanFiles::const_iterator& TScanFiles::const_iterator::operator++()
{
    ++Index_;
    if ((Index_ & Mask) == 0) {
        Leaf_ = Index_ < Files_->Size_ ? &Files_->LeafFor(Index_) : nullptr;
    }
    return *this;
}

TScanFiles::const_iterator TScanFiles::const_iterator::operator++(int)
{
    auto copy = *this;
    ++*this;
    return copy;
}

TScanFiles::const_iterator TScanFiles::begin() const
{
    return {this, 0};
}

TScanFiles::const_iterator TScanFiles::end() const
{
    return {this, Size_};
}

std::size_t TScanFiles::TailOffset() const noexcept
{
    return Size_ == 0 ? 0 : (Size_ - 1) & ~Mask;
}

const TScanFiles::TNode& TScanFiles::LeafFor(std::size_t index) const
{
    if (index >= TailOffset()) {
        return *Tail_;
    }
    const TNode* node = Root_.get();
    for (auto shift = Shift_; shift > 0; shift -= Bits) {
        node = node->Children[(index >> shift) & Mask].get();
    }
    return *node;
}

const TScanDataFile& TScanFiles::operator[](std::size_t index) const
{
    return *LeafFor(index).Files[index & Mask];
}

void TScanFiles::push_back(TScanDataFile file)
{
    // Allocate everything before changing this value: a failed push leaves it intact.
    auto item = std::make_shared<const TScanDataFile>(std::move(file));
    const auto bytes = Add(DescriptorBytes_, ScanDataFileBytes(*item));
    auto root = Root_;
    auto shift = Shift_;
    std::vector<std::shared_ptr<const TScanDataFile>> tail;
    if (Tail_ && Tail_->Files.size() < Width) {
        tail = CopyWithRoom(Tail_->Files, 1);
    } else if (Tail_) {
        // The full tail joins the trie at the element where it starts.
        const auto trieSize = Size_ - Width;
        if (!root) {
            root = MakeBranch({Tail_});
            shift = Bits;
        } else if ((trieSize >> Bits) >= (std::size_t{1} << shift)) {
            root = MakeBranch({root, NewPath(shift, Tail_)});
            shift += Bits;
        } else {
            root = PushLeaf(*root, shift, trieSize, Tail_);
        }
        tail.reserve(1);
    } else {
        tail.reserve(1);
    }
    tail.push_back(std::move(item));
    auto leaf = std::make_shared<const TNode>(TNode{{}, std::move(tail)});
    Root_ = std::move(root);
    Shift_ = shift;
    Tail_ = std::move(leaf);
    ++Size_;
    DescriptorBytes_ = bytes;
}

void TScanFiles::Update(std::size_t index, const std::function<void(TScanDataFile&)>& change)
{
    const auto& previous = (*this)[index];
    auto changed = std::make_shared<TScanDataFile>(previous);
    change(*changed);
    std::shared_ptr<const TScanDataFile> item = std::move(changed);
    const auto previousBytes = ScanDataFileBytes(previous);
    const auto bytes = DescriptorBytes_ == std::numeric_limits<std::uint64_t>::max()
        ? DescriptorBytes_
        : Add(DescriptorBytes_ - previousBytes, ScanDataFileBytes(*item));
    if (index >= TailOffset()) {
        auto tail = ReplaceItem(*Tail_, 0, index, item);
        Tail_ = std::move(tail);
    } else {
        auto root = ReplaceItem(*Root_, Shift_, index, item);
        Root_ = std::move(root);
    }
    DescriptorBytes_ = bytes;
}

bool TScanFiles::operator==(const TScanFiles& other) const
{
    if (Size_ != other.Size_) {
        return false;
    }
    // Equal sizes give equal leaf boundaries; shared descriptors compare by identity first.
    for (std::size_t start = 0; start < Size_; start += Width) {
        const auto& lhs = LeafFor(start).Files;
        const auto& rhs = other.LeafFor(start).Files;
        if (!std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), [](const auto& left, const auto& right) {
                return left == right || *left == *right;
            }))
        {
            return false;
        }
    }
    return true;
}

TSortedSnapshotIDs::TSortedSnapshotIDs(std::vector<std::int64_t> ids)
{
    std::ranges::sort(ids);
    ids.shrink_to_fit();
    IDs_ = std::make_shared<const std::vector<std::int64_t>>(std::move(ids));
}

const std::vector<std::int64_t>& TSortedSnapshotIDs::Values() const noexcept
{
    static const std::vector<std::int64_t> empty;
    return IDs_ ? *IDs_ : empty;
}

bool TSortedSnapshotIDs::operator==(const TSortedSnapshotIDs& other) const
{
    return Values() == other.Values();
}

// Leaves hold sorted paths; branches hold the first path of each child.
struct TManifestPathSet::TNode
{
    std::vector<std::shared_ptr<const std::string>> Paths;
    std::vector<std::shared_ptr<const TNode>> Children;
};

namespace {

using TPathNode = std::shared_ptr<const TManifestPathSet::TNode>;
using TPath = std::shared_ptr<const std::string>;

struct TPathLess
{
    bool operator()(const TPath& lhs, const TPath& rhs) const noexcept
    {
        return *lhs < *rhs;
    }
    bool operator()(std::string_view lhs, const TPath& rhs) const noexcept
    {
        return lhs < *rhs;
    }
    bool operator()(const TPath& lhs, std::string_view rhs) const noexcept
    {
        return *lhs < rhs;
    }
};

struct TPathInsert
{
    TPathNode Left;
    TPathNode Right;
};

TPathNode MakePathNode(std::vector<TPath> paths, std::vector<TPathNode> children)
{
    return std::make_shared<const TManifestPathSet::TNode>(
        TManifestPathSet::TNode{std::move(paths), std::move(children)});
}

template <typename T>
std::vector<T> Slice(const std::vector<T>& values, std::size_t begin, std::size_t end)
{
    return std::vector<T>(values.begin() + static_cast<std::ptrdiff_t>(begin),
        values.begin() + static_cast<std::ptrdiff_t>(end));
}

TPathInsert SplitPathNode(std::vector<TPath> paths, std::vector<TPathNode> children)
{
    if (paths.size() <= MaxPaths) {
        return {MakePathNode(std::move(paths), std::move(children)), nullptr};
    }
    const auto half = paths.size() / 2;
    const bool branch = !children.empty();
    return {MakePathNode(Slice(paths, 0, half), branch ? Slice(children, 0, half) : std::vector<TPathNode>{}),
        MakePathNode(Slice(paths, half, paths.size()),
            branch ? Slice(children, half, children.size()) : std::vector<TPathNode>{})};
}

// Equal paths go after existing ones, so duplicates keep a sorted multiset.
TPathInsert InsertPath(const TManifestPathSet::TNode& node, const TPath& path)
{
    const auto position = std::upper_bound(node.Paths.begin(), node.Paths.end(), path, TPathLess{});
    const auto offset = static_cast<std::size_t>(position - node.Paths.begin());
    if (node.Children.empty()) {
        auto paths = CopyWithRoom(node.Paths, 1);
        paths.insert(paths.begin() + static_cast<std::ptrdiff_t>(offset), path);
        return SplitPathNode(std::move(paths), {});
    }
    const auto child = offset == 0 ? 0 : offset - 1;
    auto inserted = InsertPath(*node.Children[child], path);
    const auto extra = inserted.Right ? 1 : 0;
    auto paths = CopyWithRoom(node.Paths, extra);
    auto children = CopyWithRoom(node.Children, extra);
    paths[child] = inserted.Left->Paths.front();
    children[child] = std::move(inserted.Left);
    if (inserted.Right) {
        const auto next = static_cast<std::ptrdiff_t>(child + 1);
        paths.insert(paths.begin() + next, inserted.Right->Paths.front());
        children.insert(children.begin() + next, std::move(inserted.Right));
    }
    return SplitPathNode(std::move(paths), std::move(children));
}

void VisitPaths(const TManifestPathSet::TNode& node, const std::function<void(const std::string&)>& visit)
{
    if (node.Children.empty()) {
        for (const auto& path : node.Paths) {
            visit(*path);
        }
        return;
    }
    for (const auto& child : node.Children) {
        VisitPaths(*child, visit);
    }
}

} // namespace

TManifestPathSet& TManifestPathSet::operator=(std::vector<std::string> paths)
{
    std::ranges::sort(paths);
    std::uint64_t bytes = 0;
    std::vector<TPathNode> level;
    level.reserve((paths.size() + MaxPaths - 1) / MaxPaths);
    for (std::size_t begin = 0; begin < paths.size(); begin += MaxPaths) {
        const auto end = std::min(paths.size(), begin + MaxPaths);
        std::vector<TPath> leaf;
        leaf.reserve(end - begin);
        for (auto index = begin; index < end; ++index) {
            leaf.push_back(std::make_shared<const std::string>(std::move(paths[index])));
            bytes = Add(bytes, leaf.back()->capacity() + 1);
        }
        level.push_back(MakePathNode(std::move(leaf), {}));
    }
    while (level.size() > 1) {
        std::vector<TPathNode> parents;
        parents.reserve((level.size() + MaxPaths - 1) / MaxPaths);
        for (std::size_t begin = 0; begin < level.size(); begin += MaxPaths) {
            const auto end = std::min(level.size(), begin + MaxPaths);
            std::vector<TPath> firsts;
            firsts.reserve(end - begin);
            for (auto index = begin; index < end; ++index) {
                firsts.push_back(level[index]->Paths.front());
            }
            parents.push_back(MakePathNode(std::move(firsts), Slice(level, begin, end)));
        }
        level = std::move(parents);
    }
    Root_ = level.empty() ? nullptr : std::move(level.front());
    Size_ = paths.size();
    StringBytes_ = bytes;
    return *this;
}

void TManifestPathSet::Insert(std::string path)
{
    auto item = std::make_shared<const std::string>(std::move(path));
    const auto bytes = Add(StringBytes_, item->capacity() + 1);
    TPathNode root;
    if (!Root_) {
        root = MakePathNode({item}, {});
    } else {
        auto inserted = InsertPath(*Root_, item);
        if (inserted.Right) {
            std::vector<TPath> firsts{inserted.Left->Paths.front(), inserted.Right->Paths.front()};
            root = MakePathNode(std::move(firsts), {std::move(inserted.Left), std::move(inserted.Right)});
        } else {
            root = std::move(inserted.Left);
        }
    }
    Root_ = std::move(root);
    ++Size_;
    StringBytes_ = bytes;
}

bool TManifestPathSet::Contains(std::string_view path) const
{
    const TNode* node = Root_.get();
    if (!node) {
        return false;
    }
    while (!node->Children.empty()) {
        const auto position = std::upper_bound(node->Paths.begin(), node->Paths.end(), path, TPathLess{});
        if (position == node->Paths.begin()) {
            return false;
        }
        node = node->Children[static_cast<std::size_t>(position - node->Paths.begin()) - 1].get();
    }
    return std::binary_search(node->Paths.begin(), node->Paths.end(), path, TPathLess{});
}

void TManifestPathSet::ForEach(const std::function<void(const std::string&)>& visit) const
{
    if (Root_) {
        VisitPaths(*Root_, visit);
    }
}

std::vector<std::string_view> TManifestPathSet::Paths() const
{
    std::vector<std::string_view> paths;
    paths.reserve(Size_);
    ForEach([&](const std::string& path) {
        paths.push_back(path);
    });
    return paths;
}

bool TManifestPathSet::operator==(const TManifestPathSet& other) const
{
    return Size_ == other.Size_ && Paths() == other.Paths();
}

std::uint64_t ScanIndexBytes(const TScanMembership& index,
    const TSortedSnapshotIDs& expired,
    const TManifestPathSet& manifests) noexcept
{
    std::uint64_t bytes = sizeof(index);
    bytes = Add(bytes, Mul(expired.capacity(), sizeof(std::int64_t)));
    // One shared string object with its control block (within twice the
    // string size), its leaf slot and its share of separators and nodes.
    bytes = Add(bytes, Mul(manifests.size(), ManifestPathBytes));
    bytes = Add(bytes, manifests.StringBytes());
    // One 16-byte leaf slot plus one shared descriptor block (control block
    // and descriptor) per file stays within twice the descriptor size, the
    // same envelope the former contiguous vector charged for its growth.
    bytes = Add(bytes, Mul(index.Files.size(), 2 * sizeof(TScanDataFile)));
    bytes = Add(bytes, Mul(index.Snapshots.size(), 2 * sizeof(TSnapshotBoundary)));
    bytes = Add(bytes, index.Files.DescriptorBytes());
    return bytes;
}

} // namespace NGlamDB::NStorage
