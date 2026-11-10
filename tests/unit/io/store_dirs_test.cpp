#include "core/io/store_dirs.h"
#include "tests/support/durability_trace.h"
#include "tests/support/fault_fs.h"

#include <gtest/gtest.h>

#include <cstdlib>

namespace NGlamDB::NIO {
namespace {

using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;
using NGlamDB::NTesting::TIOEvent;

constexpr std::string_view CollectionId = "01234567-89ab-cdef-0123-456789abcdef";

class TStoreDirsTest : public ::testing::Test
{
protected:
    std::shared_ptr<TPosixLocalFS> Underlying_ = std::make_shared<TPosixLocalFS>();
    std::shared_ptr<TFaultFS> Fs_ = std::make_shared<TFaultFS>(*Underlying_);
    std::shared_ptr<TWriteLatch> Latch_ = std::make_shared<TWriteLatch>();
    std::filesystem::path Parent_;
    std::filesystem::path Path_;

    void SetUp() override
    {
        auto name = (std::filesystem::temp_directory_path() / "glamdb-dirs-XXXXXX").string();
        ASSERT_NE(::mkdtemp(name.data()), nullptr);
        Parent_ = std::filesystem::canonical(name);
        Path_ = Parent_ / "store";
    }
    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(Parent_, error);
        EXPECT_FALSE(error);
    }
    static std::vector<std::string> Paths(const std::vector<TIOEvent>& events, EIOOp op, std::size_t first = 0)
    {
        std::vector<std::string> result;
        for (std::size_t i = first; i < events.size(); ++i) {
            if (events[i].Op == op) {
                result.push_back(events[i].Path);
            }
        }
        return result;
    }
    std::vector<std::string> CollectionSyncs(const std::filesystem::path& root) const
    {
        const auto collection = root / "warehouse" / CollectionId;
        return {(collection / "data").string(),
            (collection / "metadata").string(),
            collection.string(),
            (root / "warehouse").string(),
            root.string()};
    }
};

TEST_F(TStoreDirsTest, CreatesAndSyncsStoreAndItsParentEvenWhenAlreadyPresent)
{
    for (int pass = 0; pass < 2; ++pass) {
        const auto first = Fs_->Events().size();
        auto result = TStoreDirs::Create(Fs_, Path_, Latch_);
        ASSERT_TRUE(result) << result.Error();
        const auto& store = result.Value();
        EXPECT_EQ(store->Fs(), Fs_);
        EXPECT_EQ(store->Latch(), Latch_);
        EXPECT_EQ(store->Path(), Path_);
        EXPECT_EQ(Fs_->Stat(store->Root()).Value().Type, EFileType::Directory);
        EXPECT_EQ(Fs_->Stat(store->Warehouse()).Value().Type, EFileType::Directory);
        EXPECT_FALSE(store->CheckIdentity());
        const std::vector<std::string> expected{(Path_ / "warehouse").string(), Path_.string(), Parent_.string()};
        EXPECT_EQ(Paths(Fs_->Events(), EIOOp::SyncDir, first), expected);
        const std::vector<std::string> created{Path_.string(), (Path_ / "warehouse").string()};
        EXPECT_EQ(Paths(Fs_->Events(), EIOOp::MakeDir, first), created);
    }
}

TEST_F(TStoreDirsTest, CreatesAndResyncsEveryCollectionAncestor)
{
    auto result = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(result);
    const auto& store = result.Value();
    for (int pass = 0; pass < 2; ++pass) {
        const auto first = Fs_->Events().size();
        auto collection = store->CreateCollection(CollectionId);
        ASSERT_TRUE(collection) << collection.Error();
        EXPECT_EQ(Fs_->Stat(collection.Value().Collection).Value().Type, EFileType::Directory);
        EXPECT_EQ(Fs_->Stat(collection.Value().Data).Value().Type, EFileType::Directory);
        EXPECT_EQ(Fs_->Stat(collection.Value().Metadata).Value().Type, EFileType::Directory);
        EXPECT_EQ(Paths(Fs_->Events(), EIOOp::SyncDir, first), CollectionSyncs(Path_));
        EXPECT_EQ(Paths(Fs_->Events(), EIOOp::MakeDir, first).size(), 3U);
    }
    NGlamDB::NTesting::TDurabilityTrace trace({Parent_.string()});
    for (const auto& event : Fs_->Events()) {
        EXPECT_FALSE(trace.Apply(event));
    }
}

TEST_F(TStoreDirsTest, OpeningStoreAndCollectionNeverCreatesOrSyncs)
{
    auto created = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(created);
    ASSERT_TRUE(created.Value()->CreateCollection(CollectionId));
    const auto first = Fs_->Events().size();
    Latch_->Block();
    auto opened = TStoreDirs::Open(Fs_, Path_, Latch_);
    ASSERT_TRUE(opened) << opened.Error();
    auto collection = opened.Value()->OpenCollection(CollectionId);
    ASSERT_TRUE(collection);
    EXPECT_EQ(Fs_->Stat(created.Value()->Root()).Value().Inode, Fs_->Stat(opened.Value()->Root()).Value().Inode);
    EXPECT_TRUE(Paths(Fs_->Events(), EIOOp::MakeDir, first).empty());
    EXPECT_TRUE(Paths(Fs_->Events(), EIOOp::SyncDir, first).empty());
}

TEST_F(TStoreDirsTest, OpeningMissingStateDoesNotCreateDirectories)
{
    EXPECT_EQ(TStoreDirs::Open(Fs_, Path_, Latch_).Error(), std::errc::no_such_file_or_directory);
    EXPECT_FALSE(std::filesystem::exists(Path_));
    ASSERT_TRUE(std::filesystem::create_directory(Path_));
    EXPECT_EQ(TStoreDirs::Open(Fs_, Path_, Latch_).Error(), std::errc::no_such_file_or_directory);
    EXPECT_FALSE(std::filesystem::exists(Path_ / "warehouse"));
    EXPECT_TRUE(Paths(Fs_->Events(), EIOOp::MakeDir).empty());
    auto store = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(store);
    const auto first = Fs_->Events().size();
    EXPECT_EQ(store.Value()->OpenCollection(CollectionId).Error(), std::errc::no_such_file_or_directory);
    const auto collection = Path_ / "warehouse" / CollectionId;
    ASSERT_TRUE(std::filesystem::create_directory(collection));
    EXPECT_EQ(store.Value()->OpenCollection(CollectionId).Error(), std::errc::no_such_file_or_directory);
    ASSERT_TRUE(std::filesystem::create_directory(collection / "data"));
    EXPECT_EQ(store.Value()->OpenCollection(CollectionId).Error(), std::errc::no_such_file_or_directory);
    EXPECT_TRUE(Paths(Fs_->Events(), EIOOp::MakeDir, first).empty());
    EXPECT_TRUE(Paths(Fs_->Events(), EIOOp::SyncDir, first).empty());
}

TEST_F(TStoreDirsTest, StopsAtEveryFailedStoreSyncAndBlocksIoErrors)
{
    for (std::uint64_t position = 1; position <= 3; ++position) {
        SCOPED_TRACE(position);
        auto faults = std::make_shared<TFaultFS>(*Underlying_);
        auto blocked = std::make_shared<TWriteLatch>();
        faults->AddRule({EIOOp::SyncDir, "*", position, 1, std::make_error_code(std::errc::io_error), {}});
        const auto target = Parent_ / std::to_string(position);
        EXPECT_EQ(TStoreDirs::Create(faults, target, blocked).Error(), std::errc::io_error);
        EXPECT_EQ(Paths(faults->Events(), EIOOp::SyncDir).size(), position);
        EXPECT_TRUE(blocked->IsSet());
    }
}

TEST_F(TStoreDirsTest, StopsAtEveryFailedCollectionSyncIncludingAncestors)
{
    for (std::uint64_t position = 1; position <= 5; ++position) {
        SCOPED_TRACE(position);
        auto faults = std::make_shared<TFaultFS>(*Underlying_);
        auto blocked = std::make_shared<TWriteLatch>();
        const auto target = Parent_ / std::to_string(position);
        auto store = TStoreDirs::Create(faults, target, blocked);
        ASSERT_TRUE(store);
        const auto first = faults->Events().size();
        faults->AddRule({EIOOp::SyncDir, "*", position, 1, std::make_error_code(std::errc::read_only_file_system), {}});
        EXPECT_EQ(store.Value()->CreateCollection(CollectionId).Error(), std::errc::read_only_file_system);
        EXPECT_EQ(Paths(faults->Events(), EIOOp::SyncDir, first).size(), position);
        EXPECT_TRUE(blocked->IsSet());
    }
}

TEST_F(TStoreDirsTest, RetriesPartialCreationAndCapacityFailureWithoutSkippingSync)
{
    auto store = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(store);
    const auto metadata = Path_ / "warehouse" / CollectionId / "metadata";
    Fs_->AddRule({EIOOp::MakeDir, metadata.string(), 1, 1, std::make_error_code(std::errc::no_space_on_device), {}});
    EXPECT_EQ(store.Value()->CreateCollection(CollectionId).Error(), std::errc::no_space_on_device);
    EXPECT_FALSE(Latch_->IsSet());
    EXPECT_FALSE(std::filesystem::exists(metadata));
    const auto first = Fs_->Events().size();
    EXPECT_TRUE(store.Value()->CreateCollection(CollectionId));
    EXPECT_EQ(Paths(Fs_->Events(), EIOOp::SyncDir, first), CollectionSyncs(Path_));
}

TEST_F(TStoreDirsTest, RejectsCreateBeforeIoWhenLatchIsSet)
{
    auto store = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(store);
    Latch_->Block();
    const auto first = Fs_->Events().size();
    EXPECT_EQ(TStoreDirs::Create(Fs_, Parent_ / "another", Latch_).Error(), std::errc::io_error);
    EXPECT_EQ(store.Value()->CreateCollection(CollectionId).Error(), std::errc::io_error);
    EXPECT_EQ(Fs_->Events().size(), first);
    EXPECT_FALSE(std::filesystem::exists(Parent_ / "another"));
}

TEST_F(TStoreDirsTest, ObservesFailuresFromDirectoryCreationAndWritePathOpen)
{
    Fs_->AddRule({EIOOp::MakeDir, Path_.string(), 1, 1, std::make_error_code(std::errc::io_error), {}});
    EXPECT_EQ(TStoreDirs::Create(Fs_, Path_, Latch_).Error(), std::errc::io_error);
    EXPECT_TRUE(Latch_->IsSet());
    auto newLatch = std::make_shared<TWriteLatch>();
    Fs_->AddRule({EIOOp::OpenDir,
        (Path_ / "warehouse").string(),
        1,
        1,
        std::make_error_code(std::errc::read_only_file_system),
        {}});
    EXPECT_EQ(TStoreDirs::Create(Fs_, Path_, newLatch).Error(), std::errc::read_only_file_system);
    EXPECT_TRUE(newLatch->IsSet());
}

TEST_F(TStoreDirsTest, RejectsNullOwnersInvalidRootsAndMissingParents)
{
    EXPECT_EQ(TStoreDirs::Create({}, Path_, Latch_).Error(), std::errc::invalid_argument);
    EXPECT_EQ(TStoreDirs::Open(Fs_, Path_, {}).Error(), std::errc::invalid_argument);
    for (const auto& invalid : {std::filesystem::path{},
             std::filesystem::path("relative"),
             std::filesystem::path("/"),
             Path_ / "..",
             Path_ / ".",
             std::filesystem::path(Path_.string() + "/"),
             std::filesystem::path(std::string("/a\0b", 4))})
    {
        SCOPED_TRACE(invalid.string());
        EXPECT_EQ(TStoreDirs::Create(Fs_, invalid, Latch_).Error(), std::errc::invalid_argument);
        EXPECT_EQ(TStoreDirs::Open(Fs_, invalid, Latch_).Error(), std::errc::invalid_argument);
    }
    EXPECT_TRUE(Fs_->Events().empty());
    EXPECT_EQ(TStoreDirs::Create(Fs_, Parent_ / "missing/store", Latch_).Error(), std::errc::no_such_file_or_directory);
    EXPECT_FALSE(std::filesystem::exists(Parent_ / "missing"));
}

TEST_F(TStoreDirsTest, RejectsInvalidCollectionIdsBeforeIo)
{
    auto store = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(store);
    const auto first = Fs_->Events().size();
    for (const auto& invalid : {std::string{},
             std::string(".."),
             std::string("/absolute"),
             std::string("01234567-89AB-cdef-0123-456789abcdef"),
             std::string("01234567-89ab-cdef-0123-456789abcde"),
             std::string("01234567-89ab-cdef-0123-456789abcde\0", 36)})
    {
        EXPECT_EQ(store.Value()->CreateCollection(invalid).Error(), std::errc::invalid_argument);
        EXPECT_EQ(store.Value()->OpenCollection(invalid).Error(), std::errc::invalid_argument);
    }
    EXPECT_EQ(Fs_->Events().size(), first);
}

TEST_F(TStoreDirsTest, RejectsSymlinksAtRootAncestorAndWarehouse)
{
    ASSERT_TRUE(std::filesystem::create_directory(Parent_ / "target"));
    std::filesystem::create_directory_symlink(Parent_ / "target", Path_);
    EXPECT_FALSE(TStoreDirs::Create(Fs_, Path_, Latch_));
    EXPECT_FALSE(TStoreDirs::Open(Fs_, Path_, Latch_));
    EXPECT_FALSE(TStoreDirs::Create(Fs_, Path_ / "child", Latch_));
    EXPECT_FALSE(std::filesystem::exists(Parent_ / "target/child"));
    ASSERT_TRUE(std::filesystem::remove(Path_));
    ASSERT_TRUE(std::filesystem::create_directory(Path_));
    std::filesystem::create_directory_symlink(Parent_ / "target", Path_ / "warehouse");
    EXPECT_FALSE(TStoreDirs::Create(Fs_, Path_, Latch_));
    EXPECT_FALSE(TStoreDirs::Open(Fs_, Path_, Latch_));
}

TEST_F(TStoreDirsTest, ExistingCollectionSymlinksCannotBypassDirectoryChecks)
{
    auto store = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(store);
    const auto target = Parent_ / "target";
    ASSERT_TRUE(std::filesystem::create_directory(target));
    const auto collection = Path_ / "warehouse" / CollectionId;
    std::filesystem::create_directory_symlink(target, collection);
    EXPECT_FALSE(store.Value()->CreateCollection(CollectionId));
    EXPECT_FALSE(store.Value()->OpenCollection(CollectionId));
    ASSERT_TRUE(std::filesystem::remove(collection));
    ASSERT_TRUE(std::filesystem::create_directory(collection));
    std::filesystem::create_directory_symlink(target, collection / "data");
    EXPECT_FALSE(store.Value()->CreateCollection(CollectionId));
    EXPECT_FALSE(store.Value()->OpenCollection(CollectionId));
    EXPECT_TRUE(std::filesystem::is_empty(target));
}

TEST_F(TStoreDirsTest, RootReplacementBlocksWritesButKeepsAnchoredReads)
{
    auto store = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.Value()->CreateCollection(CollectionId));
    const auto rootInode = Fs_->Stat(store.Value()->Root()).Value().Inode;
    std::filesystem::rename(Path_, Parent_ / "previous");
    ASSERT_TRUE(std::filesystem::create_directory(Path_));
    EXPECT_EQ(store.Value()->CheckIdentity(), std::errc::state_not_recoverable);
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_EQ(Fs_->Stat(store.Value()->Root()).Value().Inode, rootInode);
    EXPECT_TRUE(store.Value()->OpenCollection(CollectionId));
    EXPECT_EQ(store.Value()->CreateCollection(CollectionId).Error(), std::errc::io_error);
    EXPECT_TRUE(std::filesystem::is_empty(Path_));
}

TEST_F(TStoreDirsTest, MissingRootAndIdentityIoFailuresBlockFurtherWrites)
{
    auto store = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(store);
    std::filesystem::rename(Path_, Parent_ / "previous");
    EXPECT_EQ(store.Value()->CheckIdentity(), std::errc::no_such_file_or_directory);
    EXPECT_TRUE(Latch_->IsSet());
    std::filesystem::rename(Parent_ / "previous", Path_);
    auto secondLatch = std::make_shared<TWriteLatch>();
    auto reopened = TStoreDirs::Open(Fs_, Path_, secondLatch);
    ASSERT_TRUE(reopened);
    Fs_->AddRule({EIOOp::Stat, Path_.string(), 1, 1, std::make_error_code(std::errc::io_error), {}});
    EXPECT_EQ(reopened.Value()->CheckIdentity(), std::errc::io_error);
    EXPECT_TRUE(secondLatch->IsSet());
}

TEST_F(TStoreDirsTest, CollectionCreateRechecksRootBeforeWriting)
{
    auto store = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(store);
    std::filesystem::rename(Path_, Parent_ / "previous");
    ASSERT_TRUE(std::filesystem::create_directory(Path_));
    const auto first = Fs_->Events().size();
    EXPECT_EQ(store.Value()->CreateCollection(CollectionId).Error(), std::errc::state_not_recoverable);
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_TRUE(Paths(Fs_->Events(), EIOOp::MakeDir, first).empty());
}

TEST_F(TStoreDirsTest, RetainsFilesystemAndLatchUntilTheStoreIsReleased)
{
    auto filesystem = std::make_shared<TPosixLocalFS>();
    auto writeLatch = std::make_shared<TWriteLatch>();
    std::weak_ptr<ILocalFS> weakFs = filesystem;
    std::weak_ptr<TWriteLatch> weakLatch = writeLatch;
    auto store = TStoreDirs::Create(filesystem, Path_, writeLatch).Value();
    filesystem.reset();
    writeLatch.reset();
    EXPECT_FALSE(weakFs.expired());
    EXPECT_FALSE(weakLatch.expired());
    EXPECT_FALSE(store->CheckIdentity());
    auto collection = store->CreateCollection(CollectionId);
    ASSERT_TRUE(collection);
    EXPECT_EQ(store->Fs()->Stat(collection.Value().Data).Value().Type, EFileType::Directory);
    store.reset();
    EXPECT_TRUE(weakFs.expired());
    EXPECT_TRUE(weakLatch.expired());
}

}
}
