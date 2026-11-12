#include "core/storage/recovery_visited.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>

namespace NGlamDB::NStorage {
namespace {

using NIO::EFileKind;

std::string PathFor(int index)
{
    auto path = "store://visited/" + std::to_string(index);
    path.resize(2048, 'x');
    return path;
}

TEST(RecoveryVisited, DistinguishesExactDuplicatesAndKinds)
{
    auto visited = TRecoveryVisited::Make();
    ASSERT_TRUE(visited) << visited.error().message;
    for (const auto kind : {EFileKind::Data, EFileKind::ManifestList, EFileKind::Manifest, EFileKind::Metadata}) {
        const auto first = (*visited)->Insert(kind, "store://path/я");
        ASSERT_TRUE(first);
        EXPECT_TRUE(*first);
        const auto duplicate = (*visited)->Insert(kind, "store://path/я");
        ASSERT_TRUE(duplicate);
        EXPECT_FALSE(*duplicate);
        const auto distinct = (*visited)->Insert(kind, "store://path/Я");
        ASSERT_TRUE(distinct);
        EXPECT_TRUE(*distinct);
    }
    auto independent = TRecoveryVisited::Make();
    ASSERT_TRUE(independent);
    EXPECT_EQ((*independent)->Insert(EFileKind::Data, "store://path/я"), true);
}

TEST(RecoveryVisited, RejectsInvalidWorkspaceLimits)
{
    for (const auto limit : {0ULL, 8191ULL, (1ULL << 40) + 1, std::numeric_limits<unsigned long long>::max()}) {
        const auto visited = TRecoveryVisited::Make(limit);
        ASSERT_FALSE(visited);
        EXPECT_EQ(visited.error().kind, iceberg::ErrorKind::kInvalidArgument);
    }
    EXPECT_TRUE(TRecoveryVisited::Make(8192));
    EXPECT_TRUE(TRecoveryVisited::Make(512ULL << 20));
}

TEST(RecoveryVisited, PreservesTheFirstInvalidInputError)
{
    for (const auto& path : {std::string{}, std::string("a\0b", 3), std::string(2049, 'x')}) {
        auto visited = TRecoveryVisited::Make();
        ASSERT_TRUE(visited);
        EXPECT_EQ((*visited)->Insert(EFileKind::Data, PathFor(1)), true);
        const auto invalid = (*visited)->Insert(EFileKind::Data, path);
        ASSERT_FALSE(invalid);
        EXPECT_EQ(invalid.error().kind, iceberg::ErrorKind::kInvalidArgument);
        const auto later = (*visited)->Insert(EFileKind::Data, PathFor(1));
        ASSERT_FALSE(later);
        EXPECT_EQ(later.error().message, invalid.error().message);
    }
    // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
    for (const auto kind : {EFileKind::Other, static_cast<EFileKind>(255)}) {
        auto visited = TRecoveryVisited::Make();
        ASSERT_TRUE(visited);
        EXPECT_FALSE((*visited)->Insert(kind, "path"));
    }
}

TEST(RecoveryVisited, FloorsWorkspaceBytesAndNeverReportsFullAsDuplicate)
{
    auto tooSmall = TRecoveryVisited::Make(3ULL * 4096 - 1);
    ASSERT_TRUE(tooSmall);
    const auto full = (*tooSmall)->Insert(EFileKind::Data, PathFor(1));
    ASSERT_FALSE(full);
    EXPECT_NE(full.error().message.find("CAPACITY_EXCEEDED"), std::string::npos);

    auto exact = TRecoveryVisited::Make(3ULL * 4096);
    ASSERT_TRUE(exact);
    EXPECT_EQ((*exact)->Insert(EFileKind::Data, PathFor(1)), true);
    EXPECT_EQ((*exact)->Insert(EFileKind::Data, PathFor(1)), false);
    const auto exceeded = (*exact)->Insert(EFileKind::Data, PathFor(2));
    ASSERT_FALSE(exceeded);
    EXPECT_NE(exceeded.error().message.find("CAPACITY_EXCEEDED"), std::string::npos);
    const auto sticky = (*exact)->Insert(EFileKind::Data, PathFor(1));
    ASSERT_FALSE(sticky);
    EXPECT_EQ(sticky.error().message, exceeded.error().message);
}

class TRecoveryVisitedVfsTest : public ::testing::Test
{
protected:
    static TRecoveryVisitedVfsTest* Active_;
    sqlite3_vfs* Parent_ = nullptr;
    sqlite3_vfs Vfs_{};
    sqlite3_file* File_ = nullptr;
    const sqlite3_io_methods* OriginalMethods_ = nullptr;
    sqlite3_io_methods Methods_{};
    bool FailOpen_ = false;
    bool FailWrite_ = false;
    int OpenCount_ = 0;
    int CloseCount_ = 0;
    int WriteCount_ = 0;
    int OpenFlags_ = 0;
    std::int64_t MaximumBytes_ = 0;

    void SetUp() override
    {
        ASSERT_EQ(Active_, nullptr);
        Parent_ = sqlite3_vfs_find(nullptr);
        ASSERT_NE(Parent_, nullptr);
        Vfs_ = *Parent_;
        Vfs_.pNext = nullptr;
        Vfs_.zName = "glamdb-recovery-visited-test";
        Vfs_.xOpen = Open;
        Active_ = this;
        ASSERT_EQ(sqlite3_vfs_register(&Vfs_, 1), SQLITE_OK);
    }

    void TearDown() override
    {
        EXPECT_EQ(File_, nullptr);
        EXPECT_EQ(sqlite3_vfs_register(Parent_, 1), SQLITE_OK);
        EXPECT_EQ(sqlite3_vfs_unregister(&Vfs_), SQLITE_OK);
        Active_ = nullptr;
    }

    static int Open(sqlite3_vfs* /*vfs*/, const char* name, sqlite3_file* file, int flags, int* outFlags)
    {
        auto& owner = *Active_;
        ++owner.OpenCount_;
        owner.OpenFlags_ = flags;
        if (owner.FailOpen_) {
            return SQLITE_CANTOPEN;
        }
        const auto result = owner.Parent_->xOpen(owner.Parent_, name, file, flags, outFlags);
        if (result != SQLITE_OK) {
            return result;
        }
        EXPECT_EQ(owner.File_, nullptr);
        owner.File_ = file;
        owner.OriginalMethods_ = file->pMethods;
        owner.Methods_ = *file->pMethods;
        owner.Methods_.xClose = Close;
        owner.Methods_.xWrite = Write;
        file->pMethods = &owner.Methods_;
        return SQLITE_OK;
    }

    static int Close(sqlite3_file* file)
    {
        auto& owner = *Active_;
        sqlite3_int64 bytes = 0;
        if (owner.OriginalMethods_->xFileSize(file, &bytes) == SQLITE_OK) {
            owner.MaximumBytes_ = std::max(owner.MaximumBytes_, static_cast<std::int64_t>(bytes));
        }
        file->pMethods = owner.OriginalMethods_;
        const auto result = owner.OriginalMethods_->xClose(file);
        owner.File_ = nullptr;
        ++owner.CloseCount_;
        return result;
    }

    static int Write(sqlite3_file* file, const void* data, int count, sqlite3_int64 offset)
    {
        auto& owner = *Active_;
        ++owner.WriteCount_;
        owner.MaximumBytes_ = std::max(owner.MaximumBytes_, static_cast<std::int64_t>(offset + count));
        return owner.FailWrite_ ? SQLITE_IOERR_WRITE : owner.OriginalMethods_->xWrite(file, data, count, offset);
    }
};

TRecoveryVisitedVfsTest* TRecoveryVisitedVfsTest::Active_ = nullptr;

TEST_F(TRecoveryVisitedVfsTest, SpillsToOneBoundedDeleteOnCloseFileAndCleansUp)
{
    constexpr std::uint64_t limit = 64ULL * 1024;
    auto visited = TRecoveryVisited::Make(limit);
    ASSERT_TRUE(visited);
    bool full = false;
    for (int index = 0; index < 100; ++index) {
        const auto inserted = (*visited)->Insert(EFileKind::Data, PathFor(index));
        if (!inserted) {
            EXPECT_NE(inserted.error().message.find("CAPACITY_EXCEEDED"), std::string::npos);
            full = true;
            break;
        }
        EXPECT_TRUE(*inserted);
    }
    EXPECT_TRUE(full);
    EXPECT_EQ(OpenCount_, 1);
    EXPECT_NE(OpenFlags_ & SQLITE_OPEN_TEMP_DB, 0);
    EXPECT_NE(OpenFlags_ & SQLITE_OPEN_DELETEONCLOSE, 0);
    EXPECT_EQ(OpenFlags_ & (SQLITE_OPEN_MEMORY | SQLITE_OPEN_MAIN_JOURNAL | SQLITE_OPEN_TEMP_JOURNAL), 0);
    EXPECT_GT(WriteCount_, 0);
    visited->reset();
    EXPECT_EQ(CloseCount_, 1);
    EXPECT_GT(MaximumBytes_, 0);
    EXPECT_LE(MaximumBytes_, static_cast<std::int64_t>(limit));
}

TEST_F(TRecoveryVisitedVfsTest, PreservesWriteFailureAndStopsUsingTheDatabase)
{
    FailWrite_ = true;
    auto visited = TRecoveryVisited::Make();
    ASSERT_TRUE(visited);
    std::string firstError;
    for (int index = 0; index < 100; ++index) {
        const auto inserted = (*visited)->Insert(EFileKind::Data, PathFor(index));
        if (!inserted) {
            firstError = inserted.error().message;
            EXPECT_EQ(inserted.error().kind, iceberg::ErrorKind::kIOError);
            EXPECT_NE(firstError.find(std::to_string(SQLITE_IOERR_WRITE)), std::string::npos);
            break;
        }
    }
    ASSERT_FALSE(firstError.empty());
    EXPECT_GT(WriteCount_, 0);
    const auto writes = WriteCount_;
    FailWrite_ = false;
    const auto later = (*visited)->Insert(EFileKind::Data, PathFor(0));
    ASSERT_FALSE(later);
    EXPECT_EQ(later.error().message, firstError);
    EXPECT_EQ(WriteCount_, writes);
    visited->reset();
    EXPECT_EQ(CloseCount_, 1);
}

TEST_F(TRecoveryVisitedVfsTest, PreservesTemporaryFileOpenFailure)
{
    FailOpen_ = true;
    auto visited = TRecoveryVisited::Make();
    ASSERT_TRUE(visited);
    std::string firstError;
    for (int index = 0; index < 100; ++index) {
        const auto inserted = (*visited)->Insert(EFileKind::Data, PathFor(index));
        if (!inserted) {
            firstError = inserted.error().message;
            EXPECT_NE(firstError.find(std::to_string(SQLITE_CANTOPEN)), std::string::npos);
            break;
        }
    }
    ASSERT_FALSE(firstError.empty());
    const auto opens = OpenCount_;
    FailOpen_ = false;
    const auto later = (*visited)->Insert(EFileKind::ManifestList, "fresh");
    ASSERT_FALSE(later);
    EXPECT_EQ(later.error().message, firstError);
    EXPECT_EQ(OpenCount_, opens);
    visited->reset();
    EXPECT_EQ(CloseCount_, 0);
}

} // namespace
} // namespace NGlamDB::NStorage
