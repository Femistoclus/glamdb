#include "core/io/local_fs.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <limits>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace NGlamDB::NIO {
namespace {

std::span<const std::byte> Bytes(std::string_view text)
{
    return std::as_bytes(std::span(text.data(), text.size()));
}

class TLocalFSTest : public testing::Test
{
protected:
    TPosixLocalFS Fs_;
    std::filesystem::path Path_;
    THandle Root_;

    void SetUp() override
    {
        auto name = (std::filesystem::temp_directory_path() / "glamdb-fs-XXXXXX").string();
        ASSERT_NE(::mkdtemp(name.data()), nullptr);
        Path_ = std::filesystem::canonical(name);
        auto root = Fs_.OpenDir(Path_);
        ASSERT_TRUE(root) << root.Error();
        Root_ = std::move(root).Value();
    }

    void TearDown() override
    {
        Root_.reset();
        std::error_code error;
        std::filesystem::remove_all(Path_, error);
        EXPECT_FALSE(error) << error;
    }
};

TEST_F(TLocalFSTest, WritesSyncsClosesAndReadsExactBytes)
{
    auto created = Fs_.CreateExclusive(Root_, "data");
    ASSERT_TRUE(created) << created.Error();
    const auto file = created.Value();
    const std::string content("a\0bc", 4);
    auto written = Fs_.Write(file, Bytes(content));
    ASSERT_TRUE(written);
    EXPECT_EQ(written.Value(), content.size());
    auto empty = Fs_.Write(file, {});
    ASSERT_TRUE(empty);
    EXPECT_EQ(empty.Value(), 0);
    auto info = Fs_.Stat(file);
    ASSERT_TRUE(info);
    EXPECT_EQ(info.Value().Size, 4);
    EXPECT_EQ(info.Value().Type, EFileType::Regular);
    EXPECT_FALSE(Fs_.SyncFile(file));
    EXPECT_FALSE(Fs_.Close(file));
    EXPECT_FALSE(Fs_.SyncDir(Root_));

    auto opened = Fs_.OpenRead(Root_, "data");
    ASSERT_TRUE(opened);
    std::array<std::byte, 8> buffer{};
    auto read = Fs_.PRead(opened.Value(), 0, buffer);
    ASSERT_TRUE(read);
    EXPECT_EQ(read.Value(), 4);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(buffer.data()), 4), content);
    read = Fs_.PRead(opened.Value(), 1, std::span(buffer).first(2));
    ASSERT_TRUE(read);
    EXPECT_EQ(read.Value(), 2);
    EXPECT_EQ(buffer[0], std::byte{0});
    EXPECT_EQ(buffer[1], std::byte{'b'});
    read = Fs_.PRead(opened.Value(), 4, buffer);
    ASSERT_TRUE(read);
    EXPECT_EQ(read.Value(), 0);
    EXPECT_EQ(Fs_.Write(opened.Value(), Bytes("x")).Error(), std::errc::bad_file_descriptor);
    EXPECT_FALSE(Fs_.Close(opened.Value()));
}

TEST_F(TLocalFSTest, CollisionPreservesExistingBytes)
{
    auto created = Fs_.CreateExclusive(Root_, "data");
    ASSERT_TRUE(created);
    ASSERT_TRUE(Fs_.Write(created.Value(), Bytes("original")));
    EXPECT_EQ(Fs_.CreateExclusive(Root_, "data").Error(), std::errc::file_exists);
    EXPECT_EQ(Fs_.Stat(created.Value()).Value().Size, 8);
    EXPECT_FALSE(Fs_.Close(created.Value()));
    auto opened = Fs_.OpenRead(Root_, "data");
    ASSERT_TRUE(opened);
    std::array<std::byte, 8> buffer{};
    ASSERT_TRUE(Fs_.PRead(opened.Value(), 0, buffer));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(buffer.data()), buffer.size()), "original");
}

TEST_F(TLocalFSTest, DirectoriesAreExplicitAndAnchored)
{
    EXPECT_EQ(Fs_.OpenDir(Root_, "child").Error(), std::errc::no_such_file_or_directory);
    EXPECT_FALSE(Fs_.MakeDir(Root_, "child"));
    EXPECT_EQ(Fs_.MakeDir(Root_, "child"), std::errc::file_exists);
    auto directory = Fs_.OpenDir(Root_, "child");
    ASSERT_TRUE(directory);
    const auto original = Fs_.Stat(directory.Value());
    ASSERT_TRUE(original);
    EXPECT_EQ(original.Value().Type, EFileType::Directory);
    std::filesystem::rename(Path_ / "child", Path_ / "moved");
    ASSERT_FALSE(Fs_.MakeDir(Root_, "child"));
    auto replacement = Fs_.OpenDir(Root_, "child");
    ASSERT_TRUE(replacement);
    EXPECT_NE(Fs_.Stat(replacement.Value()).Value().Inode, original.Value().Inode);
    auto file = Fs_.CreateExclusive(directory.Value(), "anchored");
    ASSERT_TRUE(file);
    EXPECT_TRUE(std::filesystem::exists(Path_ / "moved/anchored"));
    EXPECT_FALSE(std::filesystem::exists(Path_ / "child/anchored"));
    EXPECT_FALSE(Fs_.SyncDir(directory.Value()));
    EXPECT_FALSE(Fs_.SyncDir(Root_));
}

TEST_F(TLocalFSTest, RejectsInvalidLeafNamesEverywhere)
{
    const std::vector<std::string> invalid{"", ".", "..", "a/b", "/absolute", std::string("a\0b", 3)};
    for (const auto& name : invalid) {
        SCOPED_TRACE(name);
        EXPECT_EQ(Fs_.OpenDir(Root_, name).Error(), std::errc::invalid_argument);
        EXPECT_EQ(Fs_.MakeDir(Root_, name), std::errc::invalid_argument);
        EXPECT_EQ(Fs_.CreateExclusive(Root_, name).Error(), std::errc::invalid_argument);
        EXPECT_EQ(Fs_.OpenRead(Root_, name).Error(), std::errc::invalid_argument);
    }
    EXPECT_EQ(Fs_.OpenDir("relative").Error(), std::errc::invalid_argument);
    EXPECT_EQ(Fs_.OpenDir(Path_ / "..").Error(), std::errc::invalid_argument);
    EXPECT_EQ(Fs_.OpenDir(std::filesystem::path(std::string("/a\0b", 4))).Error(), std::errc::invalid_argument);
}

TEST_F(TLocalFSTest, RejectsSymlinksAndSpecialFilesWithoutBlocking)
{
    ASSERT_FALSE(Fs_.MakeDir(Root_, "directory"));
    std::filesystem::create_directory_symlink(Path_ / "directory", Path_ / "link");
    EXPECT_FALSE(Fs_.OpenDir(Root_, "link"));
    EXPECT_FALSE(Fs_.OpenDir(Path_ / "link"));
    ASSERT_FALSE(Fs_.MakeDir(Fs_.OpenDir(Root_, "directory").Value(), "nested"));
    EXPECT_FALSE(Fs_.OpenDir(Path_ / "link/nested"));
    auto file = Fs_.CreateExclusive(Root_, "regular");
    ASSERT_TRUE(file);
    std::filesystem::create_symlink(Path_ / "regular", Path_ / "file-link");
    EXPECT_FALSE(Fs_.OpenRead(Root_, "file-link"));
    EXPECT_EQ(Fs_.CreateExclusive(Root_, "file-link").Error(), std::errc::file_exists);
    EXPECT_FALSE(Fs_.OpenRead(Root_, "directory"));
    ASSERT_EQ(::mkfifo((Path_ / "fifo").c_str(), 0600), 0);
    EXPECT_EQ(Fs_.OpenRead(Root_, "fifo").Error(), std::errc::invalid_argument);
    EXPECT_EQ(Fs_.CreateExclusive(Root_, "fifo").Error(), std::errc::file_exists);
}

TEST_F(TLocalFSTest, InvalidHandlesAndWrongKindsFail)
{
    const auto foreign = std::make_shared<TFSHandle>();
    for (const THandle& handle : {THandle{}, foreign}) {
        EXPECT_EQ(Fs_.OpenDir(handle, "child").Error(), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.CreateExclusive(handle, "data").Error(), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.OpenRead(handle, "data").Error(), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.MakeDir(handle, "child"), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.Write(handle, {}).Error(), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.PRead(handle, 0, {}).Error(), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.Stat(handle).Error(), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.StatFs(handle).Error(), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.SyncFile(handle), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.SyncDir(handle), std::errc::bad_file_descriptor);
        EXPECT_EQ(Fs_.Close(handle), std::errc::bad_file_descriptor);
    }
    auto file = Fs_.CreateExclusive(Root_, "data");
    ASSERT_TRUE(file);
    EXPECT_EQ(Fs_.OpenDir(file.Value(), "child").Error(), std::errc::not_a_directory);
    EXPECT_EQ(Fs_.StatFs(file.Value()).Error(), std::errc::not_a_directory);
    EXPECT_EQ(Fs_.SyncDir(file.Value()), std::errc::not_a_directory);
    EXPECT_EQ(Fs_.Write(Root_, {}).Error(), std::errc::invalid_argument);
    EXPECT_EQ(Fs_.SyncFile(Root_), std::errc::invalid_argument);
}

TEST_F(TLocalFSTest, ClosedHandleCannotCloseReusedDescriptor)
{
    auto first = Fs_.CreateExclusive(Root_, "first");
    ASSERT_TRUE(first);
    const auto alias = first.Value();
    EXPECT_FALSE(Fs_.Close(first.Value()));
    auto second = Fs_.CreateExclusive(Root_, "second");
    ASSERT_TRUE(second);
    EXPECT_EQ(Fs_.Close(alias), std::errc::bad_file_descriptor);
    EXPECT_EQ(Fs_.Stat(alias).Error(), std::errc::bad_file_descriptor);
    EXPECT_EQ(Fs_.Write(alias, Bytes("bad")).Error(), std::errc::bad_file_descriptor);
    EXPECT_EQ(Fs_.SyncFile(alias), std::errc::bad_file_descriptor);
    EXPECT_EQ(Fs_.PRead(alias, 0, {}).Error(), std::errc::bad_file_descriptor);
    auto written = Fs_.Write(second.Value(), Bytes("ok"));
    ASSERT_TRUE(written);
    EXPECT_EQ(written.Value(), 2);
}

TEST_F(TLocalFSTest, ChecksOffsetAndReportsSpace)
{
    auto file = Fs_.CreateExclusive(Root_, "data");
    ASSERT_TRUE(file);
    ASSERT_FALSE(Fs_.Close(file.Value()));
    auto input = Fs_.OpenRead(Root_, "data");
    ASSERT_TRUE(input);
    EXPECT_EQ(Fs_.PRead(input.Value(), std::numeric_limits<std::uint64_t>::max(), {}).Error(),
        std::errc::value_too_large);
    const auto space = Fs_.StatFs(Root_);
    ASSERT_TRUE(space);
    EXPECT_GT(space.Value().Capacity, 0);
    EXPECT_LE(space.Value().Available, space.Value().Capacity);
    EXPECT_NE(TPosixLocalFS::Profile().find("UNQUALIFIED"), std::string_view::npos);
}

TEST_F(TLocalFSTest, MeasuresAllocatedAndSparseFilesIncludingOrphansWithExactLimit)
{
    ASSERT_FALSE(Fs_.MakeDir(Root_, "data"));
    auto data = Fs_.OpenDir(Root_, "data");
    ASSERT_TRUE(data);
    auto orphan = Fs_.CreateExclusive(data.Value(), "orphan.parquet");
    ASSERT_TRUE(orphan);
    ASSERT_TRUE(Fs_.Write(orphan.Value(), Bytes("x")));
    ASSERT_FALSE(Fs_.Close(orphan.Value()));
    auto sparse = Fs_.CreateExclusive(Root_, "sparse");
    ASSERT_TRUE(sparse);
    ASSERT_FALSE(Fs_.Close(sparse.Value()));
    std::filesystem::resize_file(Path_ / "sparse", 65536);
    std::uint64_t expected = 0;
    for (const auto& path : {Path_, Path_ / "data", Path_ / "data/orphan.parquet", Path_ / "sparse"}) {
        struct stat info
        { };
        ASSERT_EQ(::stat(path.c_str(), &info), 0);
        const auto allocated = static_cast<std::uint64_t>(info.st_blocks) * 512;
        expected += S_ISREG(info.st_mode) ? std::max(allocated, static_cast<std::uint64_t>(info.st_size)) : allocated;
    }
    const auto first = Fs_.MeasureTree(Root_, expected);
    ASSERT_TRUE(first);
    EXPECT_EQ(first.Value(), expected);
    EXPECT_EQ(Fs_.MeasureTree(Root_, expected).Value(), expected);
    EXPECT_EQ(Fs_.MeasureTree(Root_, expected - 1).Error(), std::errc::no_space_on_device);
    EXPECT_EQ(Fs_.MeasureTree({}, expected).Error(), std::errc::bad_file_descriptor);
    auto file = Fs_.OpenRead(Root_, "sparse");
    ASSERT_TRUE(file);
    EXPECT_EQ(Fs_.MeasureTree(file.Value(), expected).Error(), std::errc::not_a_directory);
    ASSERT_FALSE(Fs_.Close(Root_));
    EXPECT_EQ(Fs_.MeasureTree(Root_, expected).Error(), std::errc::bad_file_descriptor);
}

TEST_F(TLocalFSTest, TreeMeasurementRejectsSymlinksSpecialFilesAndExcessiveDepth)
{
    constexpr auto limit = std::numeric_limits<std::uint64_t>::max();
    std::filesystem::create_directory_symlink(Path_, Path_ / "loop");
    EXPECT_EQ(Fs_.MeasureTree(Root_, limit).Error(), std::errc::invalid_argument);
    ASSERT_TRUE(std::filesystem::remove(Path_ / "loop"));
    ASSERT_EQ(::mkfifo((Path_ / "fifo").c_str(), 0600), 0);
    EXPECT_EQ(Fs_.MeasureTree(Root_, limit).Error(), std::errc::invalid_argument);
    ASSERT_TRUE(std::filesystem::remove(Path_ / "fifo"));
    auto directory = Root_;
    for (int depth = 0; depth < 64; ++depth) {
        ASSERT_FALSE(Fs_.MakeDir(directory, "d"));
        auto child = Fs_.OpenDir(directory, "d");
        ASSERT_TRUE(child);
        directory = std::move(child).Value();
    }
    ASSERT_TRUE(Fs_.MeasureTree(Root_, limit));
    ASSERT_FALSE(Fs_.MakeDir(directory, "d"));
    EXPECT_EQ(Fs_.MeasureTree(Root_, limit).Error(), std::errc::value_too_large);
}

TEST_F(TLocalFSTest, PositionalReadsAreIndependentAcrossThreads)
{
    auto output = Fs_.CreateExclusive(Root_, "data");
    ASSERT_TRUE(output);
    ASSERT_TRUE(Fs_.Write(output.Value(), Bytes("0123456789abcdef")));
    ASSERT_FALSE(Fs_.Close(output.Value()));
    auto input = Fs_.OpenRead(Root_, "data");
    ASSERT_TRUE(input);
    std::vector<std::future<std::byte>> reads;
    for (std::size_t i = 0; i < 16; ++i) {
        reads.emplace_back(std::async(std::launch::async, [&, i] {
            std::array<std::byte, 1> byte{};
            const auto read = Fs_.PRead(input.Value(), i, byte);
            EXPECT_TRUE(read);
            if (read) {
                EXPECT_EQ(read.Value(), 1);
            }
            return byte[0];
        }));
    }
    for (std::size_t i = 0; i < reads.size(); ++i) {
        EXPECT_EQ(reads[i].get(), Bytes("0123456789abcdef")[i]);
    }
}

TEST(IoResult, HoldsValuesAndPreservesErrorCodes)
{
    TResult<int> value(7);
    ASSERT_TRUE(value);
    EXPECT_EQ(value.Value(), 7);
    EXPECT_FALSE(value.Error());
    const TResult<int> failure(std::errc::no_space_on_device);
    EXPECT_FALSE(failure);
    EXPECT_EQ(failure.Error(), std::errc::no_space_on_device);
    const auto error = std::make_error_code(std::errc::interrupted);
    EXPECT_EQ(TResult<int>(error).Error(), error);
    const TResult<int> copy = value;
    EXPECT_EQ(copy.Value(), 7);
    TResult<std::unique_ptr<int>> owned(std::make_unique<int>(9));
    auto moved = std::move(owned).Value();
    ASSERT_NE(moved, nullptr);
    EXPECT_EQ(*moved, 9);
}

}
}
