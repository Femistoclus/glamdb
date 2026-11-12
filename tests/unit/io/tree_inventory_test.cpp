#include "core/io/local_fs.h"
#include "core/io/tree_inventory.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <string>
#include <sys/stat.h>

namespace NGlamDB::NIO {
namespace {

#ifdef __linux__

class TTreeInventoryTest : public testing::Test
{
protected:
    static constexpr auto Unlimited = std::numeric_limits<std::uint64_t>::max();
    TPosixLocalFS Fs_;
    std::filesystem::path Path_;
    std::filesystem::path Outside_;
    THandle Root_;
    std::shared_ptr<ITreeInventory> Inventory_;
    std::size_t FullCalls_ = 0;
    std::error_code FullError_;
    std::function<void()> AfterFullMeasure_;

    void SetUp() override
    {
        auto name = (std::filesystem::temp_directory_path() / "glamdb-inventory-XXXXXX").string();
        ASSERT_NE(::mkdtemp(name.data()), nullptr);
        Path_ = std::filesystem::canonical(name);
        auto root = Fs_.OpenDir(Path_);
        ASSERT_TRUE(root) << root.Error();
        Root_ = std::move(root).Value();
        auto inventory = Fs_.TrackTree(Root_);
        if (!inventory && inventory.Error() == std::errc::not_supported) {
            GTEST_SKIP() << "Incremental inventory is unsupported on this filesystem.";
        }
        ASSERT_TRUE(inventory) << inventory.Error();
        Inventory_ = std::move(inventory).Value();
    }

    void TearDown() override
    {
        Inventory_.reset();
        Root_.reset();
        std::error_code error;
        std::filesystem::remove_all(Path_, error);
        EXPECT_FALSE(error) << error;
        if (!Outside_.empty()) {
            std::filesystem::remove_all(Outside_, error);
            EXPECT_FALSE(error) << error;
        }
    }

    void WriteFile(const std::filesystem::path& path, std::string_view contents = {})
    {
        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(file.is_open());
        file.write(contents.data(), static_cast<std::streamsize>(contents.size()));
        file.close();
        ASSERT_TRUE(file.good());
    }

    TResult<std::uint64_t> Measure(std::uint64_t limit = Unlimited,
        std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max(),
        const std::function<bool()>& cancelled = {})
    {
        return Inventory_->Measure(
            [&]() -> TResult<std::uint64_t> {
                ++FullCalls_;
                if (FullError_) {
                    return FullError_;
                }
                auto measured = Fs_.MeasureTree(Root_, limit, deadline, cancelled);
                if (AfterFullMeasure_) {
                    AfterFullMeasure_();
                }
                return measured;
            },
            limit,
            deadline,
            cancelled);
    }

    std::uint64_t PhysicalBytes() const
    {
        auto bytes = std::uint64_t{0};
        const auto add = [&](const std::filesystem::path& path) {
            struct stat info
            { };
            EXPECT_EQ(::stat(path.c_str(), &info), 0);
            const auto allocated = static_cast<std::uint64_t>(info.st_blocks) * 512;
            bytes += S_ISREG(info.st_mode) ? std::max(allocated, static_cast<std::uint64_t>(info.st_size)) : allocated;
        };
        add(Path_);
        for (const auto& entry : std::filesystem::recursive_directory_iterator(Path_)) {
            add(entry.path());
        }
        return bytes;
    }
};

TEST_F(TTreeInventoryTest, ReusesUnchangedBaselineAcrossCollectionsAndOrphans)
{
    for (const auto* directory : {"first", "second", "orphans"}) {
        std::filesystem::create_directory(Path_ / directory);
        for (int index = 0; index < 100; ++index) {
            WriteFile(Path_ / directory / std::to_string(index), index % 2 == 0 ? "x" : "");
        }
    }
    const auto expected = PhysicalBytes();
    for (int index = 0; index < 5; ++index) {
        const auto measured = Measure();
        ASSERT_TRUE(measured) << measured.Error();
        EXPECT_EQ(measured.Value(), expected);
    }
    EXPECT_EQ(FullCalls_, 1U);
}

TEST_F(TTreeInventoryTest, CountsNewFilesAndDirectoryBlocksWithoutAnotherWalk)
{
    std::filesystem::create_directory(Path_ / "data");
    ASSERT_TRUE(Measure());
    for (int index = 0; index < 200; ++index) {
        WriteFile(Path_ / "data" / std::to_string(index), index % 2 == 0 ? "orphan" : "");
    }
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_GE(measured.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 1U);
    const auto unchanged = Measure();
    ASSERT_TRUE(unchanged);
    EXPECT_EQ(unchanged.Value(), measured.Value());
    EXPECT_EQ(FullCalls_, 1U);
}

TEST_F(TTreeInventoryTest, DetectsExistingSparseFileGrowthAtNextAdmission)
{
    WriteFile(Path_ / "orphan");
    ASSERT_TRUE(Measure());
    std::filesystem::resize_file(Path_ / "orphan", 4U << 20);
    const auto grown = Measure();
    ASSERT_TRUE(grown) << grown.Error();
    EXPECT_GE(grown.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 1U);
    const auto rejected = Measure(PhysicalBytes() - 1);
    EXPECT_EQ(rejected.Error(), std::errc::no_space_on_device);
    EXPECT_EQ(FullCalls_, 2U);
}

TEST_F(TTreeInventoryTest, RefreshesConservativeChargesBeforeRefusingAfterDeletion)
{
    WriteFile(Path_ / "superseded", std::string(65536, 'x'));
    ASSERT_TRUE(Measure());
    ASSERT_TRUE(std::filesystem::remove(Path_ / "superseded"));
    const auto expected = PhysicalBytes();
    const auto measured = Measure(expected);
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_EQ(measured.Value(), expected);
    EXPECT_EQ(FullCalls_, 2U);
}

TEST_F(TTreeInventoryTest, RepeatedFileReplacementDoesNotCauseFalseCapacityRefusal)
{
    WriteFile(Path_ / "orphan", std::string(65536, 'a'));
    ASSERT_TRUE(Measure());
    for (const char fill : {'b', 'c', 'd'}) {
        WriteFile(Path_ / "orphan", std::string(65536, fill));
        const auto expected = PhysicalBytes();
        const auto measured = Measure(expected);
        ASSERT_TRUE(measured) << measured.Error();
        EXPECT_EQ(measured.Value(), expected);
    }
    EXPECT_EQ(FullCalls_, 4U);
}

TEST_F(TTreeInventoryTest, OverflowingConservativeChargeRebuildsInsteadOfWrapping)
{
    const auto baseline = Inventory_->Measure(
        [&]() -> TResult<std::uint64_t> {
            ++FullCalls_;
            return Unlimited;
        },
        Unlimited);
    ASSERT_TRUE(baseline);
    WriteFile(Path_ / "orphan", "x");
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_GE(measured.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 2U);
}

TEST_F(TTreeInventoryTest, RefreshesAfterPopulatedDirectoryIsMovedIntoWarehouse)
{
    Outside_ = Path_.parent_path() / (Path_.filename().string() + "-incoming");
    ASSERT_TRUE(std::filesystem::create_directory(Outside_));
    WriteFile(Outside_ / "unwatched", std::string(65536, 'x'));
    ASSERT_TRUE(Measure());
    std::filesystem::rename(Outside_, Path_ / "incoming");
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_GE(measured.Value(), PhysicalBytes());
    EXPECT_GE(FullCalls_, 2U);
    std::filesystem::resize_file(Path_ / "incoming/unwatched", 1U << 20);
    const auto grown = Measure();
    ASSERT_TRUE(grown) << grown.Error();
    EXPECT_GE(grown.Value(), PhysicalBytes());
}

TEST_F(TTreeInventoryTest, PreservesEventsArrivingDuringBaselineMeasurement)
{
    AfterFullMeasure_ = [&] {
        WriteFile(Path_ / "late-orphan", std::string(65536, 'x'));
    };
    ASSERT_TRUE(Measure());
    AfterFullMeasure_ = {};
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_GE(measured.Value(), PhysicalBytes());
}

TEST_F(TTreeInventoryTest, RebuildsWhenPendingChangesExceedTheEventBudget)
{
    ASSERT_TRUE(Measure());
    for (int index = 0; index < 5000; ++index) {
        WriteFile(Path_ / std::to_string(index));
    }
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_GE(measured.Value(), PhysicalBytes());
    EXPECT_GE(FullCalls_, 2U);
}

TEST_F(TTreeInventoryTest, FallsBackWhenDirectoryWatchBudgetIsExceeded)
{
    for (int index = 0; index < 1024; ++index) {
        std::filesystem::create_directory(Path_ / std::to_string(index));
    }
    const auto baseline = Measure();
    ASSERT_TRUE(baseline) << baseline.Error();
    EXPECT_EQ(baseline.Value(), PhysicalBytes());
    WriteFile(Path_ / "1023/orphan", std::string(32768, 'x'));
    const auto grown = Measure();
    ASSERT_TRUE(grown) << grown.Error();
    EXPECT_EQ(grown.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 2U);
}

TEST_F(TTreeInventoryTest, PreservesTheFullWalkDepthLimit)
{
    auto directory = Path_;
    for (int depth = 0; depth < 64; ++depth) {
        directory /= "d";
        std::filesystem::create_directory(directory);
    }
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_EQ(measured.Value(), PhysicalBytes());
    std::filesystem::create_directory(directory / "d");
    EXPECT_EQ(Measure().Error(), std::errc::value_too_large);
}

TEST_F(TTreeInventoryTest, InvalidationRebuildsAllPhysicalUsage)
{
    ASSERT_TRUE(Measure());
    WriteFile(Path_ / "incomplete", std::string(32768, 'x'));
    Inventory_->Invalidate();
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_EQ(measured.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 2U);
}

TEST_F(TTreeInventoryTest, FailedRebuildCannotReuseOldBytes)
{
    ASSERT_TRUE(Measure());
    WriteFile(Path_ / "incomplete", std::string(32768, 'x'));
    Inventory_->Invalidate();
    FullError_ = std::make_error_code(std::errc::io_error);
    EXPECT_EQ(Measure().Error(), std::errc::io_error);
    FullError_.clear();
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_GE(measured.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 3U);
}

TEST_F(TTreeInventoryTest, ExpiredDeadlineRefusesInventoryBeforeStartingTheBaseline)
{
    EXPECT_EQ(Measure(Unlimited, std::chrono::steady_clock::now()).Error(), std::errc::timed_out);
    EXPECT_EQ(FullCalls_, 0U);
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_EQ(measured.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 1U);
}

TEST_F(TTreeInventoryTest, ExpiredDeadlineDoesNotLosePendingGrowth)
{
    WriteFile(Path_ / "growing");
    ASSERT_TRUE(Measure());
    std::filesystem::resize_file(Path_ / "growing", 1U << 20);
    EXPECT_EQ(Measure(Unlimited, std::chrono::steady_clock::now()).Error(), std::errc::timed_out);
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_GE(measured.Value(), PhysicalBytes());
}

TEST_F(TTreeInventoryTest, FullWalkHonorsAnExpiredDeadline)
{
    WriteFile(Path_ / "orphan", "x");
    EXPECT_EQ(Fs_.MeasureTree(Root_, Unlimited, std::chrono::steady_clock::now()).Error(), std::errc::timed_out);
    const auto measured = Fs_.MeasureTree(Root_, Unlimited);
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_EQ(measured.Value(), PhysicalBytes());
}

TEST_F(TTreeInventoryTest, FullWalkPollsCancellationDuringTraversal)
{
    for (int index = 0; index < 200; ++index) {
        WriteFile(Path_ / std::to_string(index));
    }
    int polls = 0;
    const auto stopped = Fs_.MeasureTree(Root_, Unlimited, std::chrono::steady_clock::time_point::max(), [&] {
        return ++polls >= 3;
    });
    EXPECT_EQ(stopped.Error(), std::errc::operation_canceled);
    EXPECT_EQ(polls, 3);
    const auto measured = Fs_.MeasureTree(Root_, Unlimited);
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_EQ(measured.Value(), PhysicalBytes());
}

TEST_F(TTreeInventoryTest, CancelledDiscoveryBaselineAndReplayRequireFreshBaselines)
{
    for (int index = 0; index < 200; ++index) {
        WriteFile(Path_ / std::to_string(index), "x");
    }
    int polls = 0;
    EXPECT_EQ(Measure(Unlimited,
                  std::chrono::steady_clock::time_point::max(),
                  [&] {
                      return ++polls >= 4;
                  })
                  .Error(),
        std::errc::operation_canceled);
    EXPECT_EQ(FullCalls_, 0U);
    ASSERT_TRUE(Measure());
    EXPECT_EQ(FullCalls_, 1U);
    for (int index = 0; index < 100; ++index) {
        std::filesystem::resize_file(Path_ / std::to_string(index), 65536);
    }
    polls = 0;
    EXPECT_EQ(Measure(Unlimited,
                  std::chrono::steady_clock::time_point::max(),
                  [&] {
                      return ++polls >= 10;
                  })
                  .Error(),
        std::errc::operation_canceled);
    EXPECT_EQ(FullCalls_, 1U);
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_GE(measured.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 2U);
    Inventory_->Invalidate();
    bool stopped = false;
    AfterFullMeasure_ = [&] {
        stopped = true;
    };
    EXPECT_EQ(Measure(Unlimited,
                  std::chrono::steady_clock::time_point::max(),
                  [&] {
                      return stopped;
                  })
                  .Error(),
        std::errc::operation_canceled);
    EXPECT_EQ(FullCalls_, 3U);
    AfterFullMeasure_ = {};
    stopped = false;
    const auto rebuilt = Measure();
    ASSERT_TRUE(rebuilt) << rebuilt.Error();
    EXPECT_GE(rebuilt.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 4U);
}

TEST_F(TTreeInventoryTest, RequiresFullWalkForHardLinks)
{
    WriteFile(Path_ / "orphan", "x");
    std::filesystem::create_hard_link(Path_ / "orphan", Path_ / "alias");
    for (int index = 0; index < 3; ++index) {
        const auto measured = Measure();
        ASSERT_TRUE(measured) << measured.Error();
        EXPECT_EQ(measured.Value(), PhysicalBytes());
    }
    EXPECT_EQ(FullCalls_, 3U);
}

TEST_F(TTreeInventoryTest, DetectsHardLinkAddedAfterBaseline)
{
    WriteFile(Path_ / "orphan", "x");
    ASSERT_TRUE(Measure());
    std::filesystem::create_hard_link(Path_ / "orphan", Path_ / "alias");
    const auto linked = Measure();
    ASSERT_TRUE(linked) << linked.Error();
    EXPECT_EQ(linked.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 2U);
    Inventory_->Invalidate();
    std::filesystem::resize_file(Path_ / "alias", 1U << 20);
    const auto grown = Measure();
    ASSERT_TRUE(grown) << grown.Error();
    EXPECT_EQ(grown.Value(), PhysicalBytes());
    EXPECT_EQ(FullCalls_, 3U);
}

TEST_F(TTreeInventoryTest, RejectsSymlinkIntroducedAfterBaseline)
{
    ASSERT_TRUE(Measure());
    std::filesystem::create_directory_symlink(Path_, Path_ / "loop");
    EXPECT_EQ(Measure().Error(), std::errc::invalid_argument);
    ASSERT_TRUE(std::filesystem::remove(Path_ / "loop"));
    const auto measured = Measure();
    ASSERT_TRUE(measured) << measured.Error();
    EXPECT_GE(measured.Value(), PhysicalBytes());
}

#else

TEST(TreeInventory, UnsupportedPlatformRequiresFullMeasurement)
{
    TPosixLocalFS fs;
    const auto root = fs.OpenDir(std::filesystem::canonical(std::filesystem::temp_directory_path()));
    ASSERT_TRUE(root);
    EXPECT_EQ(fs.TrackTree(root.Value()).Error(), std::errc::not_supported);
}

#endif

} // namespace
} // namespace NGlamDB::NIO
