#include "core/io/store_dirs.h"
#include "core/storage/capacity.h"
#include "tests/support/fault_fs.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <limits>

namespace NGlamDB::NStorage {
namespace {

constexpr std::array UsageFields{&TStorageUsage::RootBytes,
    &TStorageUsage::Snapshots,
    &TStorageUsage::SnapshotLogEntries,
    &TStorageUsage::DataFiles,
    &TStorageUsage::Manifests,
    &TStorageUsage::ManifestBytes,
    &TStorageUsage::ManifestListBytes,
    &TStorageUsage::FooterBytes};

TEST(StorageCapacity, AcceptsExactLimitsAndRejectsEveryLimitPlusOne)
{
    const TStorageLimits limits;
    EXPECT_FALSE(CheckStorageCapacity(limits.Store, limits));
    for (const auto field : UsageFields) {
        auto usage = limits.Store;
        ++(usage.*field);
        EXPECT_EQ(CheckStorageCapacity(usage, limits), std::errc::no_space_on_device);
    }
}

TEST(StorageCapacity, ReplacesOneCollectionWithoutLosingOtherCollections)
{
    TStorageUsage previous;
    previous.RootBytes = 100;
    previous.DataFiles = 2;
    auto next = previous;
    next.RootBytes = 90;
    next.DataFiles = 3;
    auto total = previous;
    total.RootBytes = 400;
    const auto result = ReplaceUsage(total, {.Previous = previous, .Next = next});
    ASSERT_TRUE(result);
    EXPECT_EQ(result.Value().RootBytes, 390);
    EXPECT_EQ(result.Value().DataFiles, 3);
    EXPECT_EQ(total.RootBytes, 400);
}

TEST(StorageCapacity, DetectsOverflowAndInconsistentStoreCountersInEveryField)
{
    for (const auto field : UsageFields) {
        TStorageUsage one;
        one.*field = 1;
        EXPECT_EQ(ReplaceUsage({}, {.Previous = one}).Error(), std::errc::invalid_argument);
        TStorageUsage maximum;
        maximum.*field = std::numeric_limits<std::uint64_t>::max();
        EXPECT_EQ(ReplaceUsage(maximum, {.Next = one}).Error(), std::errc::value_too_large);
        const auto replaced = ReplaceUsage(maximum, {.Previous = maximum, .Next = one});
        ASSERT_TRUE(replaced);
        EXPECT_EQ(replaced.Value(), one);
    }
}

TEST(StorageCapacity, ReservesSpaceForAttemptJournalAndVolumeHeadroom)
{
    TStorageLimits limits;
    limits.WarehouseBytes = 200;
    limits.MinimumFreeBytes = 10;
    limits.AttemptReserveBytes = 20;
    limits.JournalReserveBytes = 30;
    EXPECT_FALSE(CheckDiskReserve({1000, 100}, 180, limits));
    EXPECT_EQ(CheckDiskReserve({1000, 99}, 180, limits), std::errc::no_space_on_device);
    EXPECT_EQ(CheckDiskReserve({1000, 100}, 181, limits), std::errc::no_space_on_device);
    EXPECT_EQ(CheckDiskReserve({1000, 100}, 201, limits), std::errc::no_space_on_device);
    EXPECT_EQ(CheckDiskReserve({1000, 1001}, 0, limits), std::errc::invalid_argument);
    EXPECT_EQ(CheckDiskReserve({1001, 100}, 0, limits), std::errc::no_space_on_device);
    EXPECT_FALSE(CheckDiskReserve({1001, 101}, 0, limits));
    limits.JournalReserveBytes = std::numeric_limits<std::uint64_t>::max();
    EXPECT_EQ(CheckDiskReserve({1000, 1000}, 0, limits), std::errc::no_space_on_device);
}

class TWriteCapacityTest : public ::testing::Test
{
protected:
    std::filesystem::path Path_;
    std::shared_ptr<NIO::TPosixLocalFS> Local_ = std::make_shared<NIO::TPosixLocalFS>();
    std::shared_ptr<NTesting::TFaultFS> Fs_ = std::make_shared<NTesting::TFaultFS>(*Local_);
    std::shared_ptr<NIO::TStoreDirs> Directories_;

    void SetUp() override
    {
        auto path = (std::filesystem::temp_directory_path() / "glamdb-capacity-XXXXXX").string();
        ASSERT_NE(::mkdtemp(path.data()), nullptr);
        Path_ = std::filesystem::canonical(path);
        auto directories = NIO::TStoreDirs::Create(Fs_, Path_ / "store", std::make_shared<NIO::TWriteLatch>());
        ASSERT_TRUE(directories);
        Directories_ = std::move(directories).Value();
    }

    void TearDown() override
    {
        Directories_.reset();
        std::error_code error;
        std::filesystem::remove_all(Path_, error);
        EXPECT_FALSE(error);
    }

    void ExpectOnlyReadOperations(std::size_t start)
    {
        const auto events = Fs_->Events();
        for (std::size_t index = start; index < events.size(); ++index) {
            const auto op = events[index].Op;
            EXPECT_TRUE(op == NTesting::EIOOp::OpenDir || op == NTesting::EIOOp::Stat ||
                op == NTesting::EIOOp::MeasureTree || op == NTesting::EIOOp::StatFs);
        }
    }
};

TEST_F(TWriteCapacityTest, IncludesOrphansAndReservesTheWholeNextAttemptBeforeWrites)
{
    auto orphan = Fs_->CreateExclusive(Directories_->Warehouse(), "unreferenced.parquet");
    ASSERT_TRUE(orphan);
    const std::string content = "orphan";
    ASSERT_TRUE(Fs_->Write(orphan.Value(), std::as_bytes(std::span(content))));
    ASSERT_FALSE(Fs_->Close(orphan.Value()));
    const auto usage = Fs_->MeasureTree(Directories_->Warehouse(), std::numeric_limits<std::uint64_t>::max());
    ASSERT_TRUE(usage);
    ASSERT_GT(usage.Value(), content.size());
    TStorageLimits limits;
    limits.WarehouseBytes = usage.Value() + limits.AttemptReserveBytes;
    const auto start = Fs_->Events().size();
    EXPECT_FALSE(CheckWriteCapacity(*Directories_, limits));
    EXPECT_EQ(Fs_->Events().back().Op, NTesting::EIOOp::StatFs);
    EXPECT_EQ(Fs_->Events()[start + 2].Op, NTesting::EIOOp::MeasureTree);
    EXPECT_EQ(Fs_->Events()[start + 2].Bytes, usage.Value());
    --limits.WarehouseBytes;
    EXPECT_EQ(CheckWriteCapacity(*Directories_, limits), std::errc::no_space_on_device);
    limits.WarehouseBytes = usage.Value() - 1;
    EXPECT_EQ(CheckWriteCapacity(*Directories_, limits), std::errc::no_space_on_device);
    EXPECT_EQ(Fs_->Events().back().Op, NTesting::EIOOp::MeasureTree);
    ExpectOnlyReadOperations(start);
    EXPECT_FALSE(Directories_->Latch()->IsSet());
}

TEST_F(TWriteCapacityTest, PropagatesMeasurementSpaceAndReserveFailuresWithoutWrites)
{
    const auto start = Fs_->Events().size();
    for (const auto op : {NTesting::EIOOp::MeasureTree, NTesting::EIOOp::StatFs}) {
        Fs_->AddRule({op, "*", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
        EXPECT_EQ(CheckWriteCapacity(*Directories_), std::errc::io_error);
        EXPECT_EQ(Fs_->Events().back().Op, op);
    }
    TStorageLimits limits;
    limits.MinimumFreeBytes = std::numeric_limits<std::uint64_t>::max();
    EXPECT_EQ(CheckWriteCapacity(*Directories_, limits), std::errc::no_space_on_device);
    ExpectOnlyReadOperations(start);
    EXPECT_FALSE(Directories_->Latch()->IsSet());
}

TEST_F(TWriteCapacityTest, RejectsReplacedRootBeforeWalkingWarehouse)
{
    std::filesystem::rename(Path_ / "store", Path_ / "old");
    ASSERT_TRUE(std::filesystem::create_directory(Path_ / "store"));
    const auto start = Fs_->Events().size();
    EXPECT_EQ(CheckWriteCapacity(*Directories_), std::errc::state_not_recoverable);
    EXPECT_EQ(Fs_->Events().size(), start + 2);
    EXPECT_TRUE(Directories_->Latch()->IsSet());
    ExpectOnlyReadOperations(start);
}

}
}
