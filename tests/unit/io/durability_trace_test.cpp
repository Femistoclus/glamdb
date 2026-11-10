#include "tests/support/durability_trace.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace NGlamDB::NTesting {
namespace {

class TDurabilityTraceTest : public ::testing::Test
{
protected:
    TDurabilityTrace Trace_{{"/store"}};
    std::uint64_t Seq_ = 0;

    std::optional<std::string> Add(EIOOp op, std::string path, std::uint64_t bytes = 0, std::error_code error = {})
    {
        return Trace_.Apply({++Seq_, op, std::move(path), {}, bytes, error, {}});
    }
    std::optional<std::string> Publish(std::vector<std::string> paths = {"/store/file"})
    {
        return Trace_.Apply({++Seq_, EIOOp::Publish, {}, {}, 0, {}, std::move(paths)});
    }
    void WriteFile(const std::string& path = "/store/file")
    {
        ASSERT_FALSE(Add(EIOOp::CreateExclusive, path));
        ASSERT_FALSE(Add(EIOOp::Write, path, 4));
    }
};

TEST_F(TDurabilityTraceTest, TracksEveryPrefixAndRequiresBothSyncBoundaries)
{
    WriteFile();
    EXPECT_TRUE(Trace_.DurableFiles().empty());
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    EXPECT_TRUE(Trace_.DurableFiles().empty());
    ASSERT_FALSE(Add(EIOOp::Close, "/store/file"));
    EXPECT_TRUE(Trace_.DurableFiles().empty());
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    EXPECT_EQ(Trace_.DurableFiles(), std::set<std::string>{"/store/file"});
    EXPECT_FALSE(Publish());
}

TEST_F(TDurabilityTraceTest, RejectsPublicationWithoutFileSync)
{
    WriteFile();
    ASSERT_FALSE(Add(EIOOp::Close, "/store/file"));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    EXPECT_TRUE(Publish());
}

TEST_F(TDurabilityTraceTest, RejectsPublicationWithoutDirectorySync)
{
    WriteFile();
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    ASSERT_FALSE(Add(EIOOp::Close, "/store/file"));
    EXPECT_TRUE(Publish());
}

TEST_F(TDurabilityTraceTest, LaterSyncCannotRetroactivelyValidatePublication)
{
    WriteFile();
    EXPECT_TRUE(Publish());
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    EXPECT_TRUE(Publish());
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    EXPECT_FALSE(Publish());
}

TEST_F(TDurabilityTraceTest, FileMutationInvalidatesSyncButZeroProgressDoesNot)
{
    WriteFile();
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    ASSERT_FALSE(Add(EIOOp::Write, "/store/file", 0));
    EXPECT_FALSE(Publish());
    ASSERT_FALSE(Add(EIOOp::Write, "/store/file", 1));
    EXPECT_TRUE(Publish());
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    EXPECT_FALSE(Publish());
}

TEST_F(TDurabilityTraceTest, EarlierDirectorySyncDoesNotPersistLaterNames)
{
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    WriteFile();
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    EXPECT_TRUE(Publish());
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    EXPECT_FALSE(Publish());
}

TEST_F(TDurabilityTraceTest, RequiresNewDirectoryAndEveryAncestorToBeSynced)
{
    ASSERT_FALSE(Add(EIOOp::MakeDir, "/store/collection"));
    ASSERT_FALSE(Add(EIOOp::MakeDir, "/store/collection/data"));
    WriteFile("/store/collection/data/file");
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/collection/data/file"));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store/collection/data"));
    EXPECT_TRUE(Publish({"/store/collection/data/file"}));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store/collection"));
    EXPECT_TRUE(Publish({"/store/collection/data/file"}));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    EXPECT_FALSE(Publish({"/store/collection/data/file"}));
}

TEST_F(TDurabilityTraceTest, ExistingDirectoryIsNotAnImplicitDurableBaseline)
{
    ASSERT_FALSE(Add(EIOOp::MakeDir, "/store/data", 0, std::make_error_code(std::errc::file_exists)));
    ASSERT_FALSE(Add(EIOOp::OpenDir, "/store/data"));
    WriteFile("/store/data/file");
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/data/file"));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store/data"));
    EXPECT_TRUE(Publish({"/store/data/file"}));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    EXPECT_FALSE(Publish({"/store/data/file"}));
}

TEST_F(TDurabilityTraceTest, FailedSyncDoesNotEstablishDurability)
{
    WriteFile();
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file", 0, std::make_error_code(std::errc::io_error)));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    EXPECT_TRUE(Publish());
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store", 0, std::make_error_code(std::errc::io_error)));
    EXPECT_TRUE(Publish());
}

TEST_F(TDurabilityTraceTest, ReadsAndReaderCloseDoNotChangeDurableBytes)
{
    WriteFile();
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    for (auto op : {EIOOp::OpenRead, EIOOp::PRead, EIOOp::Stat, EIOOp::Close}) {
        ASSERT_FALSE(Add(op, "/store/file", 0, std::make_error_code(std::errc::io_error)));
    }
    EXPECT_FALSE(Publish());
}

TEST(DurabilityTrace, IoErrorsRemainUncertainAfterLaterSuccessfulSync)
{
    for (const auto error : {std::errc::io_error, std::errc::read_only_file_system}) {
        for (const auto op : {EIOOp::Write, EIOOp::SyncFile, EIOOp::SyncDir}) {
            SCOPED_TRACE(static_cast<int>(op));
            TDurabilityTrace trace({"/store"});
            EXPECT_FALSE(trace.Apply({1, EIOOp::CreateExclusive, "/store/file", {}, 0, {}, {}}));
            EXPECT_FALSE(trace.Apply({2, EIOOp::Write, "/store/file", {}, 1, {}, {}}));
            EXPECT_FALSE(trace.Apply({3, EIOOp::SyncFile, "/store/file", {}, 0, {}, {}}));
            EXPECT_FALSE(trace.Apply({4, EIOOp::SyncDir, "/store", {}, 0, {}, {}}));
            EXPECT_FALSE(trace.Apply(
                {5, op, op == EIOOp::SyncDir ? "/store" : "/store/file", {}, 0, std::make_error_code(error), {}}));
            EXPECT_FALSE(trace.Apply({6, EIOOp::SyncFile, "/store/file", {}, 0, {}, {}}));
            EXPECT_FALSE(trace.Apply({7, EIOOp::SyncDir, "/store", {}, 0, {}, {}}));
            EXPECT_TRUE(trace.Apply({8, EIOOp::Publish, {}, {}, 0, {}, {"/store/file"}}));
            EXPECT_TRUE(trace.DurableFiles().empty());
        }
    }
}

TEST_F(TDurabilityTraceTest, ChecksEveryReferenceAndRejectsUnknownFiles)
{
    WriteFile();
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    EXPECT_FALSE(Publish({}));
    EXPECT_FALSE(Publish({"/store/file", "/store/file"}));
    EXPECT_TRUE(Publish({"/store/file", "/store/missing"}));
    EXPECT_TRUE(Publish({"/store/../file"}));
}

TEST_F(TDurabilityTraceTest, HandlesFailedCreateAndWriteConservatively)
{
    ASSERT_FALSE(Add(EIOOp::CreateExclusive, "/store/file", 0, std::make_error_code(std::errc::no_space_on_device)));
    EXPECT_TRUE(Add(EIOOp::SyncFile, "/store/file"));
    WriteFile();
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    ASSERT_FALSE(Add(EIOOp::Write, "/store/file", 0, std::make_error_code(std::errc::interrupted)));
    EXPECT_TRUE(Publish());
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    EXPECT_FALSE(Publish());
}

TEST_F(TDurabilityTraceTest, RejectsMalformedTraceEvents)
{
    EXPECT_TRUE(Add(EIOOp::Write, "/store/missing", 1));
    EXPECT_TRUE(Add(EIOOp::SyncDir, "/store/missing"));
    EXPECT_TRUE(Add(EIOOp::CreateExclusive, "relative"));
    EXPECT_TRUE(Add(EIOOp::CreateExclusive, "/store/../file"));
    EXPECT_TRUE(Add(EIOOp::CreateExclusive, std::string("/store/a\0b", 10)));
    WriteFile();
    EXPECT_TRUE(Add(EIOOp::CreateExclusive, "/store/file"));
    EXPECT_TRUE(Trace_.Apply({Seq_, EIOOp::SyncFile, "/store/file", {}, 0, {}, {}}));
}

TEST_F(TDurabilityTraceTest, RejectsMissingEventsIncludingMutationsAfterSync)
{
    EXPECT_TRUE(Trace_.Apply({2, EIOOp::OpenDir, "/store", {}, 0, {}, {}}));
    WriteFile();
    ASSERT_FALSE(Add(EIOOp::SyncFile, "/store/file"));
    ASSERT_FALSE(Add(EIOOp::SyncDir, "/store"));
    EXPECT_TRUE(Trace_.Apply({Seq_ + 2, EIOOp::Publish, {}, {}, 0, {}, {"/store/file"}}));
    EXPECT_TRUE(Trace_.Apply({0, EIOOp::Publish, {}, {}, 0, {}, {}}));
}

TEST(DurabilityTrace, ExplicitBaselineAnchorsKnownDurableAncestors)
{
    TDurabilityTrace trace({"/known/deep/root"});
    EXPECT_FALSE(trace.Apply({1, EIOOp::CreateExclusive, "/known/deep/root/file", {}, 0, {}, {}}));
    EXPECT_FALSE(trace.Apply({2, EIOOp::SyncFile, "/known/deep/root/file", {}, 0, {}, {}}));
    EXPECT_FALSE(trace.Apply({3, EIOOp::SyncDir, "/known/deep/root", {}, 0, {}, {}}));
    EXPECT_EQ(trace.DurableFiles(), std::set<std::string>{"/known/deep/root/file"});
    EXPECT_THROW(TDurabilityTrace({"relative"}), std::invalid_argument);
    EXPECT_THROW(TDurabilityTrace({"/a/../b"}), std::invalid_argument);
    EXPECT_THROW(TDurabilityTrace({"/store/"}), std::invalid_argument);
    EXPECT_THROW(TDurabilityTrace({std::string("/a\0b", 4)}), std::invalid_argument);
}

TEST(DurabilityTrace, OpeningAnUnseededRootDoesNotAssumeDurableAncestry)
{
    TDurabilityTrace trace({});
    EXPECT_FALSE(trace.Apply({1, EIOOp::OpenDir, "/store", {}, 0, {}, {}}));
    EXPECT_FALSE(trace.Apply({2, EIOOp::CreateExclusive, "/store/file", {}, 0, {}, {}}));
    EXPECT_FALSE(trace.Apply({3, EIOOp::SyncFile, "/store/file", {}, 0, {}, {}}));
    EXPECT_FALSE(trace.Apply({4, EIOOp::SyncDir, "/store", {}, 0, {}, {}}));
    EXPECT_TRUE(trace.DurableFiles().empty());
}

}
}
