#include "tests/support/durability_trace.h"
#include "tests/support/fault_fs.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <map>
#include <stdexcept>
#include <thread>

namespace NGlamDB::NTesting {
namespace {

using namespace NIO;

class TMemoryFS final : public ILocalFS
{
    struct TFile
    {
        std::vector<std::byte> Bytes;
        EFileType Type;
    };
    struct TMemoryHandle final : TFSHandle
    {
        std::shared_ptr<TFile> File;
        std::string Path;
        bool Closed = false;
    };

    std::map<std::string, std::shared_ptr<TFile>> Files_;

    TResult<THandle> Open(const std::string& path, EFileType type, bool exclusive = false)
    {
        if (exclusive && Files_.contains(path)) {
            return std::errc::file_exists;
        }
        auto& file = Files_[path];
        if (!file) {
            file = std::make_shared<TFile>(TFile{{}, type});
        }
        auto handle = std::make_shared<TMemoryHandle>();
        handle->File = file;
        handle->Path = path;
        return THandle(std::move(handle));
    }
    static std::shared_ptr<TMemoryHandle> Get(const THandle& handle)
    {
        return std::dynamic_pointer_cast<TMemoryHandle>(handle);
    }
    static std::string Child(const THandle& parent, std::string_view name)
    {
        return Get(parent)->Path + "/" + std::string(name);
    }

public:
    int CloseCalls = 0;
    int SyncCalls = 0;
    std::error_code SyncError;
    TResult<THandle> OpenDir(const std::filesystem::path& path) override
    {
        return Open(path.generic_string(), EFileType::Directory);
    }
    TResult<THandle> OpenDir(const THandle& parent, std::string_view name) override
    {
        return Open(Child(parent, name), EFileType::Directory);
    }
    std::error_code MakeDir(const THandle& parent, std::string_view name) override
    {
        return Open(Child(parent, name), EFileType::Directory, true).Error();
    }
    TResult<THandle> CreateExclusive(const THandle& parent, std::string_view name, const TIOTag&) override
    {
        return Open(Child(parent, name), EFileType::Regular, true);
    }
    TResult<THandle> OpenRead(const THandle& parent, std::string_view name) override
    {
        return Open(Child(parent, name), EFileType::Regular);
    }
    TResult<std::size_t> Write(const THandle& handle, std::span<const std::byte> bytes) override
    {
        auto& data = Get(handle)->File->Bytes;
        data.insert(data.end(), bytes.begin(), bytes.end());
        return bytes.size();
    }
    TResult<std::size_t> PRead(const THandle& handle, std::uint64_t offset, std::span<std::byte> bytes) override
    {
        auto& data = Get(handle)->File->Bytes;
        if (offset >= data.size()) {
            return std::size_t{0};
        }
        const auto count = std::min(bytes.size(), data.size() - static_cast<std::size_t>(offset));
        std::copy_n(data.begin() + static_cast<std::ptrdiff_t>(offset), count, bytes.begin());
        return count;
    }
    TResult<TFileStat> Stat(const THandle& handle) override
    {
        const auto& file = *Get(handle)->File;
        return TFileStat{1, 2, file.Bytes.size(), file.Type};
    }
    TResult<TSpaceInfo> StatFs(const THandle&) override
    {
        return TSpaceInfo{4096, 2048};
    }
    std::error_code SyncFile(const THandle&) override
    {
        ++SyncCalls;
        return SyncError;
    }
    std::error_code SyncDir(const THandle&) override
    {
        ++SyncCalls;
        return SyncError;
    }
    std::error_code Close(const THandle& handle) override
    {
        ++CloseCalls;
        auto tracked = Get(handle);
        if (tracked->Closed) {
            return std::make_error_code(std::errc::bad_file_descriptor);
        }
        tracked->Closed = true;
        return {};
    }
};

constexpr std::array Bytes{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};

class TFaultFSTest : public ::testing::Test
{
protected:
    TMemoryFS Underlying_;
    TFaultFS Fs_{Underlying_};
    THandle Root_ = Fs_.OpenDir("/store").Value();
    THandle File_ = Fs_.CreateExclusive(Root_, "file.parquet", {EFileKind::Data, "attempt-1"}).Value();
};

TEST_F(TFaultFSTest, TracesTreeMeasurementAndPreservesOptionalBackendSupport)
{
    EXPECT_EQ(Underlying_.MeasureTree({}, 10).Error(), std::errc::not_supported);
    EXPECT_EQ(Fs_.MeasureTree(Root_, 10).Error(), std::errc::not_supported);
    EXPECT_EQ(Fs_.Events().back().Op, EIOOp::MeasureTree);
    EXPECT_EQ(Fs_.Events().back().Path, "/store");
    EXPECT_NE(Fs_.JsonLines().find("MeasureTree"), std::string::npos);
    Fs_.AddRule({EIOOp::MeasureTree, "/store", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    EXPECT_EQ(Fs_.MeasureTree(Root_, 10).Error(), std::errc::io_error);
    EXPECT_EQ(Fs_.MeasureTree(Root_, 10).Error(), std::errc::not_supported);
    EXPECT_EQ(Fs_.MeasureTree({}, 10).Error(), std::errc::bad_file_descriptor);
}

TEST_F(TFaultFSTest, ForwardsAllOperationsAndPreservesTags)
{
    EXPECT_FALSE(Fs_.MakeDir(Root_, "metadata"));
    auto directory = Fs_.OpenDir(Root_, "metadata");
    ASSERT_TRUE(directory);
    auto written = Fs_.Write(File_, Bytes);
    ASSERT_TRUE(written);
    EXPECT_EQ(written.Value(), Bytes.size());
    EXPECT_EQ(Fs_.Stat(File_).Value().Size, Bytes.size());
    EXPECT_EQ(Fs_.StatFs(Root_).Value().Available, 2048U);
    EXPECT_FALSE(Fs_.SyncFile(File_));
    EXPECT_FALSE(Fs_.Close(File_));
    EXPECT_FALSE(Fs_.SyncDir(Root_));
    auto reader = Fs_.OpenRead(Root_, "file.parquet");
    ASSERT_TRUE(reader);
    std::array<std::byte, 5> output{};
    auto read = Fs_.PRead(reader.Value(), 2, output);
    ASSERT_TRUE(read);
    EXPECT_EQ(read.Value(), 2U);
    EXPECT_EQ(output[0], Bytes[2]);
    EXPECT_EQ(output[1], Bytes[3]);
    EXPECT_FALSE(Fs_.Close(reader.Value()));
    EXPECT_FALSE(Fs_.Close(directory.Value()));
    const auto events = Fs_.Events();
    for (std::size_t i = 0; i < events.size(); ++i) {
        EXPECT_EQ(events[i].Seq, i + 1);
        if (events[i].Path == "/store/file.parquet") {
            EXPECT_EQ(events[i].Tag.Kind, EFileKind::Data);
            EXPECT_EQ(events[i].Tag.AttemptId, "attempt-1");
        }
    }
    EXPECT_EQ(Underlying_.SyncCalls, 2);
    EXPECT_EQ(Underlying_.CloseCalls, 3);
}

TEST_F(TFaultFSTest, FaultsMatchOnlySelectedPathAndOccurrence)
{
    Fs_.AddRule({EIOOp::Write, "*/file.?arquet", 2, 1, std::make_error_code(std::errc::no_space_on_device), {}});
    auto other = Fs_.CreateExclusive(Root_, "other.parquet").Value();
    EXPECT_TRUE(Fs_.Write(other, Bytes));
    EXPECT_TRUE(Fs_.Write(File_, Bytes));
    EXPECT_EQ(Fs_.Write(File_, Bytes).Error(), std::errc::no_space_on_device);
    EXPECT_TRUE(Fs_.Write(File_, Bytes));
    EXPECT_EQ(Fs_.Stat(File_).Value().Size, Bytes.size() * 2);
    EXPECT_EQ(Fs_.Events()[5].Bytes, 0U);
}

TEST_F(TFaultFSTest, RepeatsInterruptionsThenMakesShortAndZeroProgress)
{
    Fs_.AddRule({EIOOp::Write, "*", 1, 3, std::make_error_code(std::errc::interrupted), {}});
    Fs_.AddRule({EIOOp::Write, "*", 4, 1, {}, 2});
    Fs_.AddRule({EIOOp::Write, "*", 5, 1, {}, 0});
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(Fs_.Write(File_, Bytes).Error(), std::errc::interrupted);
    }
    EXPECT_EQ(Fs_.Write(File_, Bytes).Value(), 2U);
    EXPECT_EQ(Fs_.Write(File_, Bytes).Value(), 0U);
    EXPECT_EQ(Fs_.Write(File_, Bytes).Value(), Bytes.size());
    EXPECT_EQ(Fs_.Stat(File_).Value().Size, 6U);
    auto reader = Fs_.OpenRead(Root_, "file.parquet").Value();
    Fs_.AddRule({EIOOp::PRead, "*", 1, 1, {}, 1});
    std::array<std::byte, 4> output{};
    EXPECT_EQ(Fs_.PRead(reader, 1, output).Value(), 1U);
    EXPECT_EQ(output[0], Bytes[1]);
}

TEST_F(TFaultFSTest, FirstMatchingRuleWinsWithoutDelayingOtherSchedules)
{
    Fs_.AddRule({EIOOp::Write, "*", 1, 1, std::make_error_code(std::errc::interrupted), {}});
    Fs_.AddRule({EIOOp::Write, "*", 1, 2, {}, 1});
    EXPECT_EQ(Fs_.Write(File_, Bytes).Error(), std::errc::interrupted);
    EXPECT_EQ(Fs_.Write(File_, Bytes).Value(), 1U);
    EXPECT_EQ(Fs_.Write(File_, Bytes).Value(), Bytes.size());
}

TEST_F(TFaultFSTest, CloseFaultConsumesUnderlyingHandleExactlyOnce)
{
    Fs_.AddRule({EIOOp::Close, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), {}});
    EXPECT_EQ(Fs_.Close(File_), std::errc::io_error);
    EXPECT_EQ(Underlying_.CloseCalls, 1);
    EXPECT_EQ(Fs_.Close(File_), std::errc::bad_file_descriptor);
    EXPECT_EQ(Fs_.Write(File_, Bytes).Error(), std::errc::bad_file_descriptor);
    EXPECT_EQ(Underlying_.CloseCalls, 1);
}

TEST_F(TFaultFSTest, InjectsErrorsAtEveryOperationWithoutCallingSync)
{
    const auto error = std::make_error_code(std::errc::read_only_file_system);
    for (EIOOp op : {EIOOp::OpenDir,
             EIOOp::MakeDir,
             EIOOp::CreateExclusive,
             EIOOp::OpenRead,
             EIOOp::PRead,
             EIOOp::Stat,
             EIOOp::StatFs,
             EIOOp::SyncFile,
             EIOOp::SyncDir})
    {
        Fs_.AddRule({op, "*", 1, 1, error, {}});
    }
    EXPECT_EQ(Fs_.OpenDir("/another").Error(), error);
    EXPECT_EQ(Fs_.MakeDir(Root_, "dir"), error);
    EXPECT_EQ(Fs_.CreateExclusive(Root_, "new").Error(), error);
    EXPECT_EQ(Fs_.OpenRead(Root_, "file.parquet").Error(), error);
    std::array<std::byte, 1> output{};
    EXPECT_EQ(Fs_.PRead(File_, 0, output).Error(), error);
    EXPECT_EQ(Fs_.Stat(File_).Error(), error);
    EXPECT_EQ(Fs_.StatFs(Root_).Error(), error);
    EXPECT_EQ(Fs_.SyncFile(File_), error);
    EXPECT_EQ(Fs_.SyncDir(Root_), error);
    EXPECT_EQ(Underlying_.SyncCalls, 0);
    const auto events = Fs_.Events();
    for (std::size_t i = 2; i < events.size(); ++i) {
        EXPECT_EQ(events[i].Error, error);
    }
}

TEST_F(TFaultFSTest, PreservesUnderlyingErrorsAndRejectsForeignHandles)
{
    EXPECT_EQ(Fs_.CreateExclusive(Root_, "file.parquet").Error(), std::errc::file_exists);
    TFaultFS other(Underlying_);
    EXPECT_EQ(other.Stat(File_).Error(), std::errc::bad_file_descriptor);
    EXPECT_EQ(other.Close(File_), std::errc::bad_file_descriptor);
    EXPECT_EQ(other.MakeDir(Root_, "bad"), std::errc::bad_file_descriptor);
    EXPECT_EQ(other.OpenDir(Root_, "bad").Error(), std::errc::bad_file_descriptor);
    EXPECT_EQ(other.CreateExclusive(Root_, "bad").Error(), std::errc::bad_file_descriptor);
    EXPECT_EQ(other.OpenRead(Root_, "bad").Error(), std::errc::bad_file_descriptor);
    EXPECT_EQ(Fs_.Stat({}).Error(), std::errc::bad_file_descriptor);
    const auto rejected = other.Events();
    ASSERT_EQ(rejected.size(), 6U);
    const std::array operations{EIOOp::Stat,
        EIOOp::Close,
        EIOOp::MakeDir,
        EIOOp::OpenDir,
        EIOOp::CreateExclusive,
        EIOOp::OpenRead};
    for (std::size_t i = 0; i < rejected.size(); ++i) {
        EXPECT_EQ(rejected[i].Seq, i + 1);
        EXPECT_EQ(rejected[i].Op, operations[i]);
        EXPECT_EQ(rejected[i].Error, std::errc::bad_file_descriptor);
    }
}

class TAfterSyncFaultTest
    : public TFaultFSTest
    , public ::testing::WithParamInterface<EIOOp>
{ };

TEST_P(TAfterSyncFaultTest, ReportsFailureOnlyAfterTheUnderlyingSyncSucceeds)
{
    TFaultFS outer(static_cast<ILocalFS&>(Fs_));
    const auto root = outer.OpenDir("/store");
    ASSERT_TRUE(root);
    const auto file = outer.OpenRead(root.Value(), "file.parquet");
    ASSERT_TRUE(file);
    outer.AddRule({GetParam(), "*", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt, true});
    const auto sync = [&] {
        return GetParam() == EIOOp::SyncFile ? outer.SyncFile(file.Value()) : outer.SyncDir(root.Value());
    };
    EXPECT_EQ(sync(), std::errc::io_error);
    EXPECT_EQ(Underlying_.SyncCalls, 1);
    EXPECT_EQ(Fs_.Events().back().Op, GetParam());
    EXPECT_FALSE(Fs_.Events().back().Error);
    EXPECT_EQ(outer.Events().back().Error, std::errc::io_error);
    EXPECT_FALSE(sync());
    EXPECT_EQ(Underlying_.SyncCalls, 2);
}

TEST_P(TAfterSyncFaultTest, PreservesTheUnderlyingFailureWithoutRetry)
{
    Underlying_.SyncError = std::make_error_code(std::errc::no_space_on_device);
    Fs_.AddRule({GetParam(), "*", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt, true});
    const auto error = GetParam() == EIOOp::SyncFile ? Fs_.SyncFile(File_) : Fs_.SyncDir(Root_);
    EXPECT_EQ(error, Underlying_.SyncError);
    EXPECT_EQ(Fs_.Events().back().Error, Underlying_.SyncError);
    EXPECT_EQ(Underlying_.SyncCalls, 1);
}

INSTANTIATE_TEST_SUITE_P(SyncBoundaries, TAfterSyncFaultTest, ::testing::Values(EIOOp::SyncFile, EIOOp::SyncDir));

TEST(FaultRules, RejectInvalidSchedules)
{
    TMemoryFS underlying;
    TFaultFS fs(underlying);
    const auto error = std::make_error_code(std::errc::io_error);
    EXPECT_THROW(fs.AddRule({EIOOp::Write, "*", 1, 1, {}, {}}), std::invalid_argument);
    EXPECT_THROW(fs.AddRule({EIOOp::Write, "*", 0, 1, error, {}}), std::invalid_argument);
    EXPECT_THROW(fs.AddRule({EIOOp::Write, "*", 1, 0, error, {}}), std::invalid_argument);
    EXPECT_THROW(fs.AddRule({EIOOp::Write, "*", 1, 1, error, 1}), std::invalid_argument);
    EXPECT_THROW(fs.AddRule({EIOOp::SyncFile, "*", 1, 1, {}, 1}), std::invalid_argument);
    EXPECT_THROW(fs.AddRule({EIOOp::Publish, "*", 1, 1, error, {}}), std::invalid_argument);
    EXPECT_THROW(fs.AddRule({EIOOp::Write, std::string("a\0b", 3), 1, 1, error, {}}), std::invalid_argument);
    for (const auto op : {EIOOp::Write, EIOOp::Close, EIOOp::CreateExclusive, EIOOp::MakeDir, EIOOp::PRead}) {
        EXPECT_THROW(fs.AddRule({op, "*", 1, 1, error, std::nullopt, true}), std::invalid_argument);
    }
    EXPECT_THROW(fs.AddRule({EIOOp::SyncFile, "*", 1, 1, {}, std::nullopt, true}), std::invalid_argument);
    EXPECT_THROW(fs.AddRule({EIOOp::SyncDir, "*", 1, 1, error, 1, true}), std::invalid_argument);
}

TEST(FaultTrace, JsonEscapesStringsAndExportsPublicationReferences)
{
    TMemoryFS underlying;
    TFaultFS fs(underlying);
    fs.Publish({std::string("a\"\\\n\t\0", 6), "é", std::string(1, static_cast<char>(0xff))});
    EXPECT_EQ(fs.JsonLines(),
        "{\"string_encoding\":\"latin1-bytes\",\"seq\":1,\"op\":"
        "\"Publish\",\"path\":\"\",\"kind\":\"Other\","
        "\"attempt_id\":\"\",\"bytes\":0,\"result\":0,\"references\":["
        "\"a\\\"\\\\\\u000a\\u0009\\u0000\",\"\\u00c3\\u00a9\","
        "\"\\u00ff\"]}\n");
}

TEST(FaultTrace, ReplayProducesIdenticalEventsForAllFileKinds)
{
    auto run = [] {
        TMemoryFS underlying;
        TFaultFS fs(underlying);
        auto root = fs.OpenDir("/store").Value();
        fs.AddRule({EIOOp::Write, "*/2", 1, 2, std::make_error_code(std::errc::interrupted), {}});
        int index = 0;
        for (auto kind :
            {EFileKind::Other, EFileKind::Data, EFileKind::Manifest, EFileKind::ManifestList, EFileKind::Metadata})
        {
            auto file = fs.CreateExclusive(root, std::to_string(index++), {kind, "op\"\n"}).Value();
            static_cast<void>(fs.Write(file, Bytes));
            EXPECT_FALSE(fs.Close(file));
        }
        return fs.JsonLines();
    };
    EXPECT_EQ(run(), run());
    for (auto name : {"Other", "Data", "Manifest", "ManifestList", "Metadata"}) {
        EXPECT_NE(run().find(std::string("\"kind\":\"") + name + '"'), std::string::npos);
    }
}

TEST(FaultTrace, CanonicalizesTrailingDirectorySeparators)
{
    TMemoryFS underlying;
    TFaultFS fs(underlying);
    auto directory = fs.OpenDir("/store///");
    ASSERT_TRUE(directory);
    auto file = fs.CreateExclusive(directory.Value(), "file");
    ASSERT_TRUE(file);
    ASSERT_FALSE(fs.SyncFile(file.Value()));
    ASSERT_FALSE(fs.SyncDir(directory.Value()));
    fs.Publish({"/store/file"});
    TDurabilityTrace trace({"/store"});
    for (const auto& event : fs.Events()) {
        EXPECT_FALSE(trace.Apply(event));
    }
    EXPECT_EQ(fs.Events().front().Path, "/store");
    EXPECT_TRUE(fs.OpenDir("/"));
    EXPECT_EQ(fs.Events().back().Path, "/");
}

TEST_F(TFaultFSTest, SerializesConcurrentCallsAndFaultCounters)
{
    Fs_.AddRule({EIOOp::Write, "*", 17, 1, std::make_error_code(std::errc::no_space_on_device), {}});
    std::array<std::thread, 4> workers;
    for (auto& worker : workers) {
        worker = std::thread([&] {
            for (int i = 0; i < 25; ++i) {
                static_cast<void>(Fs_.Write(File_, Bytes));
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    const auto events = Fs_.Events();
    ASSERT_EQ(events.size(), 102U);
    for (std::size_t i = 2; i < events.size(); ++i) {
        EXPECT_EQ(events[i].Seq, i + 1);
        EXPECT_EQ(static_cast<bool>(events[i].Error), i == 18);
    }
    EXPECT_EQ(Fs_.Stat(File_).Value().Size, 99 * Bytes.size());
}

TEST_F(TFaultFSTest, RecordedOperationsSatisfyTheIndependentOracle)
{
    ASSERT_TRUE(Fs_.Write(File_, Bytes));
    ASSERT_FALSE(Fs_.SyncFile(File_));
    ASSERT_FALSE(Fs_.Close(File_));
    ASSERT_FALSE(Fs_.SyncDir(Root_));
    auto reader = Fs_.OpenRead(Root_, "file.parquet");
    ASSERT_TRUE(reader);
    ASSERT_FALSE(Fs_.Close(reader.Value()));
    Fs_.Publish({"/store/file.parquet"});
    TDurabilityTrace trace({"/store"});
    for (const auto& event : Fs_.Events()) {
        EXPECT_FALSE(trace.Apply(event));
    }
    EXPECT_EQ(trace.DurableFiles(), std::set<std::string>{"/store/file.parquet"});
}

}
}
