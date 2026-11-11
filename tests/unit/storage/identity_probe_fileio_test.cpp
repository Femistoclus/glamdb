// Descriptor exhaustion during an output file's identity probe fails only that
// attempt: the shared write latch stays clear and the next attempt can bind.

#include "core/storage/durable_local_file_io.h"
#include "tests/support/fault_fs.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

namespace NGlamDB::NStorage {
namespace {

using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;

constexpr auto KStore = "11111111-1111-4111-8111-111111111111";
constexpr auto KCollection = "22222222-2222-4222-8222-222222222222";
constexpr auto KFirstAttempt = "33333333-3333-7333-8333-333333333333";
constexpr auto KSecondAttempt = "33333333-3333-7333-8333-333333333334";
constexpr auto KOperation = "44444444-4444-4444-8444-444444444444";
constexpr auto KFirstRoot = "55555555-5555-4555-8555-555555555555";
constexpr auto KSecondRoot = "66666666-6666-4666-8666-666666666666";

class TIdentityProbeFileIOTest : public ::testing::Test
{
protected:
    std::filesystem::path Parent_;
    std::shared_ptr<NIO::TPosixLocalFS> Posix_ = std::make_shared<NIO::TPosixLocalFS>();
    std::shared_ptr<TFaultFS> Fs_ = std::make_shared<TFaultFS>(*Posix_);
    std::shared_ptr<NIO::TWriteLatch> Latch_ = std::make_shared<NIO::TWriteLatch>();
    std::shared_ptr<NIO::TStoreDirs> Dirs_;
    std::shared_ptr<TDurableLocalFileIO> Io_;

    void SetUp() override
    {
        auto name = (std::filesystem::temp_directory_path() / "glamdb-i1io-XXXXXX").string();
        ASSERT_NE(::mkdtemp(name.data()), nullptr);
        Parent_ = std::filesystem::canonical(name);
        auto dirs = NIO::TStoreDirs::Create(Fs_, Parent_ / "store", Latch_);
        ASSERT_TRUE(dirs);
        Dirs_ = std::move(dirs).Value();
        ASSERT_TRUE(Dirs_->CreateCollection(KCollection));
        auto io = TDurableLocalFileIO::Make(Dirs_, KStore);
        ASSERT_TRUE(io);
        Io_ = std::move(*io);
    }

    void TearDown() override
    {
        Io_.reset();
        Dirs_.reset();
        std::error_code error;
        std::filesystem::remove_all(Parent_, error);
        EXPECT_FALSE(error);
    }

    std::shared_ptr<NIO::TIOAttempt> MakeAttempt(const char* attemptId)
    {
        auto attempt =
            NIO::TIOAttempt::Make({KStore, KCollection, attemptId, NIO::EAttemptKind::Append, KOperation}, Latch_);
        EXPECT_TRUE(attempt);
        return attempt ? std::move(attempt).Value() : nullptr;
    }

    static std::string Root(const char* uuid)
    {
        return std::string("store://") + KStore + "/warehouse/" + KCollection + "/metadata/00001-" + uuid +
            ".metadata.json";
    }
};

TEST_F(TIdentityProbeFileIOTest, DescriptorExhaustionAtOutputCreationFailsOnlyThatAttempt)
{
    {
        auto first = MakeAttempt(KFirstAttempt);
        ASSERT_NE(first, nullptr);
        auto scope = Io_->BindAttempt(first);
        ASSERT_TRUE(scope);
        // WriteFile -> NewOutputFile (no I/O) -> Create -> CheckIdentity: the first
        // absolute OpenDir of the store root after this rule is the identity probe.
        Fs_->AddRule({EIOOp::OpenDir,
            Dirs_->Path().string(),
            1,
            1,
            std::make_error_code(std::errc::too_many_files_open),
            std::nullopt});
        EXPECT_FALSE(Io_->WriteFile(Root(KFirstRoot), "{}"));
        EXPECT_EQ(first->FirstError(), std::errc::too_many_files_open);
    }
    EXPECT_FALSE(Latch_->IsSet()) << "EMFILE in the identity probe must not poison every later write";

    auto second = MakeAttempt(KSecondAttempt);
    ASSERT_NE(second, nullptr);
    auto scope = Io_->BindAttempt(second);
    ASSERT_TRUE(scope) << scope.error().message;
    EXPECT_TRUE(Io_->WriteFile(Root(KSecondRoot), "{}"));
    EXPECT_EQ(second->Receipts().size(), 1U);
}

}
}
