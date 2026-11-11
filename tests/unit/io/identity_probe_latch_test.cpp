// TStoreDirs::CheckIdentity() latches writes only on identity evidence (a
// replaced, missing or redirected root) or on EIO/EROFS. Transient resource
// errors of the probe itself (EMFILE, ENFILE, ENOMEM) fail just that call.

#include "core/io/store_dirs.h"
#include "tests/support/fault_fs.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace NGlamDB::NIO {
namespace {

using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;

constexpr std::string_view KIdentityProbeCollection = "01234567-89ab-cdef-0123-456789abcdef";

class TIdentityProbeResourceErrorTest : public ::testing::TestWithParam<std::errc>
{
protected:
    std::shared_ptr<TPosixLocalFS> Underlying_ = std::make_shared<TPosixLocalFS>();
    std::shared_ptr<TFaultFS> Fs_ = std::make_shared<TFaultFS>(*Underlying_);
    std::shared_ptr<TWriteLatch> Latch_ = std::make_shared<TWriteLatch>();
    std::filesystem::path Parent_;
    std::filesystem::path Path_;

    void SetUp() override
    {
        auto name = (std::filesystem::temp_directory_path() / "glamdb-i1-XXXXXX").string();
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
};

TEST_P(TIdentityProbeResourceErrorTest, ResourceExhaustionInRootProbeDoesNotLatchWrites)
{
    auto store = TStoreDirs::Create(Fs_, Path_, Latch_);
    ASSERT_TRUE(store);
    ASSERT_FALSE(Latch_->IsSet());

    // Rules only count matches made after they are added. After Create(), the only
    // absolute OpenDir of the store root is CheckIdentity()'s path walk
    // (store_dirs.cpp:172 -> local_fs.cpp:164).
    Fs_->AddRule({EIOOp::OpenDir, Path_.string(), 1, 1, std::make_error_code(GetParam()), std::nullopt});

    // The caller still sees the error ...
    EXPECT_EQ(store.Value()->CheckIdentity(), GetParam());
    // ... but the root identity is unchanged: a process resource was exhausted.
    // Per SCANNER.md the latch is for failed durable writes and blocks every later
    // write of the engine instance until restart.
    EXPECT_FALSE(Latch_->IsSet());

    // Once the descriptor/memory pressure is gone, writes must work again.
    EXPECT_FALSE(store.Value()->CheckIdentity());
    EXPECT_TRUE(store.Value()->CreateCollection(KIdentityProbeCollection));
}

INSTANTIATE_TEST_SUITE_P(TransientResourceErrors,
    TIdentityProbeResourceErrorTest,
    ::testing::Values(std::errc::too_many_files_open,
        std::errc::too_many_files_open_in_system,
        std::errc::not_enough_memory));

}
}
