// Environmental read errors (EMFILE, EACCES) while verifying retained state on
// open keep their filesystem classification (SCANNER.md#error-classification);
// only invariant violations of persisted state are CORRUPTION.

#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
namespace NT = NGlamDB::NTesting;

detail::BackendFactory FaultFactory(const detail::TLocalStorageOptions& options)
{
    return [options](const std::filesystem::path& path,
               const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
        auto backend = detail::TLocalStorageBackend::Open(path, info, options);
        if (!backend) {
            return std::unexpected(backend.error());
        }
        return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
    };
}

class BackendRetainedClassificationTest : public ::testing::TestWithParam<std::errc>
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";

    void SetUp() override
    {
        auto created = CreateStore({test::store_id, Root_.string()});
        ASSERT_TRUE(created) << created.error().message;
        auto engine = *created;
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
            CommitDisposition::Committed);
        ASSERT_TRUE(engine->Shutdown(1000ms));
    }
};

TEST_P(BackendRetainedClassificationTest, TransientReadFailureDuringOpenIsNotCorruption)
{
    {
        NGlamDB::NIO::TPosixLocalFS posix;
        auto faults = std::make_shared<NT::TFaultFS>(posix);
        // First open of the committed Avro manifest during VerifyReachable.
        faults->AddRule({NT::EIOOp::OpenRead, "*-m0.avro", 1, 1, std::make_error_code(GetParam()), std::nullopt});
        detail::TLocalStorageOptions options;
        options.Fs = faults;
        auto opened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, FaultFactory(options));
        ASSERT_FALSE(opened);
        EXPECT_EQ(opened.error().code, "IO_ERROR") << opened.error().message;
    }
    // Nothing was wrong with the store: an unfaulted open succeeds.
    auto clean = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(clean) << clean.error().code << ": " << clean.error().message;
    EXPECT_TRUE((*clean)->Shutdown(1000ms));
}

INSTANTIATE_TEST_SUITE_P(EnvironmentalErrors,
    BackendRetainedClassificationTest,
    ::testing::Values(std::errc::too_many_files_open, std::errc::permission_denied));

} // namespace
} // namespace glamdb
