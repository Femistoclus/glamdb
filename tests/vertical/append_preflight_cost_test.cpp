// One append walks the warehouse at most once: PrepareAppend reuses the
// admission preflight's measurement (binding writes only the catalog) and
// still checks the free-space reserve. A walk costs one fstatat per retained
// file, so a second walk doubled an O(files) cost on every append.

#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <format>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
namespace NT = NGlamDB::NTesting;

TEST(BackendPreflightCostTest, AppendWalksTheWarehouseAtMostOnce)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    NGlamDB::NIO::TPosixLocalFS posix;
    auto faults = std::make_shared<NT::TFaultFS>(posix);
    std::shared_ptr<THookBackend> hooks;
    detail::TLocalStorageOptions options;
    options.Fs = faults;
    auto created =
        detail::EngineFactory::CreateWith({test::store_id, root.string()}, THookBackend::Factory(hooks, options));
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);

    const auto mark = faults->Events().size();
    ASSERT_EQ(engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
    std::size_t walks = 0;
    std::size_t statfs = 0;
    const auto events = faults->Events();
    for (std::size_t index = mark; index < events.size(); ++index) {
        walks += events[index].Op == NT::EIOOp::MeasureTree ? 1 : 0;
        statfs += events[index].Op == NT::EIOOp::StatFs ? 1 : 0;
    }
    EXPECT_LE(walks, 1U) << "warehouse tree walks during one append";
    EXPECT_GE(statfs, 1U) << "free-space reserve must still be checked";
    ASSERT_TRUE(engine->Shutdown(1000ms));
}

} // namespace
} // namespace glamdb
