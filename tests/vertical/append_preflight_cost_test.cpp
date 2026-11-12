#include "core/storage/format_verifier.h"
#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <format>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
namespace NT = NGlamDB::NTesting;

TEST(BackendPreflightCostTest, AppendReusesWarehouseInventoryAndChecksFreeSpace)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    NGlamDB::NIO::TPosixLocalFS posix;
    auto faults = std::make_shared<NT::TFaultFS>(posix);
    std::shared_ptr<THookBackend> hooks;
    detail::TLocalStorageOptions options;
    options.Fs = faults;
    // The production default; tests otherwise also parse to verify the root.
    options.VerifyStagedRoot = false;
    auto created =
        detail::EngineFactory::CreateWith({test::store_id, root.string()}, THookBackend::Factory(hooks, options));
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);

    const auto warehouse = posix.OpenDir(root / "warehouse");
    ASSERT_TRUE(warehouse);
    const bool incremental = static_cast<bool>(posix.TrackTree(warehouse.Value()));

    const auto mark = faults->Events().size();
    const auto parses = NGlamDB::NStorage::TableMetadataParses();
    ASSERT_EQ(engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
    const auto rootParses = NGlamDB::NStorage::TableMetadataParses() - parses;
    std::size_t walks = 0;
    std::size_t statfs = 0;
    std::size_t rootOpens = 0;
    std::size_t listOpens = 0;
    std::size_t manifestOpens = 0;
    const auto events = faults->Events();
    for (std::size_t index = mark; index < events.size(); ++index) {
        const auto& event = events[index];
        walks += event.Op == NT::EIOOp::MeasureTree ? 1 : 0;
        statfs += event.Op == NT::EIOOp::StatFs ? 1 : 0;
        if (event.Op == NT::EIOOp::OpenRead) {
            const auto leaf = std::filesystem::path(event.Path).filename().string();
            rootOpens += leaf.ends_with(".metadata.json") ? 1 : 0;
            listOpens += leaf.starts_with("snap-") ? 1 : 0;
            manifestOpens += leaf.ends_with(".avro") && !leaf.starts_with("snap-") ? 1 : 0;
        }
    }
    RecordProperty("root_opens", static_cast<int>(rootOpens));
    RecordProperty("manifest_list_opens", static_cast<int>(listOpens));
    RecordProperty("manifest_opens", static_cast<int>(manifestOpens));
    std::printf("per append: walks=%zu statfs=%zu root_parses=%llu root_opens=%zu manifest_list_opens=%zu "
                "manifest_opens=%zu\n",
        walks,
        statfs,
        static_cast<unsigned long long>(rootParses),
        rootOpens,
        listOpens,
        manifestOpens);
    EXPECT_EQ(walks, incremental ? 0U : 1U) << "supported filesystems need no full inventory for each append";
    EXPECT_EQ(rootParses, 0U) << "the guard validates the staged root without parsing the durable file";
    EXPECT_GE(statfs, 1U) << "free-space reserve must still be checked";
    ASSERT_TRUE(engine->Shutdown(1000ms));
}

} // namespace
} // namespace glamdb
