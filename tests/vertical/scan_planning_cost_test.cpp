// Healthy native scans reuse verified membership. Planning and execution must
// not reopen roots, manifest lists or cumulative data manifests.

#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <cstdio>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;

std::string OperationId(int index)
{
    char buffer[37];
    std::snprintf(buffer, sizeof(buffer), "00000000-0000-4000-8000-%012x", 0x100 + index);
    return buffer;
}

size_t MetadataOpens(const TFaultFS& faults)
{
    size_t count = 0;
    for (const auto& event : faults.Events()) {
        const auto& path = event.Path;
        if (event.Op == EIOOp::OpenRead && (path.ends_with(".avro") || path.ends_with(".metadata.json"))) {
            ++count;
        }
    }
    return count;
}

TEST(ScanPlanningCost, RepeatedScansDoNotReadMetadata)
{
    constexpr int Appends = 24;
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    NGlamDB::NIO::TPosixLocalFS posix;
    const auto faults = std::make_shared<TFaultFS>(posix);
    const detail::BackendFactory factory =
        [faults](const std::filesystem::path& path,
            const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
        detail::TLocalStorageOptions options;
        options.Fs = faults;
        auto backend = detail::TLocalStorageBackend::Open(path, info, options);
        if (!backend) {
            return std::unexpected(backend.error());
        }
        return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
    };
    auto created = detail::EngineFactory::CreateWith({test::store_id, root.string()}, factory);
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    for (int index = 0; index < Appends; ++index) {
        const auto outcome = engine->AppendDurable({test::collection_id,
            OperationId(index),
            {},
            {Batch({{"k" + std::to_string(index), int64_t{index}, 1.0, true, int64_t{1}}})}});
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
            << (outcome.error ? outcome.error->code + ": " + outcome.error->message : std::string());
    }

    for (const int64_t limit : {0, 1, Appends}) {
        SCOPED_TRACE("limit=" + std::to_string(limit));
        const auto before = MetadataOpens(*faults);
        const auto cursor = engine->Scan({.collection_id = test::collection_id, .limit = limit});
        ASSERT_TRUE(cursor) << cursor.error().code << ": " << cursor.error().message;
        const auto first = (*cursor)->Next();
        ASSERT_TRUE(first) << first.error().message;
        EXPECT_EQ(*first == nullptr, limit == 0);
        const auto opened = MetadataOpens(*faults) - before;
        EXPECT_EQ(opened, 0U) << "metadata opened by a limit=" << limit << " scan after " << Appends << " appends";
    }

    ASSERT_TRUE(engine->Shutdown(1000ms));
    engine.reset();
}

} // namespace
} // namespace glamdb
