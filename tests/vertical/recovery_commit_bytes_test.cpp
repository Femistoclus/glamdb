// Recovery applies the per-commit CommitBytes limit to each retained commit
// with that commit's own files, so every history the publication guard
// accepted reopens under the same limits. Only the current commit is charged
// for the final root; older commits never wrote it.

#include "tests/vertical/vertical_support.h"

#include <algorithm>
#include <chrono>
#include <random>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;

detail::BackendFactory LimitedFactory(const detail::TLocalStorageOptions& options)
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

// Two distinct 1000-byte pseudo-random keys: the key column keeps full bounds
// ("write.metadata.metrics.column.key" = "full", table_profile.cpp:82-84) and
// random letters do not deflate well, so this commit's Avro manifest is ~1 KiB
// larger than one with one-byte keys, far more than one manifest-list entry.
std::vector<TInputRow> WideKeyRows()
{
    std::minstd_rand random(20261111);
    std::vector<TInputRow> rows;
    for (int index = 0; index < 2; ++index) {
        std::string key(1000, 'a');
        for (auto& symbol : key) {
            symbol = static_cast<char>('a' + random() % 26);
        }
        rows.push_back({key, index, 0.5, true, 1000 + index});
    }
    return rows;
}

std::filesystem::path OnlyFileWithPrefix(const std::filesystem::path& directory, std::string_view prefix)
{
    const auto files = FilesIn(directory, [&](const std::filesystem::path& path) {
        return path.filename().string().starts_with(prefix);
    });
    EXPECT_EQ(files.size(), 1U) << prefix;
    return files.empty() ? std::filesystem::path{} : files.front();
}

// Manifest-list leaves in snapshot order (compact JSON written by iceberg-cpp).
std::vector<std::string> ManifestListLeaves(const std::string& json)
{
    std::vector<std::string> leaves;
    const std::string key = "\"manifest-list\":\"";
    for (auto at = json.find(key); at != std::string::npos; at = json.find(key, at + key.size())) {
        const auto begin = at + key.size();
        const auto end = json.find('"', begin);
        const auto location = json.substr(begin, end - begin);
        leaves.push_back(location.substr(location.rfind('/') + 1));
    }
    return leaves;
}

// snap-<snapshot>-<attempt>-<commit uuid>.avro -> <commit uuid>-m0.avro
// (iceberg-cpp snapshot_update.cc:526-536 and :560-568 share commit_uuid_).
std::string ManifestOf(const std::string& listLeaf)
{
    return listLeaf.substr(listLeaf.size() - 5 - 36, 36) + "-m0.avro";
}

TEST(BackendRecoveryLimitsTest, ReopenAcceptsHistoryPublishedUnderTheSameCommitBytes)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, root.string()}, LimitedFactory({}));
        ASSERT_TRUE(created) << created.error().message;
        auto engine = std::move(*created);
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(WideKeyRows())}}).disposition,
            CommitDisposition::Committed);
        const std::vector<TInputRow> narrow{{"a", 1, 0.5, true, 1}, {"b", 2, 0.5, true, 2}};
        ASSERT_EQ(engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(narrow)}}).disposition,
            CommitDisposition::Committed);
        ASSERT_TRUE(engine->Shutdown(1000ms));
    }

    const auto metadata = root / "warehouse" / test::collection_id / "metadata";
    const auto firstRoot = OnlyFileWithPrefix(metadata, "00001-");
    const auto secondRoot = OnlyFileWithPrefix(metadata, "00002-");
    ASSERT_FALSE(firstRoot.empty());
    ASSERT_FALSE(secondRoot.empty());
    const auto lists = ManifestListLeaves(ReadBytes(secondRoot));
    ASSERT_EQ(lists.size(), 2U);
    const auto size = [&](const std::string& leaf) {
        return static_cast<std::uint64_t>(std::filesystem::file_size(metadata / leaf));
    };
    const auto rootA = static_cast<std::uint64_t>(std::filesystem::file_size(firstRoot));
    const auto rootB = static_cast<std::uint64_t>(std::filesystem::file_size(secondRoot));
    const auto manifestA = size(ManifestOf(lists[0]));
    const auto listA = size(lists[0]);
    const auto manifestB = size(ManifestOf(lists[1]));
    const auto listB = size(lists[1]);

    // Exactly what each publication guard compared with CommitBytes.
    const auto commitBytes = std::max(rootA + manifestA + listA, rootB + manifestB + listB);
    // What recovery compares for the historical snapshot A; the history is built
    // so that it exceeds every published sum (needs manifestA + listA > manifestB + listB).
    ASSERT_GT(rootB + manifestA + listA, commitBytes) << "history does not exercise the defect";

    detail::TLocalStorageOptions options;
    options.Storage.CommitBytes = commitBytes;
    auto reopened = detail::EngineFactory::OpenWith({root.string(), test::store_id}, LimitedFactory(options));
    ASSERT_TRUE(reopened) << reopened.error().code << ": " << reopened.error().message;
    auto engine = std::move(*reopened);
    const auto collection = engine->GetCollection(test::collection_id);
    ASSERT_TRUE(collection);
    EXPECT_TRUE(collection->current_snapshot_id.has_value());
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

} // namespace
} // namespace glamdb
