// The append guard validates the in-memory staged root and compares the
// durable file with its serialized bytes instead of parsing the file. That is
// equivalent only if parsing those bytes yields the staged metadata again.
// These tests establish it for roots written by real appends: every root
// reserializes byte-identically after parsing, the reparse is structurally
// equal, and the guard's verification mode (enabled here) requires the parse
// of each durable root to equal the staged metadata before publication.

#include "core/storage/format_verifier.h"
#include "tests/vertical/vertical_support.h"

#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

#include <chrono>
#include <format>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
namespace NStorage = NGlamDB::NStorage;

std::filesystem::path CurrentRoot(const std::filesystem::path& store, std::string_view collection)
{
    const auto roots = FilesIn(store / "warehouse" / collection / "metadata", [](const auto& path) {
        return path.string().ends_with(".metadata.json");
    });
    EXPECT_FALSE(roots.empty());
    return roots.empty() ? std::filesystem::path() : roots.back();
}

void ExpectRoundTrip(const std::filesystem::path& root, std::size_t snapshots)
{
    const auto bytes = ReadBytes(root);
    ASSERT_FALSE(bytes.empty()) << root;
    auto parsed = NStorage::ParseTableMetadata(bytes);
    ASSERT_TRUE(parsed) << parsed.error().message;
    const auto& metadata = **parsed;
    EXPECT_EQ(metadata.snapshots.size(), snapshots);
    EXPECT_EQ(metadata.snapshot_log.size(), snapshots);
    EXPECT_FALSE(metadata.properties.configs().empty());
    EXPECT_EQ(metadata.schemas.size(), 1U);
    auto reserialized = NStorage::SerializeTableMetadata(metadata);
    ASSERT_TRUE(reserialized) << reserialized.error().message;
    EXPECT_EQ(*reserialized, bytes) << root;
    auto reparsed = NStorage::ParseTableMetadata(*reserialized);
    ASSERT_TRUE(reparsed) << reparsed.error().message;
    EXPECT_TRUE(**reparsed == metadata) << root;
}

TEST(RootRoundTripTest, RootsWrittenByAppendsReserializeToTheSameBytes)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    std::shared_ptr<THookBackend> hooks;
    detail::TLocalStorageOptions options;
    options.VerifyStagedRoot = true;
    auto created =
        detail::EngineFactory::CreateWith({test::store_id, root.string()}, THookBackend::Factory(hooks, options));
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    ExpectRoundTrip(CurrentRoot(root, test::collection_id), 0);

    std::vector<TInputRow> many;
    for (int row = 0; row < 500; ++row) {
        many.push_back({std::format("key-{}", row % 7),
            row % 3 == 0 ? std::nullopt : std::optional<int64_t>(-row),
            row * 0.25,
            row % 2 == 0,
            253402300799999999LL - row});
    }
    const std::vector<const std::vector<TInputRow>*> inputs{&FirstRows, &SecondRows, &many, &FirstRows};
    for (std::size_t index = 0; index < 12; ++index) {
        const auto operation = std::format("00000000-0000-4000-8000-{:012x}", index + 1);
        const auto outcome =
            engine->AppendDurable({test::collection_id, operation, {}, {Batch(*inputs[index % inputs.size()])}});
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed) << "append " << index;
        ExpectRoundTrip(CurrentRoot(root, test::collection_id), index + 1);
    }
    ASSERT_TRUE(engine->Shutdown(1000ms));
}

} // namespace
} // namespace glamdb
