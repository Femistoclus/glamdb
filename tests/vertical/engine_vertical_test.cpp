#include "storage/engine.h"

#include "core/api/canonical.h"
#include "core/api/codec.h"
#include "core/backend/local_storage_backend.h"
#include "core/catalog/durable_sqlite_catalog.h"
#include "core/engine/engine_factory.h"
#include "core/io/durable_copy.h"
#include "core/io/store_uri.h"
#include "core/storage/base_state.h"
#include "tests/support/temp_dir.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <optional>
#include <spawn.h>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <thread>
#include <tuple>
#include <vector>

extern char** environ;

namespace glamdb {
namespace {

using namespace std::chrono_literals;

constexpr char FirstOperation[] = "00000000-0000-4000-8000-0000000000a1";
constexpr char SecondOperation[] = "00000000-0000-4000-8000-0000000000a2";
constexpr char ThirdOperation[] = "00000000-0000-4000-8000-0000000000a3";
constexpr char OrphanData[] = "00000000-0000-4000-8000-0000000000a3-00000000-0000-7000-8000-0000000000a4-00000.parquet";
constexpr char OrphanMetadata[] = "99999-00000000-0000-7000-8000-0000000000a4.metadata.json";

const SchemaSpec Schema{{
    {"key", FieldType::String, false},
    {"value", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"flag", FieldType::Bool, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

struct TInputRow
{
    std::string Key;
    std::optional<int64_t> Value;
    std::optional<double> Score;
    std::optional<bool> Flag;
    std::optional<int64_t> At;
};

template <class TBuilder, class TValue>
std::shared_ptr<arrow::Array> Column(const std::vector<std::optional<TValue>>& values,
    std::shared_ptr<arrow::DataType> type)
{
    TBuilder builder(type, arrow::default_memory_pool());
    for (const auto& value : values) {
        const auto status = value ? builder.Append(*value) : builder.AppendNull();
        EXPECT_TRUE(status.ok()) << status.ToString();
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

std::shared_ptr<arrow::RecordBatch> Batch(const std::vector<TInputRow>& rows)
{
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<int64_t>> values;
    std::vector<std::optional<double>> scores;
    std::vector<std::optional<bool>> flags;
    std::vector<std::optional<int64_t>> times;
    for (const auto& row : rows) {
        keys.emplace_back(row.Key);
        values.push_back(row.Value);
        scores.push_back(row.Score);
        flags.push_back(row.Flag);
        times.push_back(row.At);
    }
    const auto timestamp = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false),
                                        arrow::field("value", arrow::int64()),
                                        arrow::field("score", arrow::float64()),
                                        arrow::field("flag", arrow::boolean()),
                                        arrow::field("at", timestamp)}),
        static_cast<int64_t>(rows.size()),
        {Column<arrow::StringBuilder>(keys, arrow::utf8()),
            Column<arrow::Int64Builder>(values, arrow::int64()),
            Column<arrow::DoubleBuilder>(scores, arrow::float64()),
            Column<arrow::BooleanBuilder>(flags, arrow::boolean()),
            Column<arrow::TimestampBuilder>(times, timestamp)});
}

std::string RenderCell(const arrow::Array& array, int64_t row)
{
    if (array.IsNull(row)) {
        return "null";
    }
    switch (array.type_id()) {
        case arrow::Type::STRING:
            return std::string(static_cast<const arrow::StringArray&>(array).GetView(row));
        case arrow::Type::INT64:
            return std::to_string(static_cast<const arrow::Int64Array&>(array).Value(row));
        case arrow::Type::DOUBLE:
            return std::to_string(static_cast<const arrow::DoubleArray&>(array).Value(row));
        case arrow::Type::BOOL:
            return static_cast<const arrow::BooleanArray&>(array).Value(row) ? "true" : "false";
        case arrow::Type::TIMESTAMP:
            return "ts:" + std::to_string(static_cast<const arrow::TimestampArray&>(array).Value(row));
        default:
            ADD_FAILURE() << "unexpected type " << array.type()->ToString();
            return "?";
    }
}

// Renders rows independently of the scanner, so input and output compare as multisets.
std::vector<std::string> Render(const arrow::RecordBatch& batch)
{
    std::vector<std::string> rows;
    for (int64_t row = 0; row < batch.num_rows(); ++row) {
        std::string text;
        for (int column = 0; column < batch.num_columns(); ++column) {
            text += (column == 0 ? "" : "|") + RenderCell(*batch.column(column), row);
        }
        rows.push_back(std::move(text));
    }
    return rows;
}

std::vector<std::string> Sorted(std::vector<std::string> rows)
{
    std::ranges::sort(rows);
    return rows;
}

const std::vector<TInputRow> FirstRows{
    {"duplicate", 1, 0.5, true, 10},
    {"duplicate", 1, 0.5, true, 10},
    {"other", std::nullopt, std::nullopt, std::nullopt, std::nullopt},
};
const std::vector<TInputRow> SecondRows{
    {"duplicate", 2, -1.5, false, 20},
    {"\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87", 3, 2.5, true, 30},
};

std::vector<std::string> Expected(std::initializer_list<const std::vector<TInputRow>*> inputs)
{
    std::vector<std::string> rows;
    for (const auto* input : inputs) {
        const auto rendered = Render(*Batch(*input));
        rows.insert(rows.end(), rendered.begin(), rendered.end());
    }
    return Sorted(std::move(rows));
}

std::vector<TInputRow> VerticalRows(bool second)
{
    return second ? std::vector<TInputRow>(10, {"duplicate", 2, -1.5, false, 20})
                  : std::vector<TInputRow>(1000, {"duplicate", 1, 0.5, true, 10});
}

std::vector<std::string> VerticalExpected(bool latest)
{
    std::vector<std::string> rows(1000, "duplicate|1|0.500000|true|ts:10");
    if (latest) {
        rows.insert(rows.end(), 10, "duplicate|2|-1.500000|false|ts:20");
    }
    return Sorted(std::move(rows));
}

std::vector<std::filesystem::path> FileInventory(const std::filesystem::path& root)
{
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file()) {
            files.push_back(entry.path());
        }
    }
    std::ranges::sort(files);
    return files;
}

void ExpectFreshVerticalProcess(const std::filesystem::path& root, const std::filesystem::path& oracles)
{
#ifdef __linux__
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        const std::string_view value(*entry);
        if (!value.starts_with("GLAMDB_VERTICAL_") && !value.starts_with("GTEST_")) {
            environment.emplace_back(value);
        }
    }
    environment.push_back("GLAMDB_VERTICAL_ROOT=" + root.string());
    environment.push_back("GLAMDB_VERTICAL_ORACLES=" + oracles.string());
    std::vector<char*> variables;
    variables.reserve(environment.size() + 1);
    for (auto& value : environment) {
        variables.push_back(value.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=EngineVerticalChildProcess.ReopensHistoryAndOriginalReceipts";
    char* arguments[]{executable.data(), filter.data(), nullptr};
    pid_t child = 0;
    ASSERT_EQ(::posix_spawn(&child, executable.c_str(), nullptr, nullptr, arguments, variables.data()), 0);
    int status = 0;
    pid_t waited;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
#else
    static_cast<void>(root);
    static_cast<void>(oracles);
    GTEST_SKIP() << "Fresh-process recovery is checked on Linux.";
#endif
}

class EngineVerticalTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        auto created = CreateStore({test::store_id, Root_.string()});
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = *created;
        const auto outcome = Engine_->CreateCollection({test::collection_id, "events", Schema});
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
            << (outcome.error ? outcome.error->message : std::string());
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    AppendOutcome Append(const char* operation, const std::vector<TInputRow>& rows, SnapshotPrecondition expected = {})
    {
        return Engine_->AppendDurable({test::collection_id, operation, expected, {Batch(rows)}});
    }

    AppendResult
    Committed(const char* operation, const std::vector<TInputRow>& rows, SnapshotPrecondition expected = {})
    {
        const auto outcome = Append(operation, rows, expected);
        EXPECT_EQ(outcome.disposition, CommitDisposition::Committed)
            << (outcome.error ? outcome.error->code + ": " + outcome.error->message : std::string());
        return outcome.result.value_or(AppendResult{});
    }

    std::shared_ptr<ScanCursor> Open(ScanRequest request)
    {
        request.collection_id = test::collection_id;
        auto cursor = Engine_->Scan(request);
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().code + ": " + cursor.error().message);
        return cursor ? *cursor : nullptr;
    }

    std::vector<std::string> Drain(ScanCursor& cursor, int64_t batchRows = 65536)
    {
        std::vector<std::string> rows;
        while (true) {
            const auto batch = cursor.Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            EXPECT_TRUE((*batch)->schema()->Equals(*cursor.Info().output_schema));
            EXPECT_GT((*batch)->num_rows(), 0);
            EXPECT_LE((*batch)->num_rows(), batchRows);
            const auto rendered = Render(**batch);
            rows.insert(rows.end(), rendered.begin(), rendered.end());
        }
        return rows;
    }

    std::vector<std::string> Read(ScanRequest request)
    {
        const auto cursor = Open(std::move(request));
        return cursor ? Sorted(Drain(*cursor)) : std::vector<std::string>{};
    }

    void Reopen()
    {
        ASSERT_TRUE(Engine_->Shutdown(1000ms));
        Engine_.reset();
        auto opened = OpenExisting({Root_.string(), test::store_id});
        ASSERT_TRUE(opened) << opened.error().code << ": " << opened.error().message;
        Engine_ = *opened;
    }

    std::vector<std::filesystem::path> DataFiles() const
    {
        std::vector<std::filesystem::path> files;
        for (const auto& entry :
            std::filesystem::directory_iterator(Root_ / "warehouse" / test::collection_id / "data"))
        {
            files.push_back(entry.path());
        }
        return files;
    }
};

class EngineVerticalChildProcess : public EngineVerticalTest
{
protected:
    void SetUp() override
    {
        const auto* root = std::getenv("GLAMDB_VERTICAL_ROOT");
        const auto* oracles = std::getenv("GLAMDB_VERTICAL_ORACLES");
        if (!root && !oracles) {
            GTEST_SKIP() << "Invoked by the public vertical test in a fresh process.";
        }
        ASSERT_NE(root, nullptr);
        ASSERT_NE(oracles, nullptr);
        Root_ = root;
        auto opened = OpenExisting({Root_.string(), test::store_id});
        ASSERT_TRUE(opened) << opened.error().code << ": " << opened.error().message;
        Engine_ = std::move(*opened);
    }
};

class TPreparedBaseTest
    : public EngineVerticalTest
    , public ::testing::WithParamInterface<std::tuple<bool, bool>>
{ };

TEST_P(TPreparedBaseTest, RejectsAuthorityChangesBeforePublishingThePreparedAppend)
{
    namespace NIO = NGlamDB::NIO;
    namespace NStorage = NGlamDB::NStorage;
    const auto [hasSnapshot, metadataOnly] = GetParam();
    std::optional<AppendOutcome> winner;
    if (hasSnapshot) {
        const auto outcome = Append(FirstOperation, FirstRows);
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed);
        winner = outcome;
    }

    // The extra producer and SQL connection are disposable fault actors; all calls are sequential.
    const auto info = Engine_->GetStoreInfo();
    auto catalog = detail::DurableSqliteCatalogStore::Open(Root_ / "catalog.sqlite", info, {});
    const auto backend = detail::TLocalStorageBackend::Open(Root_, info);
    ASSERT_TRUE(backend) << backend.error().message;
    const auto input = detail::NormalizeBatches(Schema, test::collection_id, {Batch({{"stale", 9, 1.0, true, 40}})});
    ASSERT_TRUE(input) << input.error().message;
    constexpr auto attemptID = "00000000-0000-7000-8000-0000000000a6";
    const detail::BoundOperation binding{test::collection_id,
        ThirdOperation,
        input->payload_hash,
        1,
        catalog->Base(test::collection_id),
        attemptID};
    ASSERT_EQ(binding.base.current_snapshot_id.has_value(), hasSnapshot);
    catalog->BindOperation(binding);
    const auto beforePrepare = FileInventory(Root_ / "warehouse");
    auto prepared = (*backend)->PrepareAppend(info,
        binding,
        catalog->Collection(test::collection_id),
        *input,
        std::chrono::steady_clock::now() + 30s);
    ASSERT_TRUE(prepared) << prepared.error().message;
    auto staged = std::move(*prepared);
    ASSERT_NE(staged, nullptr);
    const auto preparedReceipt = staged->Receipt();
    EXPECT_EQ(preparedReceipt.parent_snapshot_id, binding.base.current_snapshot_id);
    ASSERT_GT(FileInventory(Root_ / "warehouse").size(), beforePrepare.size());
    const auto reader = (*backend)->ReadOnlyFileIO();
    ASSERT_TRUE(reader) << reader.error().message;

    if (metadataOnly) {
        const auto dirs =
            NIO::TStoreDirs::Open(std::make_shared<NIO::TPosixLocalFS>(), Root_, std::make_shared<NIO::TWriteLatch>());
        ASSERT_TRUE(dirs);
        const auto collectionDirs = dirs.Value()->OpenCollection(test::collection_id);
        ASSERT_TRUE(collectionDirs);
        const auto source = NIO::TStoreUri::Parse(binding.base.metadata_location, info.store_id);
        ASSERT_TRUE(source);
        constexpr auto target = "00099-00000000-0000-7000-8000-0000000000a7.metadata.json";
        const auto copied = NIO::CopyDurableFile(
            {dirs.Value()->Fs(), collectionDirs.Value().Metadata, std::string(source.Value().Leaf())},
            {dirs.Value()->Fs(), collectionDirs.Value().Metadata, target, dirs.Value()->Latch()},
            std::filesystem::file_size(Root_ / source.Value().RelativePath()));
        ASSERT_TRUE(copied) << copied.Error().message();
        auto replacement = binding.base.metadata_location;
        replacement.replace(replacement.rfind('/') + 1, std::string::npos, target);
        const auto verified = NStorage::VerifyReachable(*reader, replacement, info.store_id, test::collection_id);
        ASSERT_TRUE(verified) << verified.error().message;
        detail::Database actor;
        actor.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        actor.Execute("PRAGMA synchronous=EXTRA");
        actor.Execute("BEGIN IMMEDIATE");
        actor.Execute("UPDATE iceberg_tables SET metadata_location=?,previous_metadata_location=? "
                      "WHERE namespace='columnar' AND table_name='events' AND metadata_location=?",
            {replacement, binding.base.metadata_location, binding.base.metadata_location});
        ASSERT_EQ(actor.Changes(), 1);
        actor.Execute("COMMIT");
    } else {
        const auto outcome = Append(SecondOperation, SecondRows);
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed);
        winner = outcome;
    }

    const auto winningBase = catalog->Base(test::collection_id);
    ASSERT_NE(winningBase.metadata_location, binding.base.metadata_location);
    if (metadataOnly) {
        EXPECT_EQ(winningBase.current_snapshot_id, binding.base.current_snapshot_id);
    } else {
        EXPECT_NE(winningBase.current_snapshot_id, binding.base.current_snapshot_id);
    }
    const auto winningRoot = (*reader)->ReadFile(winningBase.metadata_location, std::nullopt);
    ASSERT_TRUE(winningRoot) << winningRoot.error().message;
    const auto inventory = FileInventory(Root_ / "warehouse");
    int bodies = 0;
    int guards = 0;
    bool rejected = false;
    try {
        catalog->PublishAppend(
            binding,
            preparedReceipt,
            [&](std::string_view location) {
                ++guards;
                return staged->VerifyClosure(location);
            },
            [&] {
                ++bodies;
                return staged->Publish(*catalog);
            });
    } catch (const detail::Failure& error) {
        EXPECT_EQ(error.detail.code, "STALE_BASE");
        EXPECT_FALSE(error.uncertain);
        rejected = true;
    }
    ASSERT_TRUE(rejected);
    EXPECT_EQ(bodies, 0);
    EXPECT_EQ(guards, 0);
    EXPECT_EQ(catalog->Base(test::collection_id), winningBase);
    EXPECT_EQ(staged->Receipt(), preparedReceipt);
    const auto retainedRoot = (*reader)->ReadFile(winningBase.metadata_location, std::nullopt);
    ASSERT_TRUE(retainedRoot) << retainedRoot.error().message;
    EXPECT_EQ(*retainedRoot, *winningRoot);
    staged.reset();
    EXPECT_EQ(FileInventory(Root_ / "warehouse"), inventory);
    const auto pending = catalog->FindOperation(test::collection_id, ThirdOperation);
    if (!pending.has_value()) {
        FAIL() << "prepared operation disappeared";
    }
    EXPECT_EQ(pending->binding, binding);
    EXPECT_EQ(pending->state, detail::OperationState::Bound);
    EXPECT_FALSE(pending->outcome.has_value());
    EXPECT_TRUE(pending->outcome_bytes.empty());
    if (winner.has_value()) {
        const auto encoded = detail::EncodeOutcome(*winner);
        ASSERT_TRUE(encoded) << encoded.error().message;
        const auto committed = catalog->FindOperation(test::collection_id, winner->operation_id);
        if (!committed.has_value()) {
            FAIL() << "winning operation disappeared";
        }
        EXPECT_EQ(committed->state, detail::OperationState::Committed);
        EXPECT_EQ(committed->outcome_bytes, *encoded);
    }
}

INSTANTIATE_TEST_SUITE_P(ExactPreparedBase,
    TPreparedBaseTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const ::testing::TestParamInfo<std::tuple<bool, bool>>& info) {
        return std::string(std::get<0>(info.param) ? "Snapshot" : "Empty") +
            (std::get<1>(info.param) ? "MetadataOnly" : "Append");
    });

TEST_F(EngineVerticalChildProcess, ReopensHistoryAndOriginalReceipts)
{
    const std::filesystem::path oracles(std::getenv("GLAMDB_VERTICAL_ORACLES"));
    std::vector<AppendOutcome> expected;
    expected.reserve(2);
    for (const auto* operation : {FirstOperation, SecondOperation}) {
        std::ifstream input(oracles / operation, std::ios::binary);
        ASSERT_TRUE(input.is_open());
        const std::string bytes{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
        ASSERT_FALSE(input.bad());
        const auto decoded = detail::DecodeOutcome(bytes);
        ASSERT_TRUE(decoded) << decoded.error().message;
        ASSERT_EQ(decoded->disposition, CommitDisposition::Committed);
        ASSERT_TRUE(decoded->result.has_value());
        expected.push_back(*decoded);
        const auto actual = Engine_->FindAppendOutcome(test::collection_id, operation);
        ASSERT_TRUE(actual);
        const auto& actualOutcome = *actual;
        if (!actualOutcome.has_value()) {
            FAIL() << "actualOutcome must have a value";
        }
        const auto encoded = detail::EncodeOutcome(actualOutcome.value());
        ASSERT_TRUE(encoded);
        EXPECT_EQ(*encoded, bytes);
    }
    const auto& firstReceipt = expected[0].result;
    const auto& secondReceipt = expected[1].result;
    if (!firstReceipt.has_value()) {
        FAIL() << "firstReceipt must have a value";
    }
    if (!secondReceipt.has_value()) {
        FAIL() << "secondReceipt must have a value";
    }
    EXPECT_EQ(firstReceipt->rows_appended, 1000);
    EXPECT_EQ(secondReceipt->rows_appended, 10);
    const auto firstSnapshot = firstReceipt->snapshot_id;
    const auto latestSnapshot = secondReceipt->snapshot_id;
    EXPECT_EQ(Engine_->GetCollection(test::collection_id)->current_snapshot_id, latestSnapshot);
    EXPECT_EQ(Read({}), VerticalExpected(true));
    EXPECT_EQ(Read({.selector = SnapshotSelector::Id(firstSnapshot)}), VerticalExpected(false));
    EXPECT_TRUE(Read({.selector = SnapshotSelector::InitialEmpty()}).empty());
    const auto warehouse = Root_ / "warehouse";
    EXPECT_TRUE(std::filesystem::is_regular_file(warehouse / test::collection_id / "data" / OrphanData));
    EXPECT_TRUE(std::filesystem::is_regular_file(warehouse / test::collection_id / "metadata" / OrphanMetadata));
    const auto beforeRetry = FileInventory(warehouse);
    EXPECT_EQ(Append(FirstOperation, VerticalRows(false), {SnapshotPreconditionKind::Equals, firstSnapshot}),
        expected[0]);
    EXPECT_EQ(FileInventory(warehouse), beforeRetry);
    EXPECT_EQ(Engine_->GetCollection(test::collection_id)->current_snapshot_id, latestSnapshot);
}

TEST_F(EngineVerticalTest, PublicHistoryAndReceiptsSurviveFreshProcessWithOrphans)
{
    auto initial = Open({});
    ASSERT_NE(initial, nullptr);
    EXPECT_EQ(initial->Info().selector_resolved, SnapshotSelector::InitialEmpty());
    const auto warehouse = Root_ / "warehouse";
    const auto beforeEmpty = FileInventory(warehouse);
    const auto empty = Append(SecondOperation, {});
    EXPECT_EQ(empty.disposition, CommitDisposition::DefinitelyNotCommitted);
    const auto lookup = Engine_->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(lookup);
    EXPECT_FALSE(lookup->has_value());
    EXPECT_EQ(FileInventory(warehouse), beforeEmpty);
    EXPECT_FALSE(Engine_->GetCollection(test::collection_id)->current_snapshot_id);

    const auto first = Append(FirstOperation, VerticalRows(false));
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    const auto& firstReceipt = first.result;
    if (!firstReceipt.has_value()) {
        FAIL() << "firstReceipt must have a value";
    }
    EXPECT_EQ(firstReceipt->rows_appended, 1000);
    auto retained = Open({});
    ASSERT_NE(retained, nullptr);
    EXPECT_EQ(retained->Info().selector_resolved, SnapshotSelector::Id(firstReceipt->snapshot_id));
    const auto second =
        Append(SecondOperation, VerticalRows(true), {SnapshotPreconditionKind::Equals, firstReceipt->snapshot_id});
    ASSERT_EQ(second.disposition, CommitDisposition::Committed);
    const auto& secondReceipt = second.result;
    if (!secondReceipt.has_value()) {
        FAIL() << "secondReceipt must have a value";
    }
    EXPECT_EQ(secondReceipt->rows_appended, 10);
    EXPECT_EQ(secondReceipt->parent_snapshot_id, firstReceipt->snapshot_id);
    EXPECT_TRUE(Drain(*initial).empty());
    EXPECT_EQ(initial->Info().selector_resolved, SnapshotSelector::InitialEmpty());
    EXPECT_EQ(Sorted(Drain(*retained)), VerticalExpected(false));
    initial.reset();
    retained.reset();

    const auto data = DataFiles();
    ASSERT_EQ(data.size(), 2);
    ASSERT_TRUE(std::filesystem::copy_file(data.front(), data.front().parent_path() / OrphanData));
    const auto metadata = FileInventory(warehouse / test::collection_id / "metadata");
    const auto initialRoot = std::ranges::find_if(metadata, [](const auto& path) {
        return path.filename().string().starts_with("00000-");
    });
    ASSERT_NE(initialRoot, metadata.end());
    ASSERT_TRUE(std::filesystem::copy_file(*initialRoot, initialRoot->parent_path() / OrphanMetadata));
    EXPECT_EQ(Read({}), VerticalExpected(true));
    EXPECT_EQ(Read({.selector = SnapshotSelector::Id(firstReceipt->snapshot_id)}), VerticalExpected(false));
    EXPECT_TRUE(Read({.selector = SnapshotSelector::InitialEmpty()}).empty());

    for (const auto* outcome : {&first, &second}) {
        const auto bytes = detail::EncodeOutcome(*outcome);
        ASSERT_TRUE(bytes);
        std::ofstream output(Temporary_.path / outcome->operation_id, std::ios::binary);
        ASSERT_TRUE(output.is_open());
        output.write(bytes->data(), static_cast<std::streamsize>(bytes->size()));
        output.close();
        ASSERT_TRUE(output.good());
    }
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    ExpectFreshVerticalProcess(Root_, Temporary_.path);
}

TEST_F(EngineVerticalTest, CreatesAnEmptyCollectionRoot)
{
    bool root = false;
    for (const auto& entry :
        std::filesystem::directory_iterator(Root_ / "warehouse" / test::collection_id / "metadata"))
    {
        root = root || entry.path().filename().string().starts_with("00000-");
    }
    EXPECT_TRUE(root);
    EXPECT_FALSE(Engine_->GetCollection(test::collection_id)->current_snapshot_id);

    const auto cursor = Open({});
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(cursor->Info().selector_resolved, SnapshotSelector::InitialEmpty());
    EXPECT_TRUE(cursor->Info().output_schema->Equals(**Engine_->Schema(test::collection_id)));
    EXPECT_TRUE(Drain(*cursor).empty());
}

TEST_F(EngineVerticalTest, AppendsParquetAndScansFixedSnapshots)
{
    const auto first = Committed(FirstOperation, FirstRows);
    EXPECT_EQ(first.rows_appended, 3);
    EXPECT_EQ(first.data_files_added, 1);
    EXPECT_GT(first.compressed_data_bytes_added, 0);
    EXPECT_FALSE(first.parent_snapshot_id);
    EXPECT_EQ(Engine_->GetCollection(test::collection_id)->current_snapshot_id, first.snapshot_id);
    EXPECT_EQ(DataFiles().size(), 1U);

    const auto second = Committed(SecondOperation, SecondRows, {SnapshotPreconditionKind::Equals, first.snapshot_id});
    EXPECT_EQ(second.parent_snapshot_id, first.snapshot_id);

    EXPECT_EQ(Read({}), Expected({&FirstRows, &SecondRows}));
    EXPECT_EQ(Read({.selector = SnapshotSelector::Id(first.snapshot_id)}), Expected({&FirstRows}));
    EXPECT_TRUE(Read({.selector = SnapshotSelector::InitialEmpty()}).empty());
}

TEST_F(EngineVerticalTest, OpenCursorKeepsItsResolvedSnapshot)
{
    const auto first = Committed(FirstOperation, FirstRows);
    const auto cursor = Open({});
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(cursor->Info().selector_resolved, SnapshotSelector::Id(first.snapshot_id));
    Committed(SecondOperation, SecondRows);
    EXPECT_EQ(Sorted(Drain(*cursor)), Expected({&FirstRows}));
    EXPECT_EQ(Read({}), Expected({&FirstRows, &SecondRows}));
}

TEST_F(EngineVerticalTest, FiltersProjectsAndLimits)
{
    Committed(FirstOperation, FirstRows);
    Committed(SecondOperation, SecondRows);

    EXPECT_EQ(Read({.predicate = Predicate::Compare("value", CompareOp::Gt, int64_t{1})}).size(), 2U);
    // NULL compares as UNKNOWN: it is excluded even by "not equal".
    EXPECT_EQ(Read({.predicate = Predicate::Compare("value", CompareOp::Ne, int64_t{2})}).size(), 3U);
    EXPECT_EQ(Read({.predicate = Predicate::Compare("key", CompareOp::Eq, std::string("duplicate"))}).size(), 3U);
    EXPECT_EQ(Read({.predicate = Predicate::Compare("at", CompareOp::Ge, TimestampUs{20})}).size(), 2U);

    const auto keys = Read(
        {.projection = std::vector<std::string>{"key"}, .predicate = Predicate::Compare("flag", CompareOp::Eq, true)});
    EXPECT_EQ(keys, Sorted({"duplicate", "duplicate", "\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87"}));

    const auto limited = Open({.limit = 4, .batch_rows = 1});
    ASSERT_NE(limited, nullptr);
    EXPECT_EQ(limited->Info().output_schema->num_fields(), 5);
    EXPECT_EQ(Drain(*limited, 1).size(), 4U);

    const auto none = Open({.limit = 0});
    ASSERT_NE(none, nullptr);
    EXPECT_TRUE(Drain(*none).empty());
}

TEST_F(EngineVerticalTest, RejectsInvalidScans)
{
    const auto code = [&](const ScanRequest& request) {
        const auto cursor = Engine_->Scan(request);
        return cursor ? std::string() : cursor.error().code;
    };
    EXPECT_EQ(code({.collection_id = "not-a-uuid"}), "INVALID_ARGUMENT");
    EXPECT_EQ(code({.collection_id = test::table_uuid}), "COLLECTION_NOT_FOUND");
    ScanRequest request{.collection_id = test::collection_id};
    request.projection = std::vector<std::string>{};
    EXPECT_EQ(code(request), "INVALID_ARGUMENT");
    request.projection = std::vector<std::string>{"key", "key"};
    EXPECT_EQ(code(request), "INVALID_ARGUMENT");
    EXPECT_EQ(code({.collection_id = test::collection_id, .batch_rows = 0}), "INVALID_ARGUMENT");
    EXPECT_EQ(code({.collection_id = test::collection_id, .limit = -1}), "INVALID_ARGUMENT");
    EXPECT_EQ(
        code({.collection_id = test::collection_id, .predicate = Predicate::Compare("value", CompareOp::Eq, 1.0)}),
        "INVALID_ARGUMENT");
    EXPECT_EQ(code({.collection_id = test::collection_id, .selector = SnapshotSelector::Id(12345)}), "NOT_FOUND");
}

TEST_F(EngineVerticalTest, RetryReturnsTheOriginalReceipt)
{
    const auto first = Append(FirstOperation, FirstRows);
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    EXPECT_EQ(Append(FirstOperation, FirstRows), first);
    const auto conflict = Append(FirstOperation, SecondRows);
    EXPECT_EQ(conflict.error ? conflict.error->code : "", "OPERATION_CONFLICT");
    EXPECT_EQ(Read({}), Expected({&FirstRows}));
}

TEST_F(EngineVerticalTest, ReopenVerifiesAndContinuesTheHistory)
{
    const auto first = Committed(FirstOperation, FirstRows);
    Reopen();
    EXPECT_EQ(Read({}), Expected({&FirstRows}));
    const auto second = Committed(SecondOperation, SecondRows);
    EXPECT_EQ(second.parent_snapshot_id, first.snapshot_id);
    Reopen();
    EXPECT_EQ(Read({}), Expected({&FirstRows, &SecondRows}));
    EXPECT_EQ(Read({.selector = SnapshotSelector::Id(first.snapshot_id)}), Expected({&FirstRows}));
}

TEST_F(EngineVerticalTest, OversizedAppendFailsDefinitely)
{
    std::vector<TInputRow> rows(10001, TInputRow{"k", 1, 1.0, true, 1});
    const auto outcome = Append(FirstOperation, rows);
    EXPECT_EQ(outcome.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(Engine_->GetCollection(test::collection_id)->current_snapshot_id);
    Committed(SecondOperation, SecondRows);
    EXPECT_EQ(Read({}), Expected({&SecondRows}));
}

TEST_F(EngineVerticalTest, CancelAndShutdownStopCursors)
{
    Committed(FirstOperation, FirstRows);
    const auto cancelled = Open({});
    ASSERT_NE(cancelled, nullptr);
    cancelled->Cancel();
    const auto next = cancelled->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");

    const auto open = Open({});
    ASSERT_NE(open, nullptr);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    EXPECT_EQ(open->Info().output_schema->num_fields(), 5);
    const auto stopped = open->Next();
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error().code, "CANCELLED");
}

TEST_F(EngineVerticalTest, MissingCommittedDataFailsClosed)
{
    Committed(FirstOperation, FirstRows);
    Committed(ThirdOperation, SecondRows);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto files = DataFiles();
    ASSERT_EQ(files.size(), 2U);
    std::filesystem::remove(files.front());
    EXPECT_FALSE(OpenExisting({Root_.string(), test::store_id}));
}

TEST_F(EngineVerticalTest, CursorDetectedCorruptionClosesReadiness)
{
    Committed(FirstOperation, FirstRows);
    const auto cursor = Open({});
    ASSERT_NE(cursor, nullptr);

    const auto files = DataFiles();
    ASSERT_EQ(files.size(), 1U);
    std::filesystem::remove(files.front());

    const auto next = cursor->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CORRUPTION");

    const auto collection = Engine_->GetCollection(test::collection_id);
    ASSERT_FALSE(collection);
    EXPECT_EQ(collection.error().code, "RECOVERY_REQUIRED");

    const auto blocked = Append(SecondOperation, SecondRows);
    EXPECT_EQ(blocked.disposition, CommitDisposition::CommitUnknown);
    const auto& blockedError = blocked.error;
    if (!blockedError.has_value()) {
        FAIL() << "blockedError must have a value";
    }
    EXPECT_EQ(blockedError->code, "RECOVERY_REQUIRED");
    Engine_.reset();
}

TEST_F(EngineVerticalTest, ReturnedArrowBuffersRetainStoreLeaseAfterCursorAndEngineRelease)
{
    Committed(FirstOperation, FirstRows);
    auto cursor = Open({});
    ASSERT_NE(cursor, nullptr);

    auto batch = cursor->Next();
    ASSERT_TRUE(batch);
    ASSERT_NE(*batch, nullptr);

    auto slice = (*batch)->column(0)->Slice(0, 1);
    ArrowArray exported{};
    ASSERT_TRUE(arrow::ExportArray(*(*batch)->column(1), &exported).ok());

    batch = std::shared_ptr<arrow::RecordBatch>{};
    cursor.reset();
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();

    const auto lockedBySlice = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(lockedBySlice);
    EXPECT_EQ(lockedBySlice.error().code, "STATE_IN_USE");

    slice.reset();
    const auto lockedByExport = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(lockedByExport);
    EXPECT_EQ(lockedByExport.error().code, "STATE_IN_USE");

    if (exported.release != nullptr) {
        exported.release(&exported);
    }

    auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().code << ": " << reopened.error().message;
    Engine_ = *reopened;
}

TEST_F(EngineVerticalTest, ZeroLimitCursorChecksCancellationBeforeReturningEos)
{
    Committed(FirstOperation, FirstRows);
    const auto zeroLimit = Open({.limit = 0});
    ASSERT_NE(zeroLimit, nullptr);
    zeroLimit->Cancel();
    const auto next = zeroLimit->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CANCELLED");
}

TEST_F(EngineVerticalTest, PreBindingWriterRowLimitDoesNotConsumeOperationId)
{
    constexpr char WideCollection[] = "00000000-0000-4000-8000-0000000000c1";
    const SchemaSpec wideSchema{{
        {"s1", FieldType::String, false},
        {"s2", FieldType::String, false},
        {"n", FieldType::Int64, true},
    }};
    ASSERT_EQ(Engine_->CreateCollection({WideCollection, "wide", wideSchema}).disposition,
        CommitDisposition::Committed);

    const auto arrowSchema = arrow::schema({
        arrow::field("s1", arrow::utf8(), false),
        arrow::field("s2", arrow::utf8(), false),
        arrow::field("n", arrow::int64(), true),
    });
    const auto oversizedBatch = arrow::RecordBatch::Make(arrowSchema,
        1,
        {
            Column<arrow::StringBuilder>(std::vector<std::optional<std::string>>{std::string(1048576, 'a')},
                arrow::utf8()),
            Column<arrow::StringBuilder>(std::vector<std::optional<std::string>>{std::string(1048561, 'b')},
                arrow::utf8()),
            Column<arrow::Int64Builder>(std::vector<std::optional<int64_t>>{std::nullopt}, arrow::int64()),
        });

    const auto rejected = Engine_->AppendDurable({WideCollection, FirstOperation, {}, {oversizedBatch}});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    const auto& rejectedError = rejected.error;
    if (!rejectedError.has_value()) {
        FAIL() << "rejectedError must have a value";
    }
    EXPECT_EQ(rejectedError->code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(rejectedError->retry_hint, RetryHint::FixInput);

    const auto lookup = Engine_->FindAppendOutcome(WideCollection, FirstOperation);
    ASSERT_TRUE(lookup);
    EXPECT_FALSE(lookup->has_value());

    const auto validBatch = arrow::RecordBatch::Make(arrowSchema,
        1,
        {
            Column<arrow::StringBuilder>(std::vector<std::optional<std::string>>{std::string("ok")}, arrow::utf8()),
            Column<arrow::StringBuilder>(std::vector<std::optional<std::string>>{std::string("row")}, arrow::utf8()),
            Column<arrow::Int64Builder>(std::vector<std::optional<int64_t>>{42}, arrow::int64()),
        });
    const auto committed = Engine_->AppendDurable({WideCollection, FirstOperation, {}, {validBatch}});
    EXPECT_EQ(committed.disposition, CommitDisposition::Committed);
}

TEST_F(EngineVerticalTest, MultiCollectionStoreCapacityEnforcedAcrossCommitsAndRecovery)
{
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();

    const auto customRoot = Temporary_.path / "capped-store";
    const auto countWarehouseFiles = [](const std::filesystem::path& warehouse) {
        std::size_t count = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(warehouse)) {
            if (entry.is_regular_file()) {
                ++count;
            }
        }
        return count;
    };
    const auto makeFactory = [](std::size_t maxSnapshots) -> detail::BackendFactory {
        return [maxSnapshots](const std::filesystem::path& path,
                   const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Storage.Store.Snapshots = maxSnapshots;
            auto backend = detail::TLocalStorageBackend::Open(path, info, options);
            if (!backend) {
                return std::unexpected(backend.error());
            }
            return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
        };
    };

    constexpr char SecondCollection[] = "00000000-0000-4000-8000-0000000000b2";
    auto capped = detail::EngineFactory::CreateWith({test::store_id, customRoot.string()}, makeFactory(1));
    ASSERT_TRUE(capped) << capped.error().message;
    auto engine = std::move(*capped);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events-a", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->CreateCollection({SecondCollection, "events-b", Schema}).disposition,
        CommitDisposition::Committed);

    const auto first = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    const auto filesBefore = countWarehouseFiles(customRoot / "warehouse");

    const auto second = engine->AppendDurable({SecondCollection, SecondOperation, {}, {Batch(SecondRows)}});
    EXPECT_EQ(second.disposition, CommitDisposition::DefinitelyNotCommitted);
    const auto& secondError = second.error;
    if (!secondError.has_value()) {
        FAIL() << "secondError must have a value";
    }
    EXPECT_EQ(secondError->code, "RESOURCE_EXHAUSTED");
    const auto secondLookup = engine->FindAppendOutcome(SecondCollection, SecondOperation);
    ASSERT_TRUE(secondLookup);
    EXPECT_FALSE(secondLookup->has_value());
    EXPECT_EQ(countWarehouseFiles(customRoot / "warehouse"), filesBefore);
    EXPECT_FALSE(engine->GetCollection(SecondCollection)->current_snapshot_id);
    ASSERT_TRUE(engine->Shutdown(1000ms));
    engine.reset();

    auto wider = detail::EngineFactory::OpenWith({customRoot.string(), test::store_id}, makeFactory(2));
    ASSERT_TRUE(wider) << wider.error().message;
    engine = std::move(*wider);
    ASSERT_EQ(engine->AppendDurable({SecondCollection, SecondOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
    ASSERT_TRUE(engine->Shutdown(1000ms));
    engine.reset();

    const auto tooSmall = detail::EngineFactory::OpenWith({customRoot.string(), test::store_id}, makeFactory(1));
    ASSERT_FALSE(tooSmall);
    EXPECT_EQ(tooSmall.error().code, "CORRUPTION");
}

TEST_F(EngineVerticalTest, ConcurrentCursorCorruptionAbortsPreparedAppendPublication)
{
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();

    class TBarrierBackend final : public detail::StorageBackend
    {
    public:
        explicit TBarrierBackend(std::shared_ptr<detail::StorageBackend> inner)
            : Inner_(std::move(inner))
        { }

        void ArmBarrier(std::string targetCollection)
        {
            TargetCollection_ = std::move(targetCollection);
        }

        void WaitEntered()
        {
            Entered_.get_future().wait();
        }

        void Release()
        {
            Continue_.set_value();
        }

        Result<std::unique_ptr<detail::StagedCreate>>
        PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt) override
        {
            return Inner_->PrepareCreate(store, request, attempt);
        }

        Status CheckAppendAdmissible(const StoreInfo& store,
            const CollectionInfo& collection,
            const detail::NormalizedInput& input) override
        {
            return Inner_->CheckAppendAdmissible(store, collection, input);
        }

        Result<std::unique_ptr<detail::StagedAppend>> PrepareAppend(const StoreInfo& store,
            const detail::BoundOperation& binding,
            const CollectionInfo& collection,
            const detail::NormalizedInput& input,
            std::chrono::steady_clock::time_point deadline) override
        {
            auto staged = Inner_->PrepareAppend(store, binding, collection, input, deadline);
            if (staged && binding.collection_id == TargetCollection_) {
                Entered_.set_value();
                Continue_.get_future().wait();
            }
            return staged;
        }

        void ConfirmPublication(std::string_view collectionID, std::string_view location) override
        {
            Inner_->ConfirmPublication(collectionID, location);
        }

        void InvalidateCache() override
        {
            Inner_->InvalidateCache();
        }

        Status VerifyRetained(const StoreInfo& store,
            std::span<const detail::CollectionView> collections,
            std::span<const detail::OperationRecord> operations) override
        {
            return Inner_->VerifyRetained(store, collections, operations);
        }

        Result<std::shared_ptr<iceberg::FileIO>> ReadOnlyFileIO() override
        {
            return Inner_->ReadOnlyFileIO();
        }

    private:
        std::shared_ptr<detail::StorageBackend> Inner_;
        std::string TargetCollection_;
        std::promise<void> Entered_;
        std::promise<void> Continue_;
    };

    const auto customRoot = Temporary_.path / "concurrent-corruption-store";
    std::shared_ptr<TBarrierBackend> barrierBackend;
    const detail::BackendFactory factory =
        [&](const std::filesystem::path& path,
            const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
        auto inner = detail::TLocalStorageBackend::Open(path, info);
        if (!inner) {
            return std::unexpected(inner.error());
        }
        barrierBackend = std::make_shared<TBarrierBackend>(std::move(*inner));
        return barrierBackend;
    };

    constexpr char SecondCollection[] = "00000000-0000-4000-8000-0000000000b2";
    auto created = detail::EngineFactory::CreateWith({test::store_id, customRoot.string()}, factory);
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events-a", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->CreateCollection({SecondCollection, "events-b", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);

    auto cursor = engine->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor) << cursor.error().message;

    std::filesystem::path dataFile;
    for (const auto& entry :
        std::filesystem::directory_iterator(customRoot / "warehouse" / test::collection_id / "data"))
    {
        dataFile = entry.path();
    }
    ASSERT_FALSE(dataFile.empty());
    const auto backupPath = Temporary_.path / "saved.parquet";
    std::filesystem::copy_file(dataFile, backupPath);

    barrierBackend->ArmBarrier(SecondCollection);
    auto writerFuture = std::async(std::launch::async, [&] {
        return engine->AppendDurable({SecondCollection, SecondOperation, {}, {Batch(SecondRows)}});
    });
    barrierBackend->WaitEntered();

    std::filesystem::remove(dataFile);
    const auto next = (*cursor)->Next();
    ASSERT_FALSE(next);
    EXPECT_EQ(next.error().code, "CORRUPTION");
    cursor->reset();

    barrierBackend->Release();
    const auto outcomeB = writerFuture.get();
    EXPECT_EQ(outcomeB.disposition, CommitDisposition::DefinitelyNotCommitted);
    const auto& outcomeError = outcomeB.error;
    if (!outcomeError.has_value()) {
        FAIL() << "outcomeError must have a value";
    }
    EXPECT_EQ(outcomeError->code, "RECOVERY_REQUIRED");

    const auto blockedGet = engine->GetCollection(SecondCollection);
    ASSERT_FALSE(blockedGet);
    EXPECT_EQ(blockedGet.error().code, "RECOVERY_REQUIRED");

    std::filesystem::copy_file(backupPath, dataFile);
    ASSERT_TRUE(engine->Reconcile());
    EXPECT_FALSE(engine->GetCollection(SecondCollection)->current_snapshot_id);
    const auto lookupB = engine->FindAppendOutcome(SecondCollection, SecondOperation);
    ASSERT_TRUE(lookupB);
    const auto& storedOutcome = *lookupB;
    if (!storedOutcome.has_value()) {
        FAIL() << "storedOutcome must have a value";
    }
    EXPECT_EQ(storedOutcome->disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(engine->Shutdown(1000ms));
}

TEST_F(EngineVerticalTest, MalformedAvroManifestAndOutOfRangeTimestampFailClosedAsCorruption)
{
    Committed(FirstOperation, FirstRows);

    std::filesystem::path manifestPath;
    std::filesystem::path metadataPath;
    for (const auto& entry :
        std::filesystem::directory_iterator(Root_ / "warehouse" / test::collection_id / "metadata"))
    {
        const auto filename = entry.path().filename().string();
        if (entry.path().extension() == ".avro" && !filename.starts_with("snap-")) {
            manifestPath = entry.path();
        } else if (filename.starts_with("00001-") && filename.ends_with(".metadata.json")) {
            metadataPath = entry.path();
        }
    }
    ASSERT_FALSE(manifestPath.empty());
    ASSERT_FALSE(metadataPath.empty());

    std::string manifestBytes;
    {
        std::ifstream input(manifestPath, std::ios::binary);
        manifestBytes.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    ASSERT_FALSE(manifestBytes.empty());
    {
        auto corrupted = manifestBytes;
        corrupted[0] = 'X';
        std::ofstream output(manifestPath, std::ios::binary | std::ios::trunc);
        output.write(corrupted.data(), static_cast<std::streamsize>(corrupted.size()));
    }

    const auto scan = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_FALSE(scan);
    EXPECT_EQ(scan.error().code, "CORRUPTION");
    const auto afterManifestCorruption = Engine_->ListCollections();
    ASSERT_FALSE(afterManifestCorruption);
    EXPECT_EQ(afterManifestCorruption.error().code, "RECOVERY_REQUIRED");

    {
        std::ofstream output(manifestPath, std::ios::binary | std::ios::trunc);
        output.write(manifestBytes.data(), static_cast<std::streamsize>(manifestBytes.size()));
    }
    ASSERT_TRUE(Engine_->Reconcile());

    std::string metadataJson;
    {
        std::ifstream input(metadataPath, std::ios::binary);
        metadataJson.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    const std::string key = "\"timestamp-ms\"";
    const auto keyPos = metadataJson.find(key);
    ASSERT_NE(keyPos, std::string::npos);
    const auto colonPos = metadataJson.find(':', keyPos + key.size());
    ASSERT_NE(colonPos, std::string::npos);
    const auto valueStart = metadataJson.find_first_of("-0123456789", colonPos + 1);
    ASSERT_NE(valueStart, std::string::npos);
    const auto valueEnd = metadataJson.find_first_not_of("-0123456789", valueStart);
    ASSERT_NE(valueEnd, std::string::npos);
    {
        auto badMetadata = metadataJson;
        badMetadata.replace(valueStart, valueEnd - valueStart, "-62135596800001");
        std::ofstream output(metadataPath, std::ios::binary | std::ios::trunc);
        output.write(badMetadata.data(), static_cast<std::streamsize>(badMetadata.size()));
    }

    const auto badReconcile = Engine_->Reconcile();
    ASSERT_FALSE(badReconcile);
    EXPECT_EQ(badReconcile.error().code, "CORRUPTION");

    {
        std::ofstream output(metadataPath, std::ios::binary | std::ios::trunc);
        output.write(metadataJson.data(), static_cast<std::streamsize>(metadataJson.size()));
    }
    ASSERT_TRUE(Engine_->Reconcile());
}

} // namespace
} // namespace glamdb
