#include "tests/vertical/vertical_support.h"

#include "core/api/write_settings.h"
#include "core/catalog/sqlite.h"
#include "tests/support/checked.h"

#include <arrow/util/compression.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <chrono>
#include <condition_variable>
#include <format>
#include <future>
#include <limits>
#include <map>
#include <mutex>
#include <set>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

WriteSettings Settings(std::optional<ParquetCompression> fallback, std::vector<ColumnWriteSettings> columns = {})
{
    WriteSettings settings;
    settings.compression = fallback;
    settings.columns = std::move(columns);
    return settings;
}

std::vector<std::filesystem::path> DataFiles(const std::filesystem::path& root)
{
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root / "warehouse")) {
        if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
            files.push_back(entry.path());
        }
    }
    return files;
}

// Independent of the engine: the codec each column chunk of the file whose
// name contains `operation` records in its own footer.
std::map<std::string, std::string> FooterCodecs(const std::filesystem::path& root, std::string_view operation)
{
    std::map<std::string, std::string> codecs;
    for (const auto& path : DataFiles(root)) {
        if (path.filename().string().find(operation) == std::string::npos) {
            continue;
        }
        const auto reader = parquet::ParquetFileReader::OpenFile(path.string());
        const auto footer = reader->metadata();
        for (int group = 0; group < footer->num_row_groups(); ++group) {
            for (int column = 0; column < footer->num_columns(); ++column) {
                const auto name = footer->schema()->Column(column)->name();
                const auto codec =
                    arrow::util::Codec::GetCodecAsString(footer->RowGroup(group)->ColumnChunk(column)->compression());
                const auto [it, inserted] = codecs.emplace(name, codec);
                EXPECT_EQ(it->second, codec) << name << " differs between row groups";
            }
        }
    }
    return codecs;
}

struct TChunkEncodings
{
    bool Dictionary = false;
    std::set<std::string> DataPages;
    bool operator==(const TChunkEncodings&) const = default;
};

// Independent of the engine: whether each column chunk of one append's file
// has a dictionary page, and the encodings of its data pages.
std::map<std::string, TChunkEncodings> FooterEncodings(const std::filesystem::path& root, std::string_view operation)
{
    std::map<std::string, TChunkEncodings> result;
    for (const auto& path : DataFiles(root)) {
        if (path.filename().string().find(operation) == std::string::npos) {
            continue;
        }
        const auto reader = parquet::ParquetFileReader::OpenFile(path.string());
        const auto footer = reader->metadata();
        for (int group = 0; group < footer->num_row_groups(); ++group) {
            for (int column = 0; column < footer->num_columns(); ++column) {
                const auto chunk = footer->RowGroup(group)->ColumnChunk(column);
                auto& item = result[footer->schema()->Column(column)->name()];
                item.Dictionary = chunk->has_dictionary_page();
                for (const auto& stats : chunk->encoding_stats()) {
                    if (stats.page_type != parquet::PageType::DICTIONARY_PAGE && stats.count > 0) {
                        item.DataPages.insert(parquet::EncodingToString(stats.encoding));
                    }
                }
            }
        }
    }
    return result;
}

class TWriteSettingsTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<THookBackend> Backend_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        auto created =
            detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, THookBackend::Factory(Backend_));
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = *created;
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    void Reopen()
    {
        ASSERT_TRUE(Engine_->Shutdown(1000ms));
        Engine_.reset();
        auto opened =
            detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Backend_));
        ASSERT_TRUE(opened) << opened.error().message;
        Engine_ = *opened;
    }

    CreateOutcome Create(WriteSettings settings)
    {
        return Engine_->CreateCollection({test::collection_id, "events", Schema, std::move(settings)});
    }

    AppendResult Append(const char* operation, const std::vector<TInputRow>& rows)
    {
        const auto result = Engine_->AppendDurable({test::collection_id, operation, {}, {Batch(rows)}});
        EXPECT_EQ(result.disposition, CommitDisposition::Committed)
            << (result.error ? result.error->code + ": " + result.error->message : std::string());
        return result.result.value_or(AppendResult{});
    }

    std::vector<std::string> ScanRows(SnapshotSelector selector = SnapshotSelector::Latest())
    {
        auto cursor = Engine_->Scan({.collection_id = test::collection_id, .selector = selector});
        EXPECT_TRUE(cursor) << cursor.error().message;
        std::vector<std::string> rows;
        if (!cursor) {
            return rows;
        }
        while (true) {
            auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch) << batch.error().message;
            if (!batch || !*batch) {
                break;
            }
            const auto rendered = Render(**batch);
            rows.insert(rows.end(), rendered.begin(), rendered.end());
        }
        return Sorted(std::move(rows));
    }

    int64_t CatalogVersion()
    {
        detail::Database database;
        database.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READONLY, {});
        return database.Integer("PRAGMA user_version");
    }
};

TEST_F(TWriteSettingsTest, DefaultsKeepCatalogVersionAndWriterDefaults)
{
    const auto created = Create({});
    ASSERT_EQ(created.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(created.info);
    EXPECT_EQ(test::Present(created.info).write_settings, WriteSettings{});
    EXPECT_EQ(test::Present(created.info).write_settings_version, 0);
    Append(FirstOperation, FirstRows);
    for (const auto& [column, codec] : FooterCodecs(Root_, FirstOperation)) {
        EXPECT_EQ(codec, "zstd") << column;
    }
    EXPECT_EQ(CatalogVersion(), 1);

    const auto capabilities = Engine_->GetCapabilities();
    EXPECT_TRUE(capabilities.write_settings);
    EXPECT_EQ(capabilities.default_parquet_compression, ParquetCompression{});
    const auto zstd =
        std::ranges::find(capabilities.parquet_codecs, ParquetCodec::Zstd, &ParquetCodecCapability::codec);
    ASSERT_NE(zstd, capabilities.parquet_codecs.end());
    EXPECT_EQ(zstd->min_level, 1);
    EXPECT_EQ(zstd->max_level, 22);
    EXPECT_EQ(zstd->default_level, 1);
    const auto snappy =
        std::ranges::find(capabilities.parquet_codecs, ParquetCodec::Snappy, &ParquetCodecCapability::codec);
    ASSERT_NE(snappy, capabilities.parquet_codecs.end());
    EXPECT_FALSE(snappy->min_level || snappy->max_level || snappy->default_level);
    EXPECT_EQ(capabilities.parquet_codecs.size(), 5U);
}

TEST_F(TWriteSettingsTest, CollectionDefaultAndColumnOverridesReachEveryFooter)
{
    const auto settings = Settings(ParquetCompression{ParquetCodec::Gzip, 6},
        {{"score", ParquetCompression{ParquetCodec::Snappy, std::nullopt}},
            {"key", ParquetCompression{ParquetCodec::Uncompressed, std::nullopt}},
            {"at", ParquetCompression{ParquetCodec::Lz4Raw, 9}}});
    const auto created = Create(settings);
    ASSERT_EQ(created.disposition, CommitDisposition::Committed)
        << (created.error ? test::Present(created.error).message : std::string());
    // Canonical form follows schema order.
    const auto expected = Settings(ParquetCompression{ParquetCodec::Gzip, 6},
        {{"key", ParquetCompression{ParquetCodec::Uncompressed, std::nullopt}},
            {"score", ParquetCompression{ParquetCodec::Snappy, std::nullopt}},
            {"at", ParquetCompression{ParquetCodec::Lz4Raw, 9}}});
    EXPECT_EQ(test::Present(created.info).write_settings, expected);
    EXPECT_EQ(test::Present(created.info).write_settings_version, 1);
    EXPECT_EQ(CatalogVersion(), 2);

    Append(FirstOperation, FirstRows);
    const std::map<std::string, std::string> codecs{{"key", "uncompressed"},
        {"value", "gzip"},
        {"score", "snappy"},
        {"flag", "gzip"},
        {"at", "lz4_raw"}};
    EXPECT_EQ(FooterCodecs(Root_, FirstOperation), codecs);
    EXPECT_EQ(ScanRows(), Expected({&FirstRows}));

    // The engine's own file details report the same footer codecs.
    const auto files = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(files);
    ASSERT_EQ(files->Files.size(), 1U);
    const auto details = Engine_->GetFileDetails(test::collection_id, files->Files[0].FileID);
    ASSERT_TRUE(details) << details.error().message;
    for (const auto& group : details->RowGroups) {
        for (const auto& column : group.Columns) {
            EXPECT_EQ(column.Compression, codecs.at(Schema.fields[static_cast<size_t>(column.FieldID - 1)].name));
        }
    }
}

TEST_F(TWriteSettingsTest, UpdatesApplyToLaterFilesAndHistoryStaysReadable)
{
    ASSERT_EQ(Create({}).disposition, CommitDisposition::Committed);
    const auto first = Append(FirstOperation, FirstRows);

    const auto updated = Engine_->UpdateWriteSettings({test::collection_id,
        0,
        Settings(ParquetCompression{ParquetCodec::Uncompressed, std::nullopt},
            {{"value", ParquetCompression{ParquetCodec::Zstd, 19}}})});
    ASSERT_TRUE(updated) << updated.error().message;
    EXPECT_EQ(updated->write_settings_version, 1);
    EXPECT_EQ(Engine_->GetCollection(test::collection_id)->write_settings, updated->write_settings);
    Append(SecondOperation, SecondRows);

    // Mixed-codec files in one history; old files keep their footer codecs.
    for (const auto& [column, codec] : FooterCodecs(Root_, FirstOperation)) {
        EXPECT_EQ(codec, "zstd") << column;
    }
    const std::map<std::string, std::string> second{{"key", "uncompressed"},
        {"value", "zstd"},
        {"score", "uncompressed"},
        {"flag", "uncompressed"},
        {"at", "uncompressed"}};
    EXPECT_EQ(FooterCodecs(Root_, SecondOperation), second);
    EXPECT_EQ(ScanRows(), Expected({&FirstRows, &SecondRows}));
    EXPECT_EQ(ScanRows(SnapshotSelector::Id(first.snapshot_id)), Expected({&FirstRows}));

    // Resetting to defaults is a version too; restart preserves everything.
    const auto reset = Engine_->UpdateWriteSettings({test::collection_id, 1, {}});
    ASSERT_TRUE(reset) << reset.error().message;
    EXPECT_EQ(reset->write_settings_version, 2);
    EXPECT_EQ(reset->write_settings, WriteSettings{});
    Reopen();
    const auto reopened = Engine_->GetCollection(test::collection_id);
    ASSERT_TRUE(reopened);
    EXPECT_EQ(reopened->write_settings_version, 2);
    EXPECT_EQ(reopened->write_settings, WriteSettings{});
    Append(ThirdOperation, FirstRows);
    for (const auto& [column, codec] : FooterCodecs(Root_, ThirdOperation)) {
        EXPECT_EQ(codec, "zstd") << column;
    }
    EXPECT_EQ(ScanRows(), Expected({&FirstRows, &SecondRows, &FirstRows}));
    EXPECT_EQ(ScanRows(SnapshotSelector::Id(first.snapshot_id)), Expected({&FirstRows}));
}

TEST_F(TWriteSettingsTest, CompareAndSetRetriesAndConflicts)
{
    ASSERT_EQ(Create({}).disposition, CommitDisposition::Committed);
    const auto settings = Settings(ParquetCompression{ParquetCodec::Snappy, std::nullopt});
    const auto applied = Engine_->UpdateWriteSettings({test::collection_id, 0, settings});
    ASSERT_TRUE(applied);
    EXPECT_EQ(applied->write_settings_version, 1);

    // The same request after a lost response is recognized as applied.
    const auto repeated = Engine_->UpdateWriteSettings({test::collection_id, 0, settings});
    ASSERT_TRUE(repeated) << repeated.error().message;
    EXPECT_EQ(*repeated, *applied);

    // Different content from the same stale version is a conflict.
    const auto stale = Engine_->UpdateWriteSettings(
        {test::collection_id, 0, Settings(ParquetCompression{ParquetCodec::Gzip, std::nullopt})});
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error().code, "WRITE_SETTINGS_CONFLICT");
    const auto ahead = Engine_->UpdateWriteSettings({test::collection_id, 5, settings});
    ASSERT_FALSE(ahead);
    EXPECT_EQ(ahead.error().code, "WRITE_SETTINGS_CONFLICT");
    EXPECT_EQ(Engine_->GetCollection(test::collection_id)->write_settings_version, 1);
}

TEST_F(TWriteSettingsTest, RepeatedCreateComparesCreationSettings)
{
    const auto settings = Settings(ParquetCompression{ParquetCodec::Zstd, 3});
    ASSERT_EQ(Create(settings).disposition, CommitDisposition::Committed);
    ASSERT_TRUE(Engine_->UpdateWriteSettings({test::collection_id, 1, {}}));

    // A retried creation compares with the settings it was created with, not
    // with later changes.
    const auto repeated = Create(settings);
    ASSERT_EQ(repeated.disposition, CommitDisposition::Committed);
    EXPECT_EQ(test::Present(repeated.info).write_settings_version, 2);
    const auto different = Create(Settings(ParquetCompression{ParquetCodec::Zstd, 4}));
    EXPECT_EQ(different.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(different.error);
    EXPECT_EQ(test::Present(different.error).code, "WRITE_SETTINGS_MISMATCH");
    const auto defaults = Create({});
    ASSERT_TRUE(defaults.error);
    EXPECT_EQ(test::Present(defaults.error).code, "WRITE_SETTINGS_MISMATCH");
}

TEST_F(TWriteSettingsTest, InvalidSettingsAreRejectedBeforeAnythingIsBound)
{
    const std::vector<std::pair<WriteSettings, std::string>> invalid{
        {Settings(ParquetCompression{ParquetCodec::Zstd, 23}), "INVALID_ARGUMENT"},
        {Settings(ParquetCompression{ParquetCodec::Zstd, 0}), "INVALID_ARGUMENT"},
        {Settings(ParquetCompression{ParquetCodec::Gzip, 10}), "INVALID_ARGUMENT"},
        {Settings(ParquetCompression{ParquetCodec::Snappy, 1}), "INVALID_ARGUMENT"},
        {Settings(ParquetCompression{ParquetCodec::Uncompressed, 1}), "INVALID_ARGUMENT"},
        // Deliberately invalid enum input exercises public validation.
        // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
        {Settings(ParquetCompression{static_cast<ParquetCodec>(9), std::nullopt}), "INVALID_ARGUMENT"},
        {Settings(std::nullopt, {{"missing", ParquetCompression{}}}), "INVALID_ARGUMENT"},
        {Settings(std::nullopt, {{"key", ParquetCompression{}}, {"key", ParquetCompression{}}}), "INVALID_ARGUMENT"},
        {Settings(std::nullopt, {{"key", std::nullopt}}), "INVALID_ARGUMENT"},
    };
    for (const auto& [settings, code] : invalid) {
        const auto created = Create(settings);
        EXPECT_EQ(created.disposition, CommitDisposition::DefinitelyNotCommitted);
        ASSERT_TRUE(created.error);
        EXPECT_EQ(test::Present(created.error).code, code);
        EXPECT_EQ(test::Present(created.error).retry_hint, RetryHint::FixInput);
    }
    EXPECT_TRUE(Engine_->ListCollections()->empty());
    EXPECT_EQ(CatalogVersion(), 1);

    ASSERT_EQ(Create({}).disposition, CommitDisposition::Committed);
    for (const auto& [settings, code] : invalid) {
        const auto updated = Engine_->UpdateWriteSettings({test::collection_id, 0, settings});
        ASSERT_FALSE(updated);
        EXPECT_EQ(updated.error().code, code);
    }
    const auto negative = Engine_->UpdateWriteSettings({test::collection_id, -1, {}});
    ASSERT_FALSE(negative);
    EXPECT_EQ(negative.error().code, "INVALID_ARGUMENT");
    // The next version would not be representable; never computed.
    const auto maximum = Engine_->UpdateWriteSettings({test::collection_id, std::numeric_limits<int64_t>::max(), {}});
    ASSERT_FALSE(maximum);
    EXPECT_EQ(maximum.error().code, "INVALID_ARGUMENT");
    EXPECT_EQ(maximum.error().retry_hint, RetryHint::FixInput);
    const auto unknown = Engine_->UpdateWriteSettings({SecondCollection, 0, {}});
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().code, "COLLECTION_NOT_FOUND");
    EXPECT_EQ(Engine_->GetCollection(test::collection_id)->write_settings_version, 0);
    EXPECT_EQ(CatalogVersion(), 1);
}

TEST_F(TWriteSettingsTest, BoundAppendKeepsTheSettingsItRead)
{
    ASSERT_EQ(Create({}).disposition, CommitDisposition::Committed);
    std::mutex mutex;
    std::condition_variable changed;
    bool entered = false;
    bool released = false;
    Backend_->AfterPrepareAppend = [&](const detail::BoundOperation&) {
        std::unique_lock lock(mutex);
        entered = true;
        changed.notify_all();
        changed.wait(lock, [&] {
            return released;
        });
    };
    auto append = std::async(std::launch::async, [&] {
        return Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    });
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(changed.wait_for(lock, 5s, [&] {
            return entered;
        }));
    }
    // The append holds the writer between binding and publication.
    auto update = std::async(std::launch::async, [&] {
        return Engine_->UpdateWriteSettings(
            {test::collection_id, 0, Settings(ParquetCompression{ParquetCodec::Uncompressed, std::nullopt})});
    });
    EXPECT_EQ(update.wait_for(200ms), std::future_status::timeout);
    {
        std::lock_guard lock(mutex);
        released = true;
        changed.notify_all();
    }
    EXPECT_EQ(append.get().disposition, CommitDisposition::Committed);
    const auto updated = update.get();
    ASSERT_TRUE(updated) << updated.error().message;
    Backend_->AfterPrepareAppend = {};
    for (const auto& [column, codec] : FooterCodecs(Root_, FirstOperation)) {
        EXPECT_EQ(codec, "zstd") << column;
    }
    Append(SecondOperation, SecondRows);
    for (const auto& [column, codec] : FooterCodecs(Root_, SecondOperation)) {
        EXPECT_EQ(codec, "uncompressed") << column;
    }
    // Retrying the first operation returns its original receipt unchanged.
    const auto retried = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(retried.disposition, CommitDisposition::Committed);
    EXPECT_EQ(ScanRows(), Expected({&FirstRows, &SecondRows}));
}

TEST_F(TWriteSettingsTest, CorruptStoredSettingsCloseTheStore)
{
    ASSERT_EQ(Create(Settings(ParquetCompression{ParquetCodec::Snappy, std::nullopt})).disposition,
        CommitDisposition::Committed);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    {
        detail::Database database;
        database.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        database.Execute("UPDATE collection_write_settings SET current_blob=X'00'");
    }
    auto opened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Backend_));
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION");
}

TEST_F(TWriteSettingsTest, NewerCatalogVersionsFailClosed)
{
    ASSERT_EQ(Create({}).disposition, CommitDisposition::Committed);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    {
        detail::Database database;
        database.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        database.Execute("PRAGMA user_version=2");
    }
    // Version 2 without its settings table is not a catalog this build wrote.
    auto opened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Backend_));
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION");
    {
        detail::Database database;
        database.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        database.Execute("PRAGMA user_version=3");
    }
    // Version 3 also requires the write-settings and retention tables.
    opened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Backend_));
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION");
    {
        detail::Database database;
        database.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        database.Execute("PRAGMA user_version=5");
    }
    // Version 5 also requires the KV and snapshot archive tables.
    opened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Backend_));
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION");
    {
        detail::Database database;
        database.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        database.Execute("PRAGMA user_version=6");
    }
    // Version 6 also requires the checkpoint table and the bound-operation index.
    opened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Backend_));
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION");
    {
        detail::Database database;
        database.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        database.Execute("PRAGMA user_version=7");
    }
    opened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Backend_));
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "UNSUPPORTED_STORE_VERSION");
}

TEST_F(TWriteSettingsTest, DefaultsUseDictionariesExceptForBool)
{
    ASSERT_EQ(Create({}).disposition, CommitDisposition::Committed);
    Append(FirstOperation, FirstRows);
    const TChunkEncodings dictionary{true, {"RLE_DICTIONARY"}};
    const std::map<std::string, TChunkEncodings> expected{{"key", dictionary},
        {"value", dictionary},
        {"score", dictionary},
        {"flag", {false, {"PLAIN"}}},
        {"at", dictionary}};
    EXPECT_EQ(FooterEncodings(Root_, FirstOperation), expected);
    const auto capabilities = Engine_->GetCapabilities();
    EXPECT_EQ(capabilities.parquet_encodings.size(), 6U);
    EXPECT_EQ(capabilities.dictionary_types,
        (std::vector<FieldType>{FieldType::String,
            FieldType::Int64,
            FieldType::Float64,
            FieldType::TimestampUsUtc,
            FieldType::Decimal,
            FieldType::Date}));
    EXPECT_EQ(capabilities.default_dictionary_page_size_limit, 1 << 20);
}

TEST_F(TWriteSettingsTest, ColumnEncodingsAndDictionaryPoliciesReachEveryFooter)
{
    WriteSettings settings;
    settings.columns = {{"key", std::nullopt, std::nullopt, ParquetEncoding::DeltaByteArray},
        {"value", std::nullopt, DictionaryPolicy::Disabled, ParquetEncoding::DeltaBinaryPacked},
        {"score", std::nullopt, std::nullopt, ParquetEncoding::ByteStreamSplit},
        {"flag", std::nullopt, std::nullopt, ParquetEncoding::Rle},
        {"at", ParquetCompression{ParquetCodec::Snappy, std::nullopt}, DictionaryPolicy::Disabled, std::nullopt}};
    const auto created = Create(settings);
    ASSERT_EQ(created.disposition, CommitDisposition::Committed)
        << (created.error ? test::Present(created.error).message : std::string());
    EXPECT_EQ(test::Present(created.info).write_settings, settings);
    Append(FirstOperation, FirstRows);
    Append(SecondOperation, SecondRows);
    const std::map<std::string, TChunkEncodings> expected{{"key", {false, {"DELTA_BYTE_ARRAY"}}},
        {"value", {false, {"DELTA_BINARY_PACKED"}}},
        {"score", {false, {"BYTE_STREAM_SPLIT"}}},
        {"flag", {false, {"RLE"}}},
        {"at", {false, {"PLAIN"}}}};
    EXPECT_EQ(FooterEncodings(Root_, FirstOperation), expected);
    EXPECT_EQ(FooterEncodings(Root_, SecondOperation), expected);
    EXPECT_EQ(FooterCodecs(Root_, FirstOperation).at("at"), "snappy");
    // NULLs, duplicates and every type round-trip exactly.
    EXPECT_EQ(ScanRows(), Expected({&FirstRows, &SecondRows}));

    // The engine's file details report the same footer encodings.
    const auto files = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(files);
    for (const auto& file : files->Files) {
        const auto details = Engine_->GetFileDetails(test::collection_id, file.FileID);
        ASSERT_TRUE(details) << details.error().message;
        const auto& key = details->RowGroups.at(0).Columns.at(0);
        EXPECT_FALSE(key.DictionaryPageOffset);
        EXPECT_NE(std::ranges::find(key.Encodings, "delta_byte_array"), key.Encodings.end());
        ASSERT_FALSE(key.EncodingStats.empty());
        EXPECT_EQ(key.EncodingStats.back(), (TPageEncodingCount{"data_page", "delta_byte_array", 1}));
    }
}

TEST_F(TWriteSettingsTest, DictionaryOverflowFallsBackToPlainPages)
{
    WriteSettings settings;
    settings.dictionary_page_size_limit = 4096;
    ASSERT_EQ(Create(settings).disposition, CommitDisposition::Committed);
    // 3,000 distinct keys outgrow a 4 KiB dictionary; 3 values never do.
    std::vector<TInputRow> rows;
    rows.reserve(3000);
    for (int index = 0; index < 3000; ++index) {
        rows.push_back({std::format("key-{:08}", index), index % 3, 0.5, index % 2 == 0, 10});
    }
    Append(FirstOperation, rows);
    const auto footer = FooterEncodings(Root_, FirstOperation);
    EXPECT_EQ(footer.at("key"), (TChunkEncodings{true, {"RLE_DICTIONARY", "PLAIN"}}));
    EXPECT_EQ(footer.at("value"), (TChunkEncodings{true, {"RLE_DICTIONARY"}}));
    EXPECT_EQ(footer.at("flag"), (TChunkEncodings{false, {"PLAIN"}}));
    EXPECT_EQ(ScanRows(), Expected({&rows}));

    const auto files = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(files);
    const auto details = Engine_->GetFileDetails(test::collection_id, files->Files.at(0).FileID);
    ASSERT_TRUE(details);
    const auto& stats = details->RowGroups.at(0).Columns.at(0).EncodingStats;
    const auto pages = [&](std::string_view type, std::string_view encoding) {
        const auto found = std::ranges::find_if(stats, [&](const auto& item) {
            return item.PageType == type && item.Encoding == encoding;
        });
        return found == stats.end() ? 0 : found->Count;
    };
    EXPECT_EQ(pages("dictionary_page", "plain"), 1);
    EXPECT_GE(pages("data_page", "rle_dictionary"), 1);
    EXPECT_GE(pages("data_page", "plain"), 1);
}

TEST_F(TWriteSettingsTest, PolicyChangesMixEncodingsAcrossFilesAndRestarts)
{
    ASSERT_EQ(Create({}).disposition, CommitDisposition::Committed);
    const auto first = Append(FirstOperation, FirstRows);
    WriteSettings disabled;
    disabled.dictionary = DictionaryPolicy::Disabled;
    disabled.columns = {{"value", std::nullopt, DictionaryPolicy::Enabled, std::nullopt}};
    ASSERT_TRUE(Engine_->UpdateWriteSettings({test::collection_id, 0, disabled}));
    Append(SecondOperation, SecondRows);
    EXPECT_TRUE(FooterEncodings(Root_, FirstOperation).at("key").Dictionary);
    const auto second = FooterEncodings(Root_, SecondOperation);
    EXPECT_EQ(second.at("key"), (TChunkEncodings{false, {"PLAIN"}}));
    EXPECT_EQ(second.at("value"), (TChunkEncodings{true, {"RLE_DICTIONARY"}}));
    Reopen();
    EXPECT_EQ(Engine_->GetCollection(test::collection_id)->write_settings, disabled);
    EXPECT_EQ(ScanRows(), Expected({&FirstRows, &SecondRows}));
    EXPECT_EQ(ScanRows(SnapshotSelector::Id(first.snapshot_id)), Expected({&FirstRows}));
}

TEST_F(TWriteSettingsTest, IncompatibleEncodingsAndConflictsAreRejected)
{
    const auto column =
        [](std::string field, std::optional<DictionaryPolicy> dictionary, std::optional<ParquetEncoding> encoding) {
            WriteSettings settings;
            settings.columns = {{std::move(field), std::nullopt, dictionary, encoding}};
            return settings;
        };
    WriteSettings enabled;
    enabled.dictionary = DictionaryPolicy::Enabled;
    WriteSettings small;
    small.dictionary_page_size_limit = 4095;
    WriteSettings large;
    large.dictionary_page_size_limit = (8 << 20) + 1;
    WriteSettings unknown;
    // Deliberately invalid enum input exercises public validation.
    // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
    unknown.dictionary = static_cast<DictionaryPolicy>(7);
    for (const auto& settings :
        std::vector<WriteSettings>{column("key", std::nullopt, ParquetEncoding::DeltaBinaryPacked),
            column("key", std::nullopt, ParquetEncoding::ByteStreamSplit),
            column("value", std::nullopt, ParquetEncoding::DeltaByteArray),
            column("score", std::nullopt, ParquetEncoding::DeltaBinaryPacked),
            column("flag", std::nullopt, ParquetEncoding::ByteStreamSplit),
            column("at", std::nullopt, ParquetEncoding::Rle),
            column("flag", DictionaryPolicy::Enabled, std::nullopt),
            column("key", DictionaryPolicy::Enabled, ParquetEncoding::Plain),
            column("key", DictionaryPolicy::Auto, ParquetEncoding::DeltaByteArray),
            // Deliberately invalid enum input exercises public validation.
            // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
            column("key", std::nullopt, static_cast<ParquetEncoding>(9)),
            enabled,
            small,
            large,
            unknown})
    {
        const auto created = Create(settings);
        EXPECT_EQ(created.disposition, CommitDisposition::DefinitelyNotCommitted);
        ASSERT_TRUE(created.error);
        EXPECT_EQ(test::Present(created.error).code, "INVALID_ARGUMENT") << test::Present(created.error).message;
    }
    // Bool columns accept Auto, plain or rle with any collection policy once
    // they state their own; an explicit encoding implies no dictionary.
    enabled.columns = {{"flag", std::nullopt, DictionaryPolicy::Auto, ParquetEncoding::Rle}};
    ASSERT_EQ(Create(enabled).disposition, CommitDisposition::Committed);
    Append(FirstOperation, FirstRows);
    EXPECT_EQ(FooterEncodings(Root_, FirstOperation).at("flag"), (TChunkEncodings{false, {"RLE"}}));
    EXPECT_TRUE(FooterEncodings(Root_, FirstOperation).at("score").Dictionary);
}

TEST(WriteSettingsCodec, BlobRoundTripsAndRejectsUnknownOptions)
{
    const auto settings = Settings(ParquetCompression{ParquetCodec::Zstd, 7},
        {{"value", ParquetCompression{ParquetCodec::Gzip, std::nullopt}}});
    const auto encoded = detail::EncodeWriteSettings(Schema, settings);
    ASSERT_TRUE(encoded) << encoded.error().message;
    const auto decoded = detail::DecodeWriteSettings(Schema, *encoded);
    ASSERT_TRUE(decoded) << decoded.error().message;
    EXPECT_EQ(*decoded, settings);
    const auto empty = detail::EncodeWriteSettings(Schema, {});
    ASSERT_TRUE(empty);
    EXPECT_EQ(detail::DecodeWriteSettings(Schema, *empty).value_or(settings), WriteSettings{});

    // Non-canonical input is never encoded.
    EXPECT_FALSE(detail::EncodeWriteSettings(Schema,
        Settings(std::nullopt, {{"score", ParquetCompression{}}, {"value", ParquetCompression{}}})));
    auto unknownTag = *encoded;
    unknownTag[18 + 1] = 9; // first option tag of the collection default
    const auto rejected = detail::DecodeWriteSettings(Schema, unknownTag);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, "UNSUPPORTED_STORE_VERSION");
    auto newer = *encoded;
    newer[16] = 2;
    EXPECT_EQ(detail::DecodeWriteSettings(Schema, newer).error().code, "UNSUPPORTED_STORE_VERSION");
    EXPECT_EQ(detail::DecodeWriteSettings(Schema, encoded->substr(0, encoded->size() - 1)).error().code, "CORRUPTION");
    // Dictionary and encoding options round-trip; an encoding is column-only.
    WriteSettings full;
    full.compression = ParquetCompression{ParquetCodec::Lz4Raw, 3};
    full.dictionary = DictionaryPolicy::Disabled;
    full.dictionary_page_size_limit = 65536;
    full.columns = {{"key", std::nullopt, DictionaryPolicy::Enabled, std::nullopt},
        {"value", std::nullopt, std::nullopt, ParquetEncoding::DeltaBinaryPacked}};
    const auto fullBytes = detail::EncodeWriteSettings(Schema, full);
    ASSERT_TRUE(fullBytes) << fullBytes.error().message;
    EXPECT_EQ(detail::DecodeWriteSettings(Schema, *fullBytes).value_or(WriteSettings{}), full);
    // Field IDs bind the blob to its schema.
    EXPECT_FALSE(detail::DecodeWriteSettings(SchemaSpec{{{"key", FieldType::String, false}}}, *encoded));
}

TEST(WriteSettingsCodec, NativeScalarPoliciesRoundTripAndMatchAdvertisedEncodings)
{
    const SchemaSpec schema{{{"amount", FieldType::Decimal, true, 38, 2}, {"day", FieldType::Date, true}}};
    for (const auto encoding :
        {ParquetEncoding::Plain, ParquetEncoding::DeltaBinaryPacked, ParquetEncoding::ByteStreamSplit})
    {
        WriteSettings settings;
        settings.columns = {{"amount", std::nullopt, DictionaryPolicy::Disabled, ParquetEncoding::Plain},
            {"day", std::nullopt, DictionaryPolicy::Disabled, encoding}};
        const auto canonical = detail::CanonicalWriteSettings(schema, settings);
        ASSERT_TRUE(canonical) << canonical.error().message;
        const auto encoded = detail::EncodeWriteSettings(schema, *canonical);
        ASSERT_TRUE(encoded) << encoded.error().message;
        const auto decoded = detail::DecodeWriteSettings(schema, *encoded);
        ASSERT_TRUE(decoded) << decoded.error().message;
        EXPECT_EQ(*decoded, *canonical);
    }
    for (const auto encoding : {ParquetEncoding::DeltaBinaryPacked,
             ParquetEncoding::DeltaLengthByteArray,
             ParquetEncoding::DeltaByteArray,
             ParquetEncoding::ByteStreamSplit,
             ParquetEncoding::Rle})
    {
        WriteSettings invalid;
        invalid.columns = {{"amount", std::nullopt, DictionaryPolicy::Disabled, encoding}};
        EXPECT_FALSE(detail::CanonicalWriteSettings(schema, invalid));
    }
    WriteSettings dictionary;
    dictionary.columns = {{"amount", std::nullopt, DictionaryPolicy::Enabled, std::nullopt},
        {"day", std::nullopt, DictionaryPolicy::Enabled, std::nullopt}};
    EXPECT_TRUE(detail::CanonicalWriteSettings(schema, dictionary));
    EngineCapabilities capabilities;
    detail::AddEncodingCapabilities(capabilities);
    EXPECT_TRUE(std::ranges::contains(capabilities.dictionary_types, FieldType::Decimal));
    EXPECT_TRUE(std::ranges::contains(capabilities.dictionary_types, FieldType::Date));
    for (const auto& encoding : capabilities.parquet_encodings) {
        EXPECT_EQ(std::ranges::contains(encoding.types, FieldType::Decimal),
            encoding.encoding == ParquetEncoding::Plain);
        EXPECT_EQ(std::ranges::contains(encoding.types, FieldType::Date),
            encoding.encoding == ParquetEncoding::Plain || encoding.encoding == ParquetEncoding::DeltaBinaryPacked ||
                encoding.encoding == ParquetEncoding::ByteStreamSplit);
    }
}

} // namespace
} // namespace glamdb
