// File and row-group pruning never changes scan results. Several appends with
// disjoint and overlapping ranges, all-NULL and constant columns, timestamp
// limits, truncated (long and multibyte) string bounds and small row groups
// are scanned with many generated predicates, with pruning on and off, at the
// latest and a historical snapshot. Both must return exactly the same rows in
// the same order, equal to an independent three-valued evaluation of the
// input rows. Separate cases check what statistics let a scan skip.

#include "core/scan/scan_cursor.h"
#include "core/scan/scan_pruning.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <cstdio>
#include <limits>
#include <random>
#include <variant>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;

constexpr int64_t MinTimestamp = -62135596800000000LL;
constexpr int64_t MaxTimestamp = 253402300799999999LL;
constexpr int Files = 6;
constexpr int RowsPerFile = 40;
// Large values bound row groups to a few rows each; see the fixture row builder.
constexpr size_t BlobBytes = 700 * 1024;
constexpr int BlobEveryRows = 8;
constexpr int64_t RowGroupBytes = 1LL << 20;

const SchemaSpec WideSchema{{
    {"key", FieldType::String, false},
    {"value", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"flag", FieldType::Bool, true},
    {"at", FieldType::TimestampUsUtc, true},
    {"name", FieldType::String, true},
    {"blob", FieldType::String, true},
}};

// NULL, string, bool, int64 (also timestamps) or double.
using TCell = std::variant<std::monostate, std::string, bool, int64_t, double>;
using TRow = std::vector<TCell>;

// Every non-key name of file 0 shares its first 16 code points, so its manifest
// upper bound is truncated; file 1 and 3 use multibyte characters.
const std::string LongPrefix = "abcdefghijklmnop";
const std::string Cyrillic = "\xD0\xB0\xD0\xB1\xD0\xB2\xD0\xB3\xD0\xB4\xD0\xB5\xD0\xB6\xD0\xB7\xD0\xB8\xD0\xB9"
                             "\xD0\xBA\xD0\xBB\xD0\xBC\xD0\xBD\xD0\xBE\xD0\xBF";
const std::string Emoji = "\xF0\x9F\x98\x80";

std::string Suffix(int row)
{
    return std::string(1, static_cast<char>('a' + row % 26)) + std::to_string(row);
}

TRow MakeRow(int file, int row)
{
    TRow cells(WideSchema.fields.size());
    char key[16];
    std::snprintf(key, sizeof(key), "k%d-%02d", file, row);
    cells[0] = std::string(key);
    if (file == 4 || (file != 5 && row % 9 == 0)) {
        cells[1] = std::monostate{};
    } else {
        cells[1] = file == 5 ? int64_t{7} : int64_t{file} * 100 + row * 2;
    }
    if (!(file == 2 && row % 3 == 0)) {
        cells[2] = file == 1 ? -0.5 * (row + 1) : 10.0 * file + 0.25 * row;
    }
    if (file == 0) {
        cells[3] = true;
    } else if (file == 1) {
        cells[3] = false;
    } else if (row % 5 != 0) {
        cells[3] = row % 2 == 0;
    }
    if (file == 3 && row == 1) {
        cells[4] = MinTimestamp;
    } else if (file == 3 && row == 2) {
        cells[4] = MaxTimestamp;
    } else if (row % 11 != 0) {
        cells[4] = int64_t{file} * 1'000'000 + row * 1000;
    }
    switch (file) {
        case 0:
            cells[5] = LongPrefix + Suffix(row);
            break;
        case 1:
            cells[5] = Cyrillic + Suffix(row);
            break;
        case 2:
            cells[5] = "n" + std::to_string(row);
            break;
        case 3:
            cells[5] = row % 4 == 0 ? std::string() : std::string(Emoji) + LongPrefix + Suffix(row);
            break;
        case 4:
            break;
        default:
            cells[5] = row % 2 == 0 ? std::string() : std::string("a");
    }
    // One large value per BlobEveryRows rows: with the fixture's 1 MiB row
    // group bound, a row group can hold only one of them, so every file has
    // several small row groups.
    if (row % BlobEveryRows == 0) {
        cells[6] = std::string(BlobBytes, static_cast<char>('a' + file));
    }
    return cells;
}

std::shared_ptr<arrow::RecordBatch> MakeBatch(const std::vector<TRow>& rows)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (size_t column = 0; column < WideSchema.fields.size(); ++column) {
        const auto& spec = WideSchema.fields[column];
        std::unique_ptr<arrow::ArrayBuilder> builder;
        std::shared_ptr<arrow::DataType> type;
        switch (spec.type) {
            case FieldType::String:
                type = arrow::utf8();
                break;
            case FieldType::Int64:
                type = arrow::int64();
                break;
            case FieldType::Float64:
                type = arrow::float64();
                break;
            case FieldType::Bool:
                type = arrow::boolean();
                break;
            case FieldType::TimestampUsUtc:
                type = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
                break;
        }
        EXPECT_TRUE(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder).ok());
        for (const auto& row : rows) {
            const auto& cell = row[column];
            arrow::Status status;
            if (std::holds_alternative<std::monostate>(cell)) {
                status = builder->AppendNull();
            } else if (const auto* text = std::get_if<std::string>(&cell)) {
                status = static_cast<arrow::StringBuilder&>(*builder).Append(*text);
            } else if (const auto* flag = std::get_if<bool>(&cell)) {
                status = static_cast<arrow::BooleanBuilder&>(*builder).Append(*flag);
            } else if (const auto* number = std::get_if<double>(&cell)) {
                status = static_cast<arrow::DoubleBuilder&>(*builder).Append(*number);
            } else if (spec.type == FieldType::TimestampUsUtc) {
                status = static_cast<arrow::TimestampBuilder&>(*builder).Append(std::get<int64_t>(cell));
            } else {
                status = static_cast<arrow::Int64Builder&>(*builder).Append(std::get<int64_t>(cell));
            }
            EXPECT_TRUE(status.ok()) << status.ToString();
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder->Finish(&array).ok());
        columns.push_back(std::move(array));
        fields.push_back(arrow::field(spec.name, type, spec.nullable));
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), static_cast<int64_t>(rows.size()), std::move(columns));
}

size_t FieldIndex(const std::string& name)
{
    for (size_t index = 0; index < WideSchema.fields.size(); ++index) {
        if (WideSchema.fields[index].name == name) {
            return index;
        }
    }
    ADD_FAILURE() << "unknown field " << name;
    return 0;
}

TCell FromLiteral(const Literal& literal)
{
    return std::visit(
        [](const auto& value) -> TCell {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, TimestampUs>) {
                return value.value;
            } else {
                return value;
            }
        },
        literal);
}

// The independent oracle: SQL three-valued logic over the input rows.
enum class ETruth { False, Unknown, True };

ETruth Evaluate(const Predicate& predicate, const TRow& row)
{
    const auto fromBool = [](bool value) {
        return value ? ETruth::True : ETruth::False;
    };
    switch (predicate.kind()) {
        case Predicate::Kind::True:
            return ETruth::True;
        case Predicate::Kind::Compare: {
            const auto& cell = row[FieldIndex(predicate.field())];
            if (!predicate.optional_value() || std::holds_alternative<std::monostate>(cell)) {
                return ETruth::Unknown;
            }
            const auto literal = FromLiteral(*predicate.optional_value());
            const auto order = cell <=> literal;
            switch (predicate.op()) {
                case CompareOp::Eq:
                    return fromBool(order == 0);
                case CompareOp::Ne:
                    return fromBool(order != 0);
                case CompareOp::Lt:
                    return fromBool(order < 0);
                case CompareOp::Le:
                    return fromBool(order <= 0);
                case CompareOp::Gt:
                    return fromBool(order > 0);
                case CompareOp::Ge:
                    return fromBool(order >= 0);
            }
            return ETruth::Unknown;
        }
        case Predicate::Kind::In: {
            const auto& cell = row[FieldIndex(predicate.field())];
            if (std::holds_alternative<std::monostate>(cell)) {
                return ETruth::Unknown;
            }
            bool sawNull = false;
            for (const auto& item : predicate.values()) {
                if (!item) {
                    sawNull = true;
                } else if (cell == FromLiteral(*item)) {
                    return ETruth::True;
                }
            }
            return sawNull ? ETruth::Unknown : ETruth::False;
        }
        case Predicate::Kind::IsNull:
            return fromBool(std::holds_alternative<std::monostate>(row[FieldIndex(predicate.field())]));
        case Predicate::Kind::IsNotNull:
            return fromBool(!std::holds_alternative<std::monostate>(row[FieldIndex(predicate.field())]));
        case Predicate::Kind::And: {
            auto result = ETruth::True;
            for (const auto& child : predicate.children()) {
                result = std::min(result, Evaluate(child, row));
            }
            return result;
        }
        case Predicate::Kind::Or: {
            auto result = ETruth::False;
            for (const auto& child : predicate.children()) {
                result = std::max(result, Evaluate(child, row));
            }
            return result;
        }
        case Predicate::Kind::Not: {
            const auto value = Evaluate(predicate.children().front(), row);
            return value == ETruth::Unknown ? value : value == ETruth::True ? ETruth::False : ETruth::True;
        }
    }
    return ETruth::Unknown;
}

std::string RenderRow(const TRow& row, const std::vector<size_t>& fields)
{
    std::string text;
    for (size_t index = 0; index < fields.size(); ++index) {
        const auto field = fields[index];
        const auto& cell = row[field];
        std::string value;
        if (std::holds_alternative<std::monostate>(cell)) {
            value = "null";
        } else if (const auto* string = std::get_if<std::string>(&cell)) {
            value = *string;
        } else if (const auto* flag = std::get_if<bool>(&cell)) {
            value = *flag ? "true" : "false";
        } else if (const auto* number = std::get_if<double>(&cell)) {
            value = std::to_string(*number);
        } else if (WideSchema.fields[field].type == FieldType::TimestampUsUtc) {
            value = "ts:" + std::to_string(std::get<int64_t>(cell));
        } else {
            value = std::to_string(std::get<int64_t>(cell));
        }
        text += (index == 0 ? "" : "|") + value;
    }
    return text;
}

// Literal candidates per field: data values, values in gaps and around every
// bound, and type limits.
std::vector<Literal> Candidates(const std::string& field)
{
    if (field == "key") {
        return {std::string(),
            std::string("k0-00"),
            std::string("k2-19"),
            std::string("k2-2"),
            std::string("k5-39"),
            std::string("k9"),
            std::string("j")};
    }
    if (field == "value") {
        return {int64_t{-1},
            int64_t{0},
            int64_t{7},
            int64_t{21},
            int64_t{22},
            int64_t{78},
            int64_t{100},
            int64_t{179},
            int64_t{301},
            int64_t{378},
            int64_t{5000},
            std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::max()};
    }
    if (field == "score") {
        return {-20.0, -0.5, 0.0, 0.25, 9.75, 10.0, 25.0, 59.75, 1e300, -1e300};
    }
    if (field == "flag") {
        return {true, false};
    }
    if (field == "at") {
        return {TimestampUs{MinTimestamp},
            TimestampUs{MaxTimestamp},
            TimestampUs{0},
            TimestampUs{1000},
            TimestampUs{2'039'000},
            TimestampUs{2'500'000},
            TimestampUs{5'000'000}};
    }
    return {std::string(),
        std::string("a"),
        std::string("n1"),
        std::string("n9"),
        LongPrefix,
        LongPrefix + Suffix(3),
        LongPrefix + "zzz",
        LongPrefix.substr(0, 15) + "q",
        Cyrillic + Suffix(5),
        Cyrillic + "\xD1\x8F",
        std::string(Emoji) + LongPrefix + Suffix(7),
        std::string(Emoji) + "z",
        std::string("\xF4\x8F\xBF\xBF")};
}

const std::vector<std::string> Filterable{"key", "value", "score", "flag", "at", "name"};

Predicate Leaf(std::mt19937& random)
{
    const auto& field = Filterable[random() % Filterable.size()];
    const auto candidates = Candidates(field);
    const auto literal = [&] {
        return candidates[random() % candidates.size()];
    };
    switch (random() % 10) {
        case 0:
            return Predicate::IsNull(field);
        case 1:
            return Predicate::IsNotNull(field);
        case 2:
            return Predicate::CompareNull(field, static_cast<CompareOp>(random() % 6));
        case 3:
        case 4: {
            std::vector<OptionalLiteral> values;
            const auto count = random() % 4;
            for (size_t index = 0; index < count; ++index) {
                values.push_back(random() % 5 == 0 ? OptionalLiteral{} : OptionalLiteral{literal()});
            }
            return Predicate::In(field, std::move(values));
        }
        default:
            return Predicate::Compare(field, static_cast<CompareOp>(random() % 6), literal());
    }
}

Predicate Tree(std::mt19937& random, int depth)
{
    if (depth == 0 || random() % 3 == 0) {
        return Leaf(random);
    }
    switch (random() % 5) {
        case 0:
            return Predicate::Not(Tree(random, depth - 1));
        case 1:
        case 2: {
            std::vector<Predicate> children;
            for (size_t index = 0; index < 2 + random() % 2; ++index) {
                children.push_back(Tree(random, depth - 1));
            }
            return Predicate::Or(std::move(children));
        }
        default: {
            std::vector<Predicate> children;
            for (size_t index = 0; index < 2 + random() % 2; ++index) {
                children.push_back(Tree(random, depth - 1));
            }
            return Predicate::And(std::move(children));
        }
    }
}

class ScanPruningTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::shared_ptr<Engine> Engine_;
    // Input rows per append, in order.
    std::vector<std::vector<TRow>> Appended_;
    std::vector<int64_t> Snapshots_;

    void SetUp() override
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, (Temporary_.path / "store").string()},
            [](const std::filesystem::path& path,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
                detail::TLocalStorageOptions options;
                options.Writer.MaxRowGroupBytes = RowGroupBytes;
                auto backend = detail::TLocalStorageBackend::Open(path, info, options);
                if (!backend) {
                    return std::unexpected(backend.error());
                }
                return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
            });
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "wide", WideSchema}).disposition,
            CommitDisposition::Committed);
        for (int file = 0; file < Files; ++file) {
            std::vector<TRow> rows;
            for (int row = 0; row < RowsPerFile; ++row) {
                rows.push_back(MakeRow(file, row));
            }
            char operation[37];
            std::snprintf(operation, sizeof(operation), "00000000-0000-4000-8000-%012x", 0x500 + file);
            const auto outcome = Engine_->AppendDurable({test::collection_id, operation, {}, {MakeBatch(rows)}});
            ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
                << (outcome.error ? outcome.error->code + ": " + outcome.error->message : std::string());
            ASSERT_TRUE(outcome.result);
            Snapshots_.push_back(outcome.result->snapshot_id);
            Appended_.push_back(std::move(rows));
        }
    }

    void TearDown() override
    {
        detail::SetScanPruning(true);
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    struct TScan
    {
        std::vector<std::string> Rows;
        ScanProfile Profile;
        detail::TScanReadStats Stats;
    };

    TScan Run(const ScanRequest& request, bool pruning)
    {
        detail::SetScanPruning(pruning);
        TScan result;
        const auto cursor = Engine_->Scan(request);
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().message);
        if (!cursor) {
            return result;
        }
        while (true) {
            const auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().code + ": " + batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            const auto rendered = Render(**batch);
            result.Rows.insert(result.Rows.end(), rendered.begin(), rendered.end());
        }
        result.Profile = (*cursor)->Profile();
        result.Stats = detail::GetScanReadStats(**cursor).value();
        detail::SetScanPruning(true);
        return result;
    }

    // Rows of the first `files` appends, in scan order, that the oracle keeps.
    std::vector<std::string> Expected(const Predicate& predicate,
        size_t files,
        const std::vector<size_t>& projection,
        std::optional<int64_t> limit = std::nullopt)
    {
        std::vector<std::string> rows;
        // The scanner reads the newest manifest first (manifest list order).
        for (size_t file = files; file-- > 0;) {
            for (const auto& row : Appended_[file]) {
                if (Evaluate(predicate, row) == ETruth::True && (!limit || static_cast<int64_t>(rows.size()) < *limit))
                {
                    rows.push_back(RenderRow(row, projection));
                }
            }
        }
        return rows;
    }
};

TEST_F(ScanPruningTest, PruningNeverChangesResults)
{
    const std::vector<std::string> projection{"key", "value", "score", "flag", "at", "name"};
    const std::vector<size_t> fields{0, 1, 2, 3, 4, 5};
    // The order of a whole-view scan pins the manifest order the oracle assumes.
    const auto all = Run({.collection_id = test::collection_id, .projection = projection}, false);
    ASSERT_EQ(all.Rows, Expected(Predicate::True(), Files, fields));

    std::mt19937 random(20261112);
    std::vector<Predicate> predicates;
    for (const auto& field : Filterable) {
        for (const auto& literal : Candidates(field)) {
            for (int op = 0; op < 6; ++op) {
                predicates.push_back(Predicate::Compare(field, static_cast<CompareOp>(op), literal));
            }
            predicates.push_back(Predicate::In(field, {literal}));
            predicates.push_back(Predicate::In(field, {literal, std::nullopt}));
        }
        predicates.push_back(Predicate::In(field, {}));
        predicates.push_back(Predicate::IsNull(field));
        predicates.push_back(Predicate::IsNotNull(field));
        predicates.push_back(Predicate::CompareNull(field, CompareOp::Ne));
        predicates.push_back(Predicate::Not(Predicate::IsNull(field)));
    }
    for (int index = 0; index < 200; ++index) {
        predicates.push_back(Tree(random, 3));
    }

    detail::TScanReadStats pruned;
    int64_t bytesOn = 0;
    int64_t bytesOff = 0;
    int64_t emptyResults = 0;
    for (size_t index = 0; index < predicates.size(); ++index) {
        SCOPED_TRACE(index);
        const auto& predicate = predicates[index];
        for (const size_t files : {size_t{Files}, size_t{3}}) {
            const ScanRequest request{.collection_id = test::collection_id,
                .selector = files == Files ? SnapshotSelector::Latest() : SnapshotSelector::Id(Snapshots_[files - 1]),
                .projection = projection,
                .predicate = predicate,
                .limit = index % 7 == 0 ? std::optional<int64_t>(5) : std::nullopt,
                .batch_rows = index % 2 == 0 ? 65536 : 3};
            const auto on = Run(request, true);
            const auto off = Run(request, false);
            ASSERT_EQ(on.Rows, off.Rows);
            ASSERT_EQ(on.Rows, Expected(predicate, files, fields, request.limit));
            EXPECT_EQ(off.Stats.FilesPruned + off.Stats.RowGroupsPruned, 0);
            EXPECT_LE(*on.Profile.data_bytes_read.value, *off.Profile.data_bytes_read.value);
            pruned.FilesPruned += on.Stats.FilesPruned;
            pruned.RowGroupsPruned += on.Stats.RowGroupsPruned;
            bytesOn += *on.Profile.data_bytes_read.value;
            bytesOff += *off.Profile.data_bytes_read.value;
            emptyResults += on.Rows.empty() ? 1 : 0;
        }
    }
    std::printf("%zu predicates x 2 views: files pruned %lld, row groups pruned %lld, bytes %lld (on) vs %lld (off), "
                "%lld empty results\n",
        predicates.size(),
        static_cast<long long>(pruned.FilesPruned),
        static_cast<long long>(pruned.RowGroupsPruned),
        static_cast<long long>(bytesOn),
        static_cast<long long>(bytesOff),
        static_cast<long long>(emptyResults));
    EXPECT_GT(pruned.FilesPruned, 0);
    EXPECT_GT(pruned.RowGroupsPruned, 0);
    EXPECT_LT(bytesOn, bytesOff);
}

TEST_F(ScanPruningTest, ManifestMetricsSkipFilesWithoutReadingThem)
{
    // value is within 0..578 or NULL everywhere.
    const ScanRequest request{.collection_id = test::collection_id,
        .predicate = Predicate::Compare("value", CompareOp::Gt, int64_t{5000})};
    const auto on = Run(request, true);
    EXPECT_TRUE(on.Rows.empty());
    // Indexed planning reads no manifest; the index carries the metrics.
    EXPECT_EQ(on.Profile.manifests_read.value, 0);
    EXPECT_EQ(on.Profile.data_files_read.value, 0);
    EXPECT_EQ(on.Profile.data_bytes_read.value, 0);
    EXPECT_EQ(on.Profile.rows_decoded.value, 0);
    EXPECT_EQ(on.Stats.FilesPruned, Files);
    EXPECT_EQ(on.Stats.ReadCalls, 0);
    EXPECT_EQ(on.Profile.data_files_pruned.value, Files);
    EXPECT_EQ(on.Profile.row_groups_pruned.value, 0);
    const auto off = Run(request, false);
    EXPECT_TRUE(off.Rows.empty());
    EXPECT_EQ(off.Profile.data_files_read.value, Files);
    EXPECT_EQ(off.Profile.rows_decoded.value, Files * RowsPerFile);
    EXPECT_EQ(off.Stats.FilesPruned, 0);

    // An all-NULL column (file 4) is skipped by IS NOT NULL and comparisons, the
    // constant column of file 5 by "<> 7"; IS NULL keeps only files with NULLs.
    const auto notNull = Run({.collection_id = test::collection_id,
                                 .predicate = Predicate::And({Predicate::IsNotNull("value"),
                                     Predicate::Compare("value", CompareOp::Ne, int64_t{7})})},
        true);
    EXPECT_EQ(notNull.Stats.FilesPruned, 2);
    const auto nulls = Run({.collection_id = test::collection_id, .predicate = Predicate::IsNull("value")}, true);
    EXPECT_EQ(nulls.Stats.FilesPruned, 1);
    // OR prunes only when every branch does; NOT is never used to prune.
    const auto either = Run({.collection_id = test::collection_id,
                                .predicate = Predicate::Or({Predicate::Compare("value", CompareOp::Gt, int64_t{5000}),
                                    Predicate::Compare("score", CompareOp::Lt, -100.0)})},
        true);
    EXPECT_EQ(either.Stats.FilesPruned, Files);
    const auto negated =
        Run({.collection_id = test::collection_id,
                .predicate = Predicate::Not(Predicate::Compare("value", CompareOp::Le, int64_t{5000}))},
            true);
    EXPECT_EQ(negated.Stats.FilesPruned, 0);
    EXPECT_EQ(negated.Stats.RowGroupsPruned, 0);
}

TEST_F(ScanPruningTest, RowGroupStatisticsLeaveOnlyTheFooterRead)
{
    // File 0 holds even values 2..78 in row groups of a few rows each. The odd
    // value after its first row group's maximum lies between two row groups, so
    // the manifest range [2, 78] cannot exclude it but every row group can.
    // Other files are skipped by their manifests.
    const auto listing = Engine_->ListFiles(test::collection_id);
    ASSERT_TRUE(listing);
    std::optional<int64_t> firstGroupMax;
    size_t groups = 0;
    const auto decode = [](const std::vector<uint8_t>& bytes) {
        uint64_t value = 0;
        for (size_t index = 0; index < 8; ++index) {
            value |= uint64_t{bytes.at(index)} << (8 * index);
        }
        return static_cast<int64_t>(value);
    };
    for (const auto& file : listing->Files) {
        const auto details = Engine_->GetFileDetails(test::collection_id, file.FileID);
        ASSERT_TRUE(details);
        const auto& first = details->RowGroups.front().Columns.at(1);
        ASSERT_EQ(first.FieldID, 2);
        if (first.LowerBound && decode(*first.LowerBound) == 2) {
            firstGroupMax = decode(first.UpperBound.value());
            groups = details->RowGroups.size();
        }
    }
    ASSERT_TRUE(firstGroupMax);
    ASSERT_GT(groups, 2U);
    const auto gap = *firstGroupMax + 1;
    const auto on =
        Run({.collection_id = test::collection_id, .predicate = Predicate::Compare("value", CompareOp::Eq, gap)}, true);
    EXPECT_TRUE(on.Rows.empty());
    EXPECT_EQ(on.Stats.FilesPruned, Files - 1);
    EXPECT_EQ(on.Profile.data_files_read.value, 1);
    EXPECT_EQ(on.Stats.RowGroupsPruned, static_cast<int64_t>(groups));
    EXPECT_EQ(on.Profile.row_groups_pruned.value, static_cast<int64_t>(groups));
    EXPECT_EQ(on.Stats.RowGroupsRead, 0);
    // Magic, trailer and footer only.
    EXPECT_EQ(on.Stats.ReadCalls, 3);
    EXPECT_EQ(on.Profile.rows_decoded.value, 0);

    // A matching value decodes only its own row group.
    const auto one = Run({.collection_id = test::collection_id,
                             .predicate = Predicate::Compare("value", CompareOp::Eq, *firstGroupMax + 2)},
        true);
    ASSERT_EQ(one.Rows.size(), 1U);
    EXPECT_EQ(one.Stats.RowGroupsRead, 1);
    EXPECT_LT(*one.Profile.rows_decoded.value, RowsPerFile);
}

TEST_F(ScanPruningTest, TruncatedStringUpperBoundsAreNotUsed)
{
    // Every name of file 0 starts with the 16 code points of LongPrefix, so its
    // manifest upper bound is truncated: the manifest must not exclude a larger
    // name. Its exact Parquet row-group statistics may.
    // Only file 0 has names in [LongPrefix, above); larger names (n.., Cyrillic,
    // emoji) are in files 1..3.
    const auto above = LongPrefix + "zzz";
    const auto predicate = Predicate::And({Predicate::Compare("name", CompareOp::Ge, above),
        Predicate::Compare("name", CompareOp::Lt, std::string("b"))});
    const auto on = Run({.collection_id = test::collection_id,
                            .projection = std::vector<std::string>{"key", "name"},
                            .predicate = predicate},
        true);
    EXPECT_TRUE(on.Rows.empty());
    EXPECT_EQ(on.Rows, Expected(predicate, Files, {0, 5}));
    // Files 1, 2, 4 and 5 are excluded by exact or prefix lower bounds. Files 0
    // and 3 have truncated upper bounds, so they are opened; every row group of
    // file 0 is then excluded by its exact Parquet statistics.
    EXPECT_EQ(on.Stats.FilesPruned, Files - 2);
    EXPECT_EQ(on.Profile.data_files_read.value, 2);
    EXPECT_GT(on.Stats.RowGroupsPruned, 2);
    // Nothing sorts before the empty string; prefix lower bounds suffice.
    const auto below = Run(
        {.collection_id = test::collection_id, .predicate = Predicate::Compare("name", CompareOp::Lt, std::string())},
        true);
    EXPECT_TRUE(below.Rows.empty());
    EXPECT_EQ(below.Stats.FilesPruned, Files);
}

} // namespace
} // namespace glamdb
