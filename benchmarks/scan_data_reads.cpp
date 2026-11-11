// Data read cost of warm native scans over a 64-field collection: a full scan,
// a 2-of-64-field projection and a predicate that matches nothing (c1 is always
// within -1000..1000), the last with pruning and without it. Prints one JSON line per case with the mean wall time
// and the cursor profile (data files opened, data bytes read, rows decoded).
//   glamdb_scan_data_reads_bench [files=100] [rows=1000] [repeats=10]

#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "core/scan/scan_pruning.h"
#include "tests/support/temp_dir.h"

#include <arrow/api.h>

#include <charconv>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace {

using namespace glamdb;
using TClock = std::chrono::steady_clock;

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw std::runtime_error(result.error().code + ": " + result.error().message);
    }
    return std::move(*result);
}

void Require(bool condition, const char* message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

int Count(const char* argument, int maximum)
{
    const std::string_view text(argument);
    int value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    Require(error == std::errc{} && end == text.data() + text.size() && value > 0 && value <= maximum,
        "invalid benchmark count");
    return value;
}

constexpr int Fields = 64;

std::shared_ptr<arrow::RecordBatch> MakeBatch(const std::shared_ptr<arrow::Schema>& schema, int file, int rows)
{
    std::vector<std::shared_ptr<arrow::Array>> columns;
    arrow::StringBuilder keys;
    for (int row = 0; row < rows; ++row) {
        Require(keys.Append("k" + std::to_string(file) + "-" + std::to_string(row)).ok(), "build keys");
    }
    columns.push_back(keys.Finish().ValueOrDie());
    for (int field = 1; field < Fields; ++field) {
        if (field % 2 == 1) {
            arrow::Int64Builder builder;
            for (int row = 0; row < rows; ++row) {
                Require(builder.Append((int64_t{row} * 7 + file * 13 + field) % 2001 - 1000).ok(), "build int");
            }
            columns.push_back(builder.Finish().ValueOrDie());
        } else {
            arrow::DoubleBuilder builder;
            for (int row = 0; row < rows; ++row) {
                Require(builder.Append(0.25 * ((row * 3 + file + field) % 4096)).ok(), "build double");
            }
            columns.push_back(builder.Finish().ValueOrDie());
        }
    }
    return arrow::RecordBatch::Make(schema, rows, std::move(columns));
}

} // namespace

int main(int argc, char** argv)
try {
    Require(argc <= 4, "usage: glamdb_scan_data_reads_bench [files=100] [rows=1000] [repeats=10]");
    const int files = argc > 1 ? Count(argv[1], 4096) : 100;
    const int rowsPerFile = argc > 2 ? Count(argv[2], 10000) : 1000;
    const int repeats = argc > 3 ? Count(argv[3], 100000) : 10;
    SchemaSpec schema;
    std::vector<std::shared_ptr<arrow::Field>> arrowFields;
    schema.fields.push_back({"key", FieldType::String, false});
    arrowFields.push_back(arrow::field("key", arrow::utf8(), false));
    for (int field = 1; field < Fields; ++field) {
        const auto name = "c" + std::to_string(field);
        const bool integer = field % 2 == 1;
        schema.fields.push_back({name, integer ? FieldType::Int64 : FieldType::Float64, true});
        arrowFields.push_back(arrow::field(name, integer ? arrow::int64() : arrow::float64()));
    }
    const auto arrowSchema = arrow::schema(arrowFields);

    test::TempDir temporary;
    const auto root = temporary.path / "store";
    const detail::BackendFactory factory =
        [](const std::filesystem::path& path,
            const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
        auto opened = detail::TLocalStorageBackend::Open(path, info);
        if (!opened) {
            return std::unexpected(opened.error());
        }
        return std::shared_ptr<detail::StorageBackend>(std::move(*opened));
    };
    auto engine = Take(detail::EngineFactory::CreateWith({test::store_id, root.string()}, factory));
    Require(engine->CreateCollection({test::collection_id, "wide", schema}).disposition == CommitDisposition::Committed,
        "create collection failed");
    for (int file = 0; file < files; ++file) {
        char operation[37];
        std::snprintf(operation, sizeof(operation), "00000000-0000-4000-8000-%012x", file + 4096);
        const auto outcome =
            engine->AppendDurable({test::collection_id, operation, {}, {MakeBatch(arrowSchema, file, rowsPerFile)}});
        Require(outcome.disposition == CommitDisposition::Committed, "append failed");
    }
    int64_t parquetBytes = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
            parquetBytes += static_cast<int64_t>(entry.file_size());
        }
    }

    struct TCase
    {
        const char* Name;
        ScanRequest Request;
        int64_t Rows;
        bool Pruning = true;
    };
    const int64_t total = int64_t{files} * rowsPerFile;
    const std::vector<TCase> cases{
        {"full", {.collection_id = test::collection_id}, total},
        {"projection_2_of_64",
            {.collection_id = test::collection_id, .projection = std::vector<std::string>{"key", "c1"}},
            total},
        {"predicate_c1_gt_5000",
            {.collection_id = test::collection_id, .predicate = Predicate::Compare("c1", CompareOp::Gt, int64_t{5000})},
            0},
        {"predicate_c1_gt_5000_unpruned",
            {.collection_id = test::collection_id, .predicate = Predicate::Compare("c1", CompareOp::Gt, int64_t{5000})},
            0,
            false},
    };
    for (const auto& item : cases) {
        detail::SetScanPruning(item.Pruning);
        ScanProfile profile;
        const auto run = [&] {
            auto cursor = Take(engine->Scan(item.Request));
            int64_t rows = 0;
            while (const auto batch = Take(cursor->Next())) {
                rows += batch->num_rows();
            }
            Require(rows == item.Rows, "unexpected row count");
            profile = cursor->Profile();
        };
        run();
        const auto started = TClock::now();
        for (int repeat = 0; repeat < repeats; ++repeat) {
            run();
        }
        const auto meanMs = std::chrono::duration<double, std::milli>(TClock::now() - started).count() / repeats;
        std::cout << "{\"case\":\"" << item.Name << "\",\"files\":" << files << ",\"rows_per_file\":" << rowsPerFile
                  << ",\"parquet_bytes\":" << parquetBytes << ",\"mean_ms\":" << meanMs
                  << ",\"data_files_read\":" << profile.data_files_read.value.value_or(-1)
                  << ",\"data_bytes_read\":" << profile.data_bytes_read.value.value_or(-1)
                  << ",\"rows_decoded\":" << profile.rows_decoded.value.value_or(-1) << "}\n";
    }
    detail::SetScanPruning(true);
    Require(engine->Shutdown(std::chrono::seconds(10)).has_value(), "shutdown failed");
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
