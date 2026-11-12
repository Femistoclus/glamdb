// Manual micro-benchmark of the server JSON codecs (not registered in CTest):
// append-body parsing (DOM + ParseRows vs streaming rows) and scan row
// encoding (per-row DOM dump vs direct writer) for 2/16/64-field schemas.
// Usage: glamdb_json_codec_bench [min-seconds-per-case [fields rows parse|encode]]
// The second form runs only the new codec for one case (for profiling).
#include "apps/transport/codec.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>

namespace {
using namespace glamdb;
using namespace glamdb::NTransport;
using TClock = std::chrono::steady_clock;

SchemaSpec MakeSchema(size_t width)
{
    // Mixed types in the proportions of a typical wide record.
    const FieldType cycle[] = {FieldType::String,
        FieldType::Int64,
        FieldType::Float64,
        FieldType::Int64,
        FieldType::Bool,
        FieldType::TimestampUsUtc,
        FieldType::String,
        FieldType::Float64};
    SchemaSpec schema;
    for (size_t i = 0; i < width; ++i) {
        schema.fields.push_back({i == 0 ? "key" : "f" + std::to_string(i), cycle[i % std::size(cycle)], i != 0});
    }
    return schema;
}

std::string MakeBody(const SchemaSpec& schema, size_t rows)
{
    std::mt19937_64 random(rows * 131 + schema.fields.size());
    std::string body = R"({"operation_id":"00000000-0000-4000-8000-000000000000","rows":[)";
    for (size_t row = 0; row < rows; ++row) {
        body += row == 0 ? "{" : ",{";
        for (size_t i = 0; i < schema.fields.size(); ++i) {
            const auto& field = schema.fields[i];
            body += (i == 0 ? "\"" : ",\"") + field.name + "\":";
            switch (field.type) {
                case FieldType::String:
                    body += "\"s" + std::to_string(random() % 1000000) + "\"";
                    break;
                case FieldType::Bool:
                    body += random() % 2 ? "true" : "false";
                    break;
                case FieldType::Int64:
                    body += "\"" + std::to_string(static_cast<int64_t>(random() % 10000000000ULL)) + "\"";
                    break;
                case FieldType::Float64:
                    body += std::to_string(static_cast<double>(random() % 1000000) / 100.0);
                    break;
                case FieldType::TimestampUsUtc:
                    body += "\"2026-0" + std::to_string(1 + random() % 9) + "-1" + std::to_string(random() % 10) +
                        "T12:34:56." + std::to_string(100000 + random() % 900000) + "Z\"";
                    break;
            }
        }
        body += "}";
    }
    return body + "]}";
}

template <class F>
double SecondsPerRun(F run, double minimum)
{
    run();
    size_t runs = 0;
    const auto start = TClock::now();
    double elapsed = 0;
    do {
        run();
        ++runs;
        elapsed = std::chrono::duration<double>(TClock::now() - start).count();
    } while (elapsed < minimum);
    return elapsed / static_cast<double>(runs);
}

std::shared_ptr<arrow::RecordBatch> ParseOld(const SchemaSpec& schema, const std::string& body)
{
    const auto json = ParseJson(body);
    return ParseRows(schema, json.at("rows"));
}

std::shared_ptr<arrow::RecordBatch> ParseNew(const SchemaSpec& schema, const std::string& body)
{
    TJsonRows rows;
    const auto json = ParseJson(body, rows);
    (void)json.at("rows");
    return ParseRows(schema, rows);
}
} // namespace

int main(int argc, char** argv)
{
    const double minimum = argc > 1 ? std::atof(argv[1]) : 1.0;
    if (argc == 5) {
        const auto schema = MakeSchema(std::strtoull(argv[2], nullptr, 10));
        const auto rows = std::strtoull(argv[3], nullptr, 10);
        const auto body = MakeBody(schema, rows);
        const auto batch = ParseNew(schema, body);
        const TRowJsonBudget budget{.MaxRows = rows, .TargetBytes = SIZE_MAX, .HardBytes = SIZE_MAX};
        const auto seconds = SecondsPerRun(
            [&] {
                if (std::string_view(argv[4]) == "parse") {
                    (void)ParseNew(schema, body);
                } else {
                    std::string output = "[";
                    AppendRowsJson(*batch, output, budget);
                }
            },
            minimum);
        std::printf("%s %.3f ms\n", argv[4], seconds * 1e3);
        return 0;
    }
    std::printf("| codec | fields | rows | bytes | old ms | new ms | old MB/s | new MB/s | old rows/s | new rows/s | "
                "speedup |\n|---|---|---|---|---|---|---|---|---|---|---|\n");
    for (const size_t width : {2, 16, 64}) {
        for (const size_t rows : {1000, 10000}) {
            const auto schema = MakeSchema(width);
            const auto body = MakeBody(schema, rows);
            const auto expected = ParseOld(schema, body);
            if (!ParseNew(schema, body)->Equals(*expected)) {
                std::fprintf(stderr, "parse mismatch\n");
                return 1;
            }
            const auto report = [&](const char* codec, size_t bytes, double old, double now) {
                std::printf("| %s | %zu | %zu | %zu | %.2f | %.2f | %.1f | %.1f | %.0f | %.0f | %.2fx |\n",
                    codec,
                    width,
                    rows,
                    bytes,
                    old * 1e3,
                    now * 1e3,
                    static_cast<double>(bytes) / old / 1e6,
                    static_cast<double>(bytes) / now / 1e6,
                    static_cast<double>(rows) / old,
                    static_cast<double>(rows) / now,
                    old / now);
                std::fflush(stdout);
            };
            report("append parse",
                body.size(),
                SecondsPerRun(
                    [&] {
                        (void)ParseOld(schema, body);
                    },
                    minimum),
                SecondsPerRun(
                    [&] {
                        (void)ParseNew(schema, body);
                    },
                    minimum));
            const TRowJsonBudget budget{.MaxRows = rows, .TargetBytes = SIZE_MAX, .HardBytes = SIZE_MAX};
            std::string reference = "[", direct = "[";
            AppendRowsJsonReference(*expected, reference, budget);
            AppendRowsJson(*expected, direct, budget);
            if (reference != direct) {
                std::fprintf(stderr, "encode mismatch\n");
                return 1;
            }
            const auto encode = [&](auto function) {
                return SecondsPerRun(
                    [&] {
                        std::string output = "[";
                        function(*expected, output, budget);
                    },
                    minimum);
            };
            report("row encode", reference.size(), encode(AppendRowsJsonReference), encode(AppendRowsJson));
        }
    }
    return 0;
}
