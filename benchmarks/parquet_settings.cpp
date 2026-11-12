// Compares collection write settings end to end through the public engine:
// stored bytes, durable append latency, full-scan latency and process peak
// RSS. Run one configuration per process so peak RSS belongs to it.
#include "storage/engine.h"
#include "tests/support/temp_dir.h"

#include <arrow/api.h>
#include <arrow/util/byte_size.h>

#include <sys/resource.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <format>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using namespace glamdb;
using TClock = std::chrono::steady_clock;

constexpr int64_t Rows = 10000;
constexpr char StoreID[] = "00000000-0000-4000-8000-00000000f001";
constexpr char CollectionID[] = "00000000-0000-4000-8000-00000000f002";

void Require(bool condition, const std::string& message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <class T>
T Take(Result<T> result)
{
    Require(result.has_value(), result ? std::string() : result.error().code + ": " + result.error().message);
    return std::move(*result);
}

int Number(std::string_view text, int minimum, int maximum)
{
    int value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    Require(error == std::errc{} && end == text.data() + text.size() && value >= minimum && value <= maximum,
        "invalid number " + std::string(text));
    return value;
}

ParquetCompression Compression(std::string_view text)
{
    const auto colon = text.find(':');
    const auto name = text.substr(0, colon);
    const std::vector<std::pair<std::string_view, ParquetCodec>> codecs{{"uncompressed", ParquetCodec::Uncompressed},
        {"snappy", ParquetCodec::Snappy},
        {"gzip", ParquetCodec::Gzip},
        {"lz4_raw", ParquetCodec::Lz4Raw},
        {"zstd", ParquetCodec::Zstd}};
    const auto codec = std::ranges::find(codecs, name, &std::pair<std::string_view, ParquetCodec>::first);
    Require(codec != codecs.end(), "unknown codec " + std::string(name));
    ParquetCompression result{codec->second, std::nullopt};
    if (colon != std::string_view::npos) {
        result.level = Number(text.substr(colon + 1), -100, 100);
    }
    return result;
}

const SchemaSpec Schema{{
    {"key", FieldType::String, false},
    {"uuid", FieldType::String, true},
    {"status", FieldType::String, true},
    {"text", FieldType::String, true},
    {"seq", FieldType::Int64, true},
    {"rand_i64", FieldType::Int64, true},
    {"small_i64", FieldType::Int64, true},
    {"sensor", FieldType::Float64, true},
    {"price", FieldType::Float64, true},
    {"ts", FieldType::TimestampUsUtc, true},
    {"flag", FieldType::Bool, true},
}};

// "default" keeps the engine defaults; "mixed" keeps zstd but stores the
// incompressible random integers uncompressed; anything else is codec[:level].
WriteSettings Settings(std::string_view config)
{
    if (config == "default") {
        return {};
    }
    if (config == "mixed") {
        return {std::nullopt, {{"rand_i64", ParquetCompression{ParquetCodec::Uncompressed, std::nullopt}}}};
    }
    return {Compression(config), {}};
}

// The same shapes as .audit estimates: sorted KV keys, random hex IDs, a
// 12-value status, word text, monotonic/random/small integers, a random walk,
// log-normal prices, increasing timestamps and a 10% flag.
std::shared_ptr<arrow::RecordBatch> Batch(int64_t offset, std::mt19937_64& random)
{
    static const std::vector<std::string> statuses{"new",
        "paid",
        "shipped",
        "delivered",
        "cancelled",
        "refunded",
        "on_hold",
        "failed",
        "retry",
        "archived",
        "lost",
        "returned"};
    std::vector<std::string> words{"alpha",
        "beta",
        "gamma",
        "delta",
        "order",
        "status",
        "shipped",
        "pending",
        "user",
        "account",
        "payment",
        "refund",
        "error",
        "timeout",
        "retry",
        "warehouse",
        "item",
        "price",
        "total",
        "region"};
    for (int i = 0; i < 400; ++i) {
        words.push_back(std::format("w{}", i));
    }
    arrow::StringBuilder key, uuid, status, text;
    arrow::Int64Builder seq, randomInts, small;
    arrow::DoubleBuilder sensor, price;
    arrow::TimestampBuilder ts(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow::default_memory_pool());
    arrow::BooleanBuilder flag;
    std::uniform_int_distribution<int> statusPick(0, 11), wordCount(5, 29),
        wordPick(0, static_cast<int>(words.size()) - 1);
    std::uniform_int_distribution<int64_t> wide(-(1LL << 62), 1LL << 62), narrow(0, 999), step(500, 5000000);
    std::normal_distribution<double> walk(0, 0.05);
    std::lognormal_distribution<double> prices(3, 1);
    std::bernoulli_distribution flags(0.1);
    static double position = 20;
    static int64_t time = 1762905600000000LL;
    const auto ok = [](const arrow::Status& status) {
        Require(status.ok(), status.ToString());
    };
    for (int64_t row = 0; row < Rows; ++row) {
        ok(key.Append(std::format("user-{:010}", 1000000 + offset + row)));
        ok(uuid.Append(std::format("{:016x}{:016x}", random(), random())));
        ok(status.Append(statuses[static_cast<size_t>(statusPick(random))]));
        std::string sentence;
        for (int count = wordCount(random); count > 0; --count) {
            sentence += (sentence.empty() ? "" : " ") + words[static_cast<size_t>(wordPick(random))];
        }
        ok(text.Append(sentence));
        ok(seq.Append(5000000 + offset + row));
        ok(randomInts.Append(wide(random)));
        ok(small.Append(narrow(random)));
        position += walk(random);
        ok(sensor.Append(std::round(position * 1000) / 1000));
        ok(price.Append(std::round(prices(random) * 100) / 100));
        time += step(random);
        ok(ts.Append(time));
        ok(flag.Append(flags(random)));
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (arrow::ArrayBuilder* builder : std::vector<
             arrow::ArrayBuilder*>{&key, &uuid, &status, &text, &seq, &randomInts, &small, &sensor, &price, &ts, &flag})
    {
        std::shared_ptr<arrow::Array> array;
        ok(builder->Finish(&array));
        arrays.push_back(std::move(array));
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t i = 0; i < Schema.fields.size(); ++i) {
        fields.push_back(arrow::field(Schema.fields[i].name, arrays[i]->type(), Schema.fields[i].nullable));
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), Rows, arrays);
}

double Median(std::vector<double> values)
{
    std::ranges::sort(values);
    return values[values.size() / 2];
}

} // namespace

int main(int argc, char** argv)
{
    try {
        Require(argc >= 2 && argc <= 3,
            "usage: glamdb_parquet_settings_bench <default|mixed|codec[:level]> [appends=5]");
        const std::string config = argv[1];
        const int appends = argc == 3 ? Number(argv[2], 1, 100) : 5;
        test::TempDir temporary;
        const auto engine = Take(CreateStore({StoreID, (temporary.path / "store").string()}));
        const auto created = engine->CreateCollection({CollectionID, "bench", Schema, Settings(config)});
        Require(created.disposition == CommitDisposition::Committed,
            created.error ? created.error->code + ": " + created.error->message : "create failed");

        std::mt19937_64 random(20261112);
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        int64_t arrowBytes = 0;
        for (int i = 0; i < appends; ++i) {
            batches.push_back(Batch(static_cast<int64_t>(i) * Rows, random));
            arrowBytes += arrow::util::TotalBufferSize(*batches.back());
        }
        std::vector<double> appendMs;
        int64_t storedBytes = 0;
        for (int i = 0; i < appends; ++i) {
            const auto operation = std::format("00000000-0000-4000-8000-{:012}", i + 1);
            const auto start = TClock::now();
            const auto outcome =
                engine->AppendDurable({CollectionID, operation, {}, {batches[static_cast<size_t>(i)]}});
            appendMs.push_back(std::chrono::duration<double, std::milli>(TClock::now() - start).count());
            Require(outcome.disposition == CommitDisposition::Committed && outcome.result,
                outcome.error ? outcome.error->code + ": " + outcome.error->message : "append failed");
            storedBytes += outcome.result->compressed_data_bytes_added;
        }
        batches.clear();
        std::vector<double> scanMs;
        for (int repeat = 0; repeat < 5; ++repeat) {
            const auto start = TClock::now();
            auto cursor = Take(engine->Scan({.collection_id = CollectionID}));
            int64_t rows = 0;
            while (auto batch = Take(cursor->Next())) {
                rows += batch->num_rows();
            }
            scanMs.push_back(std::chrono::duration<double, std::milli>(TClock::now() - start).count());
            Require(rows == static_cast<int64_t>(appends) * Rows, "scan lost rows");
        }
        Require(engine->Shutdown(std::chrono::seconds(10)).has_value(), "shutdown failed");
        rusage usage{};
        getrusage(RUSAGE_SELF, &usage);
        std::cout << std::format(
            "{{\"config\":\"{}\",\"appends\":{},\"rows_per_append\":{},\"arrow_bytes\":{},\"stored_bytes\":{},"
            "\"ratio\":{:.3f},\"append_ms_p50\":{:.2f},\"append_ms_min\":{:.2f},\"scan_ms_p50\":{:.2f},"
            "\"peak_rss_mib\":{:.1f}}}\n",
            config,
            appends,
            Rows,
            arrowBytes,
            storedBytes,
            static_cast<double>(arrowBytes) / static_cast<double>(storedBytes),
            Median(appendMs),
            *std::ranges::min_element(appendMs),
            Median(scanMs),
            static_cast<double>(usage.ru_maxrss) / 1024.0);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
