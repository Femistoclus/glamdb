// This source is copied out of the checkout and built with installed public
// headers only. The compute predicates below never call storage's evaluator.
#include <storage/engine.h>
#include <storage/scan_negotiation.h>

#include <arrow/api.h>

#include <algorithm>
#include <functional>
#include <future>
#include <iostream>
#include <stdexcept>

namespace {
using namespace glamdb;
constexpr char Collection[] = "00112233-4455-6677-8899-aabbccddee20";
const SchemaSpec Schema{{{"key", FieldType::String},
    {"n", FieldType::Int64, true},
    {"text", FieldType::String, true},
    {"amount", FieldType::Decimal, true, 12, 2},
    {"day", FieldType::Date, true},
    {"at", FieldType::TimestampUsUtc, true}}};

void Check(bool ok, const std::string& message)
{
    if (!ok) {
        throw std::runtime_error("negotiation adapter: " + message);
    }
}
template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw std::runtime_error(result.error().code + ": " + result.error().message);
    }
    return std::move(*result);
}
void ArrowOK(const arrow::Status& status)
{
    Check(status.ok(), status.ToString());
}

struct TRow
{
    std::string Key;
    std::optional<int64_t> N;
    std::optional<std::string> Text;
    int64_t Amount;
    int32_t Day;
    int64_t At;
};
std::shared_ptr<arrow::RecordBatch> Batch(const std::vector<TRow>& rows)
{
    const auto decimal = arrow::decimal128(12, 2);
    const auto timestamp = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    arrow::StringBuilder keys, text;
    arrow::Int64Builder n;
    arrow::Decimal128Builder amount(decimal);
    arrow::Date32Builder day;
    arrow::TimestampBuilder at(timestamp, arrow::default_memory_pool());
    for (const auto& row : rows) {
        ArrowOK(keys.Append(row.Key));
        ArrowOK(row.N ? n.Append(*row.N) : n.AppendNull());
        ArrowOK(row.Text ? text.Append(*row.Text) : text.AppendNull());
        ArrowOK(amount.Append(arrow::Decimal128(row.Amount)));
        ArrowOK(day.Append(row.Day));
        ArrowOK(at.Append(row.At));
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays(6);
    ArrowOK(keys.Finish(&arrays[0]));
    ArrowOK(n.Finish(&arrays[1]));
    ArrowOK(text.Finish(&arrays[2]));
    ArrowOK(amount.Finish(&arrays[3]));
    ArrowOK(day.Finish(&arrays[4]));
    ArrowOK(at.Finish(&arrays[5]));
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false),
                                        arrow::field("n", arrow::int64()),
                                        arrow::field("text", arrow::utf8()),
                                        arrow::field("amount", decimal),
                                        arrow::field("day", arrow::date32()),
                                        arrow::field("at", timestamp)}),
        static_cast<int64_t>(rows.size()),
        arrays);
}

template <class T>
const T& Column(const arrow::RecordBatch& batch, const char* name)
{
    const auto column = batch.GetColumnByName(name);
    Check(column != nullptr, std::string("compute input missing: ") + name);
    return static_cast<const T&>(*column);
}
using TTruth = std::optional<bool>;
TTruth Not(TTruth v)
{
    return v ? TTruth(!*v) : std::nullopt;
}
TTruth Or(TTruth a, TTruth b)
{
    if (a == true || b == true) {
        return true;
    }
    return a && b ? TTruth(false) : std::nullopt;
}
TTruth In(const arrow::RecordBatch& batch, int64_t row, const std::vector<std::optional<int64_t>>& values)
{
    const auto& n = Column<arrow::Int64Array>(batch, "n");
    if (n.IsNull(row)) {
        return std::nullopt;
    }
    bool null = false;
    for (const auto value : values) {
        if (!value) {
            null = true;
        } else if (*value == n.Value(row)) {
            return true;
        }
    }
    return null ? std::nullopt : TTruth(false);
}
using TFilter = std::function<TTruth(const arrow::RecordBatch&, int64_t)>;
struct TComputeExpression
{
    TFilter Filter;
};
TScanExpression Expr(std::string id,
    std::vector<int32_t> fields,
    TFilter filter,
    std::optional<Predicate> translated = {},
    EScanTranslation kind = EScanTranslation::Exact)
{
    TScanExpression expression;
    expression.ID = std::move(id);
    expression.Expression = std::make_shared<const TComputeExpression>(TComputeExpression{std::move(filter)});
    for (const auto field : fields) {
        expression.Fields.push_back({field, Schema.fields.at(field - 1)});
    }
    expression.Translated = std::move(translated);
    expression.Translation = kind;
    return expression;
}

struct TOutput
{
    std::vector<std::vector<std::string>> Rows;
    uint64_t Bytes = 0;
    uint64_t Decoded = 0;
};
// A small source/compute adapter: open a native source, independently evaluate
// original opaque residuals, then order/limit globally and remove helper columns.
TOutput
Execute(Engine& engine, const std::shared_ptr<TReadView>& view, const TScanNegotiationRequest& request, bool pushdown)
{
    auto plan = Take(NegotiateScan(Schema, request));
    auto scan = plan.Scan;
    std::vector<TScanExpression> residuals;
    if (pushdown) {
        for (const auto& decision : plan.Expressions) {
            if (decision.Pushdown != EScanPushdown::Exact) {
                residuals.push_back(decision.Original);
            }
        }
    } else {
        scan = {};
        scan.BatchRows = request.BatchRows;
        residuals = request.Conjuncts;
    }
    auto drain = [&](std::shared_ptr<ScanCursor> cursor) {
        TOutput output;
        while (auto batch = Take(cursor->Next())) {
            for (int64_t row = 0; row < batch->num_rows(); ++row) {
                bool matches = true;
                for (const auto& expression : residuals) {
                    const auto compute = std::static_pointer_cast<const TComputeExpression>(expression.Expression);
                    if (compute->Filter(*batch, row) != true) {
                        matches = false;
                        break;
                    }
                }
                if (!matches) {
                    continue;
                }
                std::vector<std::string> values;
                // Materialize the order key separately before final projection.
                if (request.Ordering == EScanOrdering::Ordered) {
                    values.push_back(Column<arrow::StringArray>(*batch, "key").GetString(row));
                }
                const auto fields = request.Projection.value_or(std::vector<int32_t>{1, 2, 3, 4, 5, 6});
                for (size_t i = 0; i < fields.size(); ++i) {
                    const auto column = pushdown ? batch->column(static_cast<int>(plan.FinalProjection[i]))
                                                 : batch->column(fields[i] - 1);
                    auto value = column->GetScalar(row);
                    Check(value.ok(), "compute scalar extraction");
                    values.push_back((*value)->ToString());
                }
                output.Rows.push_back(std::move(values));
            }
        }
        const auto profile = cursor->Profile();
        output.Bytes = static_cast<uint64_t>(profile.data_bytes_read.value.value());
        output.Decoded = static_cast<uint64_t>(profile.rows_decoded.value.value());
        return output;
    };
    TOutput output;
    if (pushdown && request.Execution == EScanExecution::Split) {
        Check(!scan.Limit, "split request must never contain a global limit");
        const auto splits = Take(engine.PlanScan(view, 0, scan, "adapter", {.SplitsPerPage = 1, .TargetBytes = 1}));
        std::vector<std::future<TOutput>> workers;
        for (;;) {
            const auto page = Take(engine.NextScanSplits(*splits, "adapter"));
            for (const auto& split : page.Splits) {
                workers.push_back(std::async(std::launch::async, [&, split] {
                    return drain(Take(engine.ReadSplit(*split, "adapter")));
                }));
            }
            if (page.Done) {
                break;
            }
        }
        for (auto& worker : workers) {
            auto part = worker.get();
            output.Rows.insert(output.Rows.end(), part.Rows.begin(), part.Rows.end());
            output.Bytes += part.Bytes;
            output.Decoded += part.Decoded;
        }
    } else {
        output = drain(Take(engine.ScanReadView(*view, 0, scan, "adapter")));
    }
    if (request.Ordering == EScanOrdering::Ordered) {
        std::ranges::sort(output.Rows);
    }
    // A query-global compute limit applies after every residual and all workers.
    const auto limit = pushdown ? plan.ComputeLimit : request.Limit;
    if (limit && output.Rows.size() > static_cast<size_t>(*limit)) {
        output.Rows.resize(static_cast<size_t>(*limit));
    }
    if (request.Ordering == EScanOrdering::Ordered) {
        for (auto& row : output.Rows) {
            row.erase(row.begin());
        }
    }
    std::ranges::sort(output.Rows);
    return output;
}
} // namespace

void CheckNegotiation(glamdb::Engine& engine, bool write)
{
    using namespace glamdb;
    constexpr int64_t ExactTime = 9007199254740993LL;
    if (write) {
        Check(engine.CreateCollection({Collection, "negotiation", Schema}).disposition == CommitDisposition::Committed,
            "create typed adapter collection");
        const std::vector<std::vector<TRow>> files{{{"a0", 1, "A", 100, -1, ExactTime - 1},
                                                       {"a1", 2, std::string("a\0z", 3), 201, 0, ExactTime},
                                                       {"a2", {}, "é", -300, 1, ExactTime + 1},
                                                       {"a3", -3, {}, -300, 2, ExactTime},
                                                       {"a4", 3, "b", 300, 3, ExactTime},
                                                       {"a5", 4, "aa", 400, 4, ExactTime}},
            {{"b0", 100, std::string(6000, 'q') + "x", 10000, 5, ExactTime},
                {"b1", 101, std::string(6000, 'q') + "y", 10100, 6, ExactTime}},
            {{"c0", {}, std::string(100, 'a') + "x", 1, 7, ExactTime},
                {"c1", {}, std::string(100, 'a') + "y", 2, 8, ExactTime}}};
        for (size_t i = 0; i < files.size(); ++i) {
            const auto outcome = engine.AppendDurable(
                {Collection, "00000000-0000-0000-0000-00000000002" + std::to_string(i), {}, {Batch(files[i])}});
            Check(outcome.disposition == CommitDisposition::Committed, "append oracle fixture");
        }
    }
    const auto files = Take(engine.ListFiles(Collection));
    bool truncated = false;
    bool missing = false;
    int64_t rows = 0;
    for (const auto& file : files.Files) {
        rows += file.RecordCount;
        for (const auto& metric : file.FieldMetrics) {
            if (metric.FieldID == 3) {
                missing = missing || (!metric.LowerBound && !metric.UpperBound);
                truncated = truncated || (metric.LowerBound && metric.LowerBound->size() == 16);
            }
            if (metric.ValueCount) {
                Check(*metric.ValueCount == file.RecordCount, "exact present value count");
            }
        }
    }
    Check(rows == 10 && truncated && missing, "real native truncated and missing bounds fixture");
    const auto view = Take(engine.AcquireReadView({{{Collection, {}}}, "adapter"}));
    auto verify = [&](std::string name, TScanNegotiationRequest request, size_t count) {
        const auto oracle = Execute(engine, view, request, false);
        const auto pushed = Execute(engine, view, request, true);
        Check(oracle.Rows.size() == count, name + " literal oracle count");
        if (request.Limit && request.Ordering == EScanOrdering::Unordered) {
            // Unordered LIMIT promises a matching subset, never a particular
            // physical traversal order across independently scheduled workers.
            auto unlimited = request;
            unlimited.Limit.reset();
            const auto all = Execute(engine, view, unlimited, false);
            Check(pushed.Rows.size() == count && std::ranges::includes(all.Rows, pushed.Rows),
                name + " limited matching multiset/cardinality");
        } else {
            Check(pushed.Rows == oracle.Rows, name + " full-scan compute multiset mismatch");
        }
    };
    auto query = [](TScanExpression expression) {
        TScanNegotiationRequest request;
        request.Conjuncts = {std::move(expression)};
        request.Projection = std::vector<int32_t>{1};
        request.BatchRows = 2;
        return request;
    };
    verify("NOT IN with NULL",
        query(Expr(
            "not-in-null",
            {2},
            [](const auto& b, auto r) {
                return Not(In(b, r, {1, std::nullopt}));
            },
            Predicate::Not(Predicate::In("n", {int64_t{1}, std::nullopt})))),
        0);
    verify("NULL OR NOT IN",
        query(Expr(
            "or",
            {2},
            [](const auto& b, auto r) {
                return Or(Column<arrow::Int64Array>(b, "n").IsNull(r), Not(In(b, r, {1, 3, 100, 101})));
            },
            Predicate::Or({Predicate::IsNull("n"),
                Predicate::Not(Predicate::In("n", {int64_t{1}, int64_t{3}, int64_t{100}, int64_t{101}}))}))),
        6);
    verify("NOT compare NULL",
        query(Expr(
            "null-compare",
            {2},
            [](const auto&, auto) -> TTruth {
                return Not(std::nullopt);
            },
            Predicate::Not(Predicate::CompareNull("n", CompareOp::Eq)))),
        0);
    verify("binary NUL string",
        query(Expr(
            "binary",
            {3},
            [](const auto& b, auto r) -> TTruth {
                const auto& text = Column<arrow::StringArray>(b, "text");
                return text.IsNull(r) ? std::nullopt : TTruth(text.GetString(r) == std::string("a\0z", 3));
            },
            Predicate::Compare("text", CompareOp::Eq, std::string("a\0z", 3)))),
        1);
    verify("binary string order",
        query(Expr(
            "binary-order",
            {3},
            [](const auto& b, auto r) -> TTruth {
                const auto& text = Column<arrow::StringArray>(b, "text");
                return text.IsNull(r) ? std::nullopt : TTruth(text.GetString(r) < "b");
            },
            Predicate::Compare("text", CompareOp::Lt, std::string("b")))),
        5);
    verify("decimal coefficient",
        query(Expr(
            "decimal",
            {4},
            [](const auto& b, auto r) -> TTruth {
                const auto& amount = Column<arrow::Decimal128Array>(b, "amount");
                return arrow::Decimal128(amount.GetValue(r)) == arrow::Decimal128(201);
            },
            Predicate::Compare("amount", CompareOp::Eq, Decimal{0, 201, 12, 2}))),
        1);
    verify("calendar day",
        query(Expr(
            "date",
            {5},
            [](const auto& b, auto r) -> TTruth {
                return Column<arrow::Date32Array>(b, "day").Value(r) == -1;
            },
            Predicate::Compare("day", CompareOp::Eq, Date{-1}))),
        1);
    verify("UTC exact microsecond",
        query(Expr(
            "timestamp",
            {6},
            [](const auto& b, auto r) -> TTruth {
                return Column<arrow::TimestampArray>(b, "at").Value(r) == ExactTime;
            },
            Predicate::Compare("at", CompareOp::Eq, TimestampUs{ExactTime}))),
        8);
    auto residual = query(Expr("cast-to-text", {2}, [](const auto& b, auto r) -> TTruth {
        const auto& n = Column<arrow::Int64Array>(b, "n");
        return n.IsNull(r) ? std::nullopt : TTruth(std::to_string(n.Value(r)) == "3");
    }));
    residual.Limit = 1;
    verify("opaque cast then limit", residual, 1);
    residual.Projection = std::vector<int32_t>{};
    verify("zero final columns with residual then limit", residual, 1);
    residual.Limit = 0;
    verify("residual limit zero", residual, 0);
    auto necessary = query(Expr(
        "abs",
        {2},
        [](const auto& b, auto r) -> TTruth {
            const auto& n = Column<arrow::Int64Array>(b, "n");
            return n.IsNull(r) ? std::nullopt : TTruth(n.Value(r) == -3 || n.Value(r) == 3);
        },
        Predicate::And(
            {Predicate::Compare("n", CompareOp::Ge, int64_t{-3}), Predicate::Compare("n", CompareOp::Le, int64_t{3})}),
        EScanTranslation::NecessaryCondition));
    verify("necessary condition retains abs residual", necessary, 2);
    necessary.Execution = EScanExecution::Split;
    verify("parallel split residual union", necessary, 2);
    necessary.Limit = 1;
    verify("parallel split global limit", necessary, 1);
    necessary.Ordering = EScanOrdering::Ordered;
    necessary.ComputeFields = {{1, Schema.fields[0]}};
    Check(Take(NegotiateScan(Schema, necessary)).Order == EScanOrderSupport::Unsupported, "Top-N explicit unsupported");
    verify("compute ordered Top-N", necessary, 1);
    auto parallelResidual = query(Expr("length", {3}, [](const auto& b, auto r) -> TTruth {
        const auto& text = Column<arrow::StringArray>(b, "text");
        return text.IsNull(r) ? std::nullopt : TTruth(text.GetString(r).size() > 1);
    }));
    parallelResidual.Execution = EScanExecution::Split;
    verify("opaque function across all splits", parallelResidual, 7);
    parallelResidual.Limit = 4;
    verify("residual global limit spans multiple splits", parallelResidual, 4);
    parallelResidual.Projection = std::vector<int32_t>{};
    verify("parallel residual zero-column cardinality", parallelResidual, 4);
    parallelResidual.Ordering = EScanOrdering::Ordered;
    parallelResidual.ComputeFields = {{1, Schema.fields[0]}};
    verify("ordered zero-column output retains compute sort input", parallelResidual, 4);
    auto partialOr = query(Expr("opaque-or", {2, 3}, [](const auto& b, auto r) {
        const auto& text = Column<arrow::StringArray>(b, "text");
        const auto length = text.IsNull(r) ? std::nullopt : TTruth(text.GetString(r).size() > 5000);
        return Or(In(b, r, {1}), length);
    }));
    verify("partial OR remains wholly residual", partialOr, 3);
    for (const auto& value : {std::string(100, 'a') + "x", std::string(6000, 'q') + "y"}) {
        verify("truncated or missing string bounds",
            query(Expr(
                "long-string",
                {3},
                [value](const auto& b, auto r) -> TTruth {
                    const auto& text = Column<arrow::StringArray>(b, "text");
                    return text.IsNull(r) ? std::nullopt : TTruth(text.GetString(r) == value);
                },
                Predicate::Compare("text", CompareOp::Eq, value))),
            1);
    }
    // Correctness and saved work are separate checks. No timing/physical-I/O claims.
    auto selective = query(Expr(
        "small",
        {2},
        [](const auto& b, auto r) -> TTruth {
            const auto& n = Column<arrow::Int64Array>(b, "n");
            return n.IsNull(r) ? std::nullopt : TTruth(n.Value(r) < 10);
        },
        Predicate::Compare("n", CompareOp::Lt, int64_t{10})));
    auto exactLimit = selective;
    exactLimit.Limit = 0;
    verify("exact serial limit zero", exactLimit, 0);
    exactLimit.Limit = 2;
    verify("exact serial limit", exactLimit, 2);
    const auto baseline = Execute(engine, view, selective, false);
    const auto optimized = Execute(engine, view, selective, true);
    Check(baseline.Rows == optimized.Rows && optimized.Rows.size() == 5, "selective literal multiset");
    Check(optimized.Bytes < baseline.Bytes && optimized.Decoded < baseline.Decoded, "saved source bytes and rows");
    std::cout << "Negotiation adapter correctness PASS; source work full=" << baseline.Bytes << " bytes/"
              << baseline.Decoded << " rows, pushed=" << optimized.Bytes << " bytes/" << optimized.Decoded << " rows\n";
    Check(engine.ReleaseReadView(*view, "adapter").has_value(), "release adapter view");
}
