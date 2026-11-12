#include "core/engine/storage_mapping.h"
#include "core/scan/predicate.h"

#include <gtest/gtest.h>

#if GLAMDB_TEST_STORAGE

#include "core/scan/pruning.h"
#include "core/storage/table_profile.h"

#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/schema.h>

#include <array>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

namespace glamdb::detail {
namespace {

constexpr Decimal LowAmount{0, 1234, 15, 2};
constexpr Decimal HighAmount{0, 5678, 15, 2};
constexpr Date LowDay{-1};
constexpr Date HighDay{0};

const SchemaSpec ScalarSchema{{
    {"key", FieldType::String, false},
    {"amount", FieldType::Decimal, true, 15, 2},
    {"day", FieldType::Date, true},
    {"gate", FieldType::Int64, false},
}};

const std::vector<Row> MixedRows{
    {std::string("low"), LowAmount, LowDay, int64_t{1}},
    {std::string("high"), HighAmount, HighDay, int64_t{1}},
    {std::string("null"), std::monostate{}, std::monostate{}, int64_t{1}},
};

std::vector<TColumnZoneStats> ScalarStats(int64_t nullCount = 1, bool withBounds = false)
{
    std::vector<TColumnZoneStats> stats;
    stats.reserve(ScalarSchema.fields.size());
    for (const auto& field : ScalarSchema.fields) {
        stats.push_back({.Type = field.type,
            .Nullable = field.nullable,
            .HasStats = true,
            .NullCount = field.nullable ? nullCount : int64_t{0}});
    }
    stats[3].LowerBound = Literal{int64_t{1}};
    stats[3].UpperBound = Literal{int64_t{1}};
    if (withBounds) {
        stats[1].LowerBound = Literal{LowAmount};
        stats[1].UpperBound = Literal{HighAmount};
        stats[2].LowerBound = Literal{LowDay};
        stats[2].UpperBound = Literal{HighDay};
    }
    return stats;
}

void CheckScalarZone(const Predicate& predicate,
    const std::vector<Row>& rows,
    const std::vector<TColumnZoneStats>& stats,
    const std::vector<int64_t>& expectedRows,
    TZoneTruth expectedTruth,
    bool expectedPrune = false)
{
    const auto batch = MakeRecordBatch(ScalarSchema, rows);
    ASSERT_TRUE(batch) << batch.error().message;
    const auto bound = BindPredicate(predicate, ScalarSchema);
    ASSERT_TRUE(bound) << bound.error().message;
    const auto selected = SelectRows(*bound, **batch);
    ASSERT_TRUE(selected) << selected.error().message;
    EXPECT_EQ(*selected, expectedRows);

    std::vector<size_t> fields(ScalarSchema.fields.size());
    std::iota(fields.begin(), fields.end(), size_t{0});
    const auto truth = EvaluateZonePredicate(*bound, static_cast<int64_t>(rows.size()), fields, stats);
    ASSERT_TRUE(truth) << truth.error().message;
    EXPECT_EQ(*truth, expectedTruth);
    const auto prune = CanPruneZone(*bound, static_cast<int64_t>(rows.size()), fields, stats);
    ASSERT_TRUE(prune) << prune.error().message;
    EXPECT_EQ(*prune, expectedPrune);
}

TEST(DecimalDatePruningTest, CountsOnlyInPreservesGenuineDecimalAndDateMatches)
{
    const auto stats = ScalarStats();
    for (const auto& [field, value] :
        std::vector<std::pair<std::string, Literal>>{{"amount", HighAmount}, {"day", HighDay}})
    {
        SCOPED_TRACE(field);
        CheckScalarZone(Predicate::In(field, {value}),
            MixedRows,
            stats,
            {1},
            {.CanBeTrue = true, .CanBeFalse = true, .CanBeUnknown = true});
        CheckScalarZone(Predicate::In(field, {value, std::nullopt}),
            MixedRows,
            stats,
            {1},
            {.CanBeTrue = true, .CanBeFalse = false, .CanBeUnknown = true});
        CheckScalarZone(Predicate::Not(Predicate::In(field, {value})),
            MixedRows,
            stats,
            {0},
            {.CanBeTrue = true, .CanBeFalse = true, .CanBeUnknown = true});
        CheckScalarZone(Predicate::Not(Predicate::In(field, {value, std::nullopt})),
            MixedRows,
            stats,
            {},
            {.CanBeTrue = false, .CanBeFalse = true, .CanBeUnknown = true},
            true);
    }
}

TEST(DecimalDatePruningTest, ComparisonsAndNegationPreserveBothOutcomesWithCountsOrBounds)
{
    struct TComparison
    {
        CompareOp Op;
        bool UseHigh;
        std::vector<int64_t> Matches;
        std::vector<int64_t> NegatedMatches;
    };
    const std::array<TComparison, 6> comparisons{{
        {CompareOp::Eq, true, {1}, {0}},
        {CompareOp::Ne, true, {0}, {1}},
        {CompareOp::Lt, true, {0}, {1}},
        {CompareOp::Le, false, {0}, {1}},
        {CompareOp::Gt, false, {1}, {0}},
        {CompareOp::Ge, true, {1}, {0}},
    }};
    for (const bool withBounds : {false, true}) {
        const auto stats = ScalarStats(1, withBounds);
        SCOPED_TRACE(withBounds);
        for (const auto& comparison : comparisons) {
            const std::array<std::pair<std::string, Literal>, 2> values{{
                {"amount", comparison.UseHigh ? HighAmount : LowAmount},
                {"day", comparison.UseHigh ? HighDay : LowDay},
            }};
            for (const auto& [field, value] : values) {
                SCOPED_TRACE(field);
                SCOPED_TRACE(static_cast<int>(comparison.Op));
                const auto predicate = Predicate::Compare(field, comparison.Op, value);
                CheckScalarZone(predicate,
                    MixedRows,
                    stats,
                    comparison.Matches,
                    {.CanBeTrue = true, .CanBeFalse = true, .CanBeUnknown = true});
                CheckScalarZone(Predicate::Not(predicate),
                    MixedRows,
                    stats,
                    comparison.NegatedMatches,
                    {.CanBeTrue = true, .CanBeFalse = true, .CanBeUnknown = true});
            }
        }
    }
}

TEST(DecimalDatePruningTest, EmptyAndNullOnlyInSetsRetainThreeValuedLogic)
{
    const auto stats = ScalarStats();
    for (const auto& field : {"amount", "day"}) {
        SCOPED_TRACE(field);
        CheckScalarZone(Predicate::In(field, {}),
            MixedRows,
            stats,
            {},
            {.CanBeTrue = false, .CanBeFalse = true, .CanBeUnknown = true},
            true);
        CheckScalarZone(Predicate::Not(Predicate::In(field, {})),
            MixedRows,
            stats,
            {0, 1},
            {.CanBeTrue = true, .CanBeFalse = false, .CanBeUnknown = true});
        CheckScalarZone(Predicate::In(field, {std::nullopt}),
            MixedRows,
            stats,
            {},
            {.CanBeTrue = false, .CanBeFalse = false, .CanBeUnknown = true},
            true);
        CheckScalarZone(Predicate::Not(Predicate::In(field, {std::nullopt})),
            MixedRows,
            stats,
            {},
            {.CanBeTrue = false, .CanBeFalse = false, .CanBeUnknown = true},
            true);
    }
}

TEST(DecimalDatePruningTest, AllNullScalarZonesRetainGenericNullSemantics)
{
    const std::vector<Row> rows{
        {std::string("null-a"), std::monostate{}, std::monostate{}, int64_t{1}},
        {std::string("null-b"), std::monostate{}, std::monostate{}, int64_t{1}},
    };
    const auto stats = ScalarStats(2);
    for (const auto& [field, value] :
        std::vector<std::pair<std::string, Literal>>{{"amount", HighAmount}, {"day", HighDay}})
    {
        SCOPED_TRACE(field);
        for (const auto& predicate : {Predicate::Compare(field, CompareOp::Eq, value),
                 Predicate::Not(Predicate::Compare(field, CompareOp::Ne, value)),
                 Predicate::In(field, {value}),
                 Predicate::Not(Predicate::In(field, {value})),
                 Predicate::CompareNull(field, CompareOp::Eq)})
        {
            CheckScalarZone(predicate,
                rows,
                stats,
                {},
                {.CanBeTrue = false, .CanBeFalse = false, .CanBeUnknown = true},
                true);
        }
        CheckScalarZone(Predicate::IsNull(field),
            rows,
            stats,
            {0, 1},
            {.CanBeTrue = true, .CanBeFalse = false, .CanBeUnknown = false});
        CheckScalarZone(Predicate::IsNotNull(field),
            rows,
            stats,
            {},
            {.CanBeTrue = false, .CanBeFalse = true, .CanBeUnknown = false},
            true);
    }
}

TEST(DecimalDatePruningTest, CompoundPredicatesPreserveMatchesAndSafeSiblingPruning)
{
    const auto stats = ScalarStats();
    const auto gateTrue = Predicate::Compare("gate", CompareOp::Eq, int64_t{1});
    const auto gateFalse = Predicate::Compare("gate", CompareOp::Eq, int64_t{0});
    for (const auto& [field, value] :
        std::vector<std::pair<std::string, Literal>>{{"amount", HighAmount}, {"day", HighDay}})
    {
        SCOPED_TRACE(field);
        const auto scalarIn = Predicate::In(field, {value});
        CheckScalarZone(Predicate::And({scalarIn, gateTrue}),
            MixedRows,
            stats,
            {1},
            {.CanBeTrue = true, .CanBeFalse = true, .CanBeUnknown = true});
        CheckScalarZone(Predicate::Or({scalarIn, gateFalse}),
            MixedRows,
            stats,
            {1},
            {.CanBeTrue = true, .CanBeFalse = true, .CanBeUnknown = true});
        CheckScalarZone(Predicate::And({scalarIn, gateFalse}),
            MixedRows,
            stats,
            {},
            {.CanBeTrue = false, .CanBeFalse = true, .CanBeUnknown = false},
            true);
        CheckScalarZone(Predicate::Or({scalarIn, gateTrue}),
            MixedRows,
            stats,
            {0, 1, 2},
            {.CanBeTrue = true, .CanBeFalse = false, .CanBeUnknown = false});
        CheckScalarZone(Predicate::Not(Predicate::Or({scalarIn, gateFalse})),
            MixedRows,
            stats,
            {0},
            {.CanBeTrue = true, .CanBeFalse = true, .CanBeUnknown = true});
    }
}

TEST(DecimalDatePruningTest, UnsupportedManifestBoundsCannotPruneMatchingRows)
{
    const auto profile = NGlamDB::NStorage::MakeTableProfile(ToStorageSchema(ScalarSchema));
    ASSERT_TRUE(profile) << profile.error().message;
    iceberg::DataFile file;
    file.record_count = 3;
    // Iceberg decimal coefficients are big endian; DATE bounds are signed little-endian int32.
    file.value_counts = {{2, 3}, {3, 3}};
    file.null_value_counts = {{2, 1}, {3, 1}};
    file.lower_bounds = {{2, {0x04, 0xd2}}, {3, {0xff, 0xff, 0xff, 0xff}}};
    file.upper_bounds = {{2, {0x16, 0x2e}}, {3, {0x00, 0x00, 0x00, 0x00}}};
    const std::array<size_t, 2> fields{1, 2};
    const auto stats = ExtractFileZoneStats(file, *profile->Schema, ScalarSchema, fields);
    ASSERT_TRUE(stats) << stats.error().message;
    for (const auto& [field, value] :
        std::vector<std::pair<std::string, Literal>>{{"amount", HighAmount}, {"day", HighDay}})
    {
        SCOPED_TRACE(field);
        const auto bound = BindPredicate(Predicate::In(field, {value}), ScalarSchema);
        ASSERT_TRUE(bound) << bound.error().message;
        const auto prune = CanPruneZone(*bound, file.record_count, fields, *stats);
        ASSERT_TRUE(prune) << prune.error().message;
        EXPECT_FALSE(*prune);
    }
}

} // namespace
} // namespace glamdb::detail

#endif // GLAMDB_TEST_STORAGE
