#include "storage/resources.h"
#include "storage/scan_negotiation.h"

#include <gtest/gtest.h>

namespace glamdb {
namespace {
const SchemaSpec Schema{
    {{"key", FieldType::String}, {"n", FieldType::Int64, true}, {"amount", FieldType::Decimal, true, 12, 2}}};

TScanExpression Expression(std::string id,
    std::optional<Predicate> predicate = {},
    EScanTranslation translation = EScanTranslation::Exact)
{
    return {.ID = std::move(id),
        .Expression = std::make_shared<const int>(42),
        .Fields = {{2, Schema.fields[1]}},
        .Translated = std::move(predicate),
        .Translation = translation};
}

TEST(ScanNegotiation, KeepsOpaqueResidualsAndAuxiliaryColumns)
{
    TScanNegotiationRequest request;
    request.Conjuncts = {Expression("exact", Predicate::IsNotNull("n")),
        Expression("cast"),
        Expression("function",
            Predicate::Compare("n", CompareOp::Ge, int64_t{0}),
            EScanTranslation::NecessaryCondition)};
    request.Projection = std::vector<int32_t>{1};
    request.Limit = 2;
    const auto result = NegotiateScan(Schema, request);
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(result->Expressions[0].Pushdown, EScanPushdown::Exact);
    EXPECT_EQ(result->Expressions[1].Pushdown, EScanPushdown::Unsupported);
    EXPECT_EQ(result->Expressions[2].Pushdown, EScanPushdown::PruningOnly);
    EXPECT_EQ(result->ResidualIDs, (std::vector<std::string>{"cast", "function"}));
    EXPECT_EQ(result->Expressions[2].Original.Expression.get(), request.Conjuncts[2].Expression.get());
    EXPECT_EQ(result->StorageFieldIDs, (std::vector<int32_t>{1, 2}));
    EXPECT_EQ(*result->Scan.Projection, (std::vector<std::string>{"key", "n"}));
    EXPECT_EQ(result->FinalProjection, (std::vector<size_t>{0}));
    EXPECT_EQ(result->ComputeLimit, 2);
    EXPECT_FALSE(result->Scan.Limit);
    EXPECT_EQ(result->Limit, EScanLimit::Residual);
    request.Projection = std::vector<int32_t>{};
    const auto zero = NegotiateScan(Schema, request);
    ASSERT_TRUE(zero);
    EXPECT_TRUE(zero->FinalProjection.empty());
    EXPECT_EQ(zero->StorageFieldIDs, (std::vector<int32_t>{2}));
}

TEST(ScanNegotiation, PushesLimitsOnlyForCompleteUnorderedSerialFilter)
{
    TScanNegotiationRequest request;
    request.Conjuncts = {Expression("exact", Predicate::IsNotNull("n"))};
    request.Projection = std::vector<int32_t>{};
    for (const auto limit : {int64_t{0}, int64_t{2}}) {
        request.Limit = limit;
        request.Execution = EScanExecution::Serial;
        request.Ordering = EScanOrdering::Unordered;
        auto result = NegotiateScan(Schema, request);
        ASSERT_TRUE(result);
        EXPECT_EQ(result->Scan.Limit, limit);
        EXPECT_FALSE(result->ComputeLimit);
        EXPECT_TRUE(result->StorageFieldIDs.empty()); // exact filter-only columns stay internal
        EXPECT_EQ(result->Limit, EScanLimit::Pushed);
        request.Execution = EScanExecution::Split;
        result = NegotiateScan(Schema, request);
        ASSERT_TRUE(result);
        EXPECT_FALSE(result->Scan.Limit);
        EXPECT_EQ(result->ComputeLimit, limit);
        EXPECT_EQ(result->Limit, EScanLimit::Split);
        request.Ordering = EScanOrdering::Ordered;
        result = NegotiateScan(Schema, request);
        ASSERT_TRUE(result);
        EXPECT_FALSE(result->Scan.Limit);
        EXPECT_EQ(result->ComputeLimit, limit);
        EXPECT_EQ(result->Limit, EScanLimit::Ordered);
        EXPECT_EQ(result->Order, EScanOrderSupport::Unsupported);
    }
}

TEST(ScanNegotiation, InvalidTypedTranslationsRemainOriginalResiduals)
{
    TScanNegotiationRequest request;
    request.Conjuncts = {Expression("bad-cast", Predicate::Compare("n", CompareOp::Eq, std::string("1"))),
        Expression("wrong-field", Predicate::IsNotNull("key"))};
    auto decimal = Expression("decimal", Predicate::Compare("amount", CompareOp::Eq, Decimal{0, 1, 12, 3}));
    decimal.Fields = {{3, Schema.fields[2]}};
    request.Conjuncts.push_back(decimal);
    const auto result = NegotiateScan(Schema, request);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->ResidualIDs, (std::vector<std::string>{"bad-cast", "wrong-field", "decimal"}));
    EXPECT_EQ(result->Scan.Filter.kind(), Predicate::Kind::True);
    for (size_t i = 0; i < request.Conjuncts.size(); ++i) {
        EXPECT_EQ(result->Expressions[i].Pushdown, EScanPushdown::Unsupported);
        EXPECT_TRUE(result->Expressions[i].TranslationError);
        EXPECT_EQ(result->Expressions[i].Original.Expression, request.Conjuncts[i].Expression);
    }
}

TEST(ScanNegotiation, RejectsAmbiguousIdentitiesMappingsAndInvalidRequests)
{
    TScanNegotiationRequest good;
    good.Conjuncts = {Expression("f")};
    auto check = [&](TScanNegotiationRequest request) {
        const auto result = NegotiateScan(Schema, request);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, "INVALID_ARGUMENT");
    };
    auto bad = good;
    bad.Conjuncts.push_back(bad.Conjuncts.front());
    check(bad);
    bad = good;
    bad.Conjuncts[0].ID.clear();
    check(bad);
    bad = good;
    bad.Conjuncts[0].Expression.reset();
    check(bad);
    bad = good;
    bad.Conjuncts[0].Fields[0].FieldID = 0;
    check(bad);
    bad = good;
    bad.Conjuncts[0].Fields[0].Spec.nullable = false;
    check(bad);
    bad = good;
    bad.Conjuncts[0].Fields.push_back(bad.Conjuncts[0].Fields.front());
    check(bad);
    bad = good;
    bad.Projection = std::vector<int32_t>{1, 1};
    check(bad);
    bad = good;
    bad.Projection = std::vector<int32_t>{4};
    check(bad);
    bad = good;
    bad.Limit = -1;
    check(bad);
    bad = good;
    bad.BatchRows = 0;
    check(bad);
    bad = good;
    bad.Execution = static_cast<EScanExecution>(99);
    check(bad);
    bad = good;
    bad.Ordering = static_cast<EScanOrdering>(99);
    check(bad);
    bad = good;
    bad.Conjuncts[0].Translation = static_cast<EScanTranslation>(99);
    check(bad);
    bad = good;
    bad.Conjuncts.resize(65, good.Conjuncts[0]);
    check(bad);
}
TEST(ScanNegotiation, RetainsOrderingInputsOutsideFinalProjection)
{
    TScanNegotiationRequest request;
    request.Projection = std::vector<int32_t>{};
    request.ComputeFields = {{2, Schema.fields[1]}};
    request.Ordering = EScanOrdering::Ordered;
    const auto result = NegotiateScan(Schema, request);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->StorageFieldIDs, (std::vector<int32_t>{2}));
    EXPECT_TRUE(result->FinalProjection.empty());
    EXPECT_EQ(result->Order, EScanOrderSupport::Unsupported);
    request.ComputeFields[0].Spec.type = FieldType::Float64;
    EXPECT_FALSE(NegotiateScan(Schema, request));
}

TEST(ScanNegotiation, AggregatePredicateLimitKeepsOverflowConjunctResidual)
{
    std::vector<Predicate> terms(600, Predicate::IsNotNull("n"));
    TScanNegotiationRequest request;
    request.Conjuncts = {Expression("first", Predicate::And(terms)), Expression("second", Predicate::And(terms))};
    request.Projection = std::vector<int32_t>{1};
    request.Limit = 1;
    const auto result = NegotiateScan(Schema, request);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->Expressions[0].Pushdown, EScanPushdown::Exact);
    EXPECT_EQ(result->Expressions[1].Pushdown, EScanPushdown::Unsupported);
    EXPECT_TRUE(result->Expressions[1].TranslationError);
    EXPECT_EQ(result->ResidualIDs, (std::vector<std::string>{"second"}));
    EXPECT_EQ(result->StorageFieldIDs, (std::vector<int32_t>{1, 2}));
    EXPECT_FALSE(result->Scan.Limit);
    EXPECT_EQ(result->ComputeLimit, 1);
}

TEST(ScanNegotiation, RefusesMetadataPressureAndRetainsExtractedExpressions)
{
    ResourceLimits limits;
    limits.memory_bytes[static_cast<size_t>(ResourceClass::Metadata)] = 1;
    const auto small = std::make_shared<ResourceOwner>(limits);
    TScanNegotiationRequest request;
    request.Conjuncts = {Expression("f", Predicate::IsNotNull("n"))};
    const auto denied = NegotiateScan(Schema, request, small);
    ASSERT_FALSE(denied);
    EXPECT_EQ(denied.error().code, "RESOURCE_EXHAUSTED");
    auto owner = std::make_shared<ResourceOwner>();
    auto result = NegotiateScan(Schema, request, owner);
    ASSERT_TRUE(result);
    auto expression = result->Expressions.front().Original;
    auto predicate = result->Scan.Filter;
    result = std::unexpected(ErrorDetail{});
    EXPECT_GT(owner->GetUsage().memory_bytes[static_cast<size_t>(ResourceClass::Metadata)], 0U);
    expression = {};
    EXPECT_GT(owner->GetUsage().memory_bytes[static_cast<size_t>(ResourceClass::Metadata)], 0U);
    predicate = {};
    EXPECT_EQ(owner->GetUsage().memory_bytes[static_cast<size_t>(ResourceClass::Metadata)], 0U);
}
} // namespace
} // namespace glamdb
