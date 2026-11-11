#include "core/resources/metadata_result.h"

#include <gtest/gtest.h>

namespace glamdb::detail {
namespace {
TEST(MetadataResources, CompoundPredicatesIncludeNestedLiteralCapacity)
{
    ResourceLimits limits;
    limits.memory_bytes[1] = 512ULL << 10;
    auto owner = std::make_shared<ResourceOwner>(limits);
    ScanExplanation explanation;
    explanation.predicate = Predicate::And({Predicate::In("label", {std::string(1ULL << 20, 'x')})});
    const auto retained = RetainMetadata(owner, explanation);
    EXPECT_FALSE(retained);
    if (!retained) {
        EXPECT_EQ(retained.error().code, "RESOURCE_EXHAUSTED");
    }
}

TEST(MetadataResources, PredicateVectorCapacityIncludesUnusedSlots)
{
    ResourceLimits limits;
    limits.memory_bytes[1] = 64ULL << 10;
    auto owner = std::make_shared<ResourceOwner>(limits);
    std::vector<OptionalLiteral> values;
    values.reserve(10000);
    values.emplace_back(int64_t{1});
    ScanExplanation explanation;
    explanation.predicate = Predicate::In("value", std::move(values));
    EXPECT_FALSE(RetainMetadata(owner, explanation));
    EXPECT_EQ(owner->GetUsage().memory_bytes[1], 0);
}

TEST(MetadataResources, ExtractedPredicateSubtreesKeepTheirReservation)
{
    auto owner = std::make_shared<ResourceOwner>();
    Predicate predicate;
    Predicate child;
    {
        ScanExplanation explanation;
        explanation.predicate = Predicate::And({Predicate::In("label", {std::string(64ULL << 10, 'x')})});
        ASSERT_TRUE(RetainMetadata(owner, explanation));
        EXPECT_GT(owner->GetUsage().memory_bytes[1], 64ULL << 10);
        predicate = explanation.predicate;
        child = predicate.children().front();
    }
    EXPECT_GT(owner->GetUsage().memory_bytes[1], 64ULL << 10);
    predicate = {};
    EXPECT_GT(owner->GetUsage().memory_bytes[1], 64ULL << 10);
    child = {};
    EXPECT_EQ(owner->GetUsage().memory_bytes[1], 0);
}

TEST(MetadataResources, ManySmallBoundsRequireAllocationHeadroom)
{
    ResourceLimits limits;
    limits.memory_bytes[1] = 1ULL << 20;
    auto owner = std::make_shared<ResourceOwner>(limits);
    TFileListing listing;
    listing.Files.resize(64);
    for (auto& file : listing.Files) {
        file.FileID = std::string(64, 'a');
        file.FieldMetrics.resize(64);
        for (auto& field : file.FieldMetrics) {
            field.LowerBound = std::vector<uint8_t>(8, 0);
            field.UpperBound = std::vector<uint8_t>(8, 1);
        }
    }
    // Payload capacities fit in 1 MiB; thousands of allocation headers and
    // rounded small bounds need additional capacity before returning this DTO.
    const auto retained = RetainMetadata(owner, listing);
    EXPECT_FALSE(retained);
    if (!retained) {
        EXPECT_EQ(retained.error().code, "RESOURCE_EXHAUSTED");
    }
    EXPECT_EQ(owner->GetUsage().memory_bytes[1], 0);
}
} // namespace
} // namespace glamdb::detail
