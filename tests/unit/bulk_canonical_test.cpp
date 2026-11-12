#include "core/api/canonical.h"
#include "tests/vertical/vertical_support.h"

namespace glamdb::detail {
namespace {

TEST(BulkCanonical, ChunkBoundariesPreserveOrdinaryCanonicalBytes)
{
    const auto first = vertical::Batch(vertical::FirstRows);
    const auto second = vertical::Batch(vertical::SecondRows);
    const auto expected = FingerprintBatches(vertical::Schema, test::collection_id, {first, second});
    ASSERT_TRUE(expected);
    const auto header =
        EncodeBulkAppendHeader(vertical::Schema, test::collection_id, first->num_rows() + second->num_rows());
    ASSERT_TRUE(header);
    TCanonicalHasher hash;
    hash.AddBytes(*header);
    ASSERT_TRUE(UpdateFingerprintBatches(vertical::Schema, {first}, hash));
    ASSERT_TRUE(UpdateFingerprintBatches(vertical::Schema, {second}, hash));
    EXPECT_EQ(hash.ComputeDigest(), *expected);
}

TEST(BulkCanonical, OnlyBulkExtendsTheDeclaredRowCount)
{
    EXPECT_FALSE(EncodeAppendHeader(vertical::Schema, test::collection_id, 10'001));
    EXPECT_TRUE(EncodeBulkAppendHeader(vertical::Schema, test::collection_id, 10'001));
    EXPECT_FALSE(EncodeBulkAppendHeader(vertical::Schema, test::collection_id, 0));
    EXPECT_FALSE(EncodeBulkAppendHeader(vertical::Schema, test::collection_id, BulkAppendLimits::MaxRows + 1));
}

TEST(BulkCanonical, RejectsSmallSliceRetainingAnOversizedParent)
{
    auto allocated = arrow::AllocateBuffer(17LL << 20);
    ASSERT_TRUE(allocated.ok());
    std::shared_ptr<arrow::Buffer> parent = std::move(*allocated);
    parent->mutable_data()[0] = 'a';
    const auto offsets = arrow::Buffer::FromVector<int32_t>({0, 1});
    const auto values = arrow::SliceBuffer(parent, 0, 1);
    const auto array = arrow::MakeArray(arrow::ArrayData::Make(arrow::utf8(), 1, {nullptr, offsets, values}));
    const auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false)}), 1, {array});
    TCanonicalHasher hash;
    const auto result = UpdateFingerprintBatches({{{"key", FieldType::String, false}}}, {batch}, hash);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, "INPUT_TOO_LARGE");
}

} // namespace
} // namespace glamdb::detail
