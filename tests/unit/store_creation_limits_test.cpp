// Invalid catalog limits are rejected before CreateStore touches the
// filesystem, so the caller-retained root and store UUID stay usable.

#include "storage/engine.h"

#include "core/engine/engine_factory.h"
#include "tests/support/fake_storage.h"
#include "tests/support/temp_dir.h"

#include <filesystem>
#include <functional>
#include <string>

#include <gtest/gtest.h>

namespace glamdb {
namespace {

struct InvalidLimitsCase
{
    std::string name;
    std::function<void(Limits&)> mutate;
};

class StoreCreationLimits : public testing::TestWithParam<InvalidLimitsCase>
{ };

TEST_P(StoreCreationLimits, InvalidCatalogLimitsLeaveTheRootReusable)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    const auto backend = std::make_shared<test::FakeStorage>(root);

    CreateStoreRequest invalid{test::store_id, root.string()};
    GetParam().mutate(invalid.limits);
    const auto rejected = detail::EngineFactory::Create(invalid, backend);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, "INVALID_ARGUMENT");

    // Argument validation must not consume the caller-retained root/UUID.
    EXPECT_FALSE(std::filesystem::exists(root)) << "rejected configuration left store files behind";
    const auto reopened = detail::EngineFactory::Open({root.string(), test::store_id}, backend);
    EXPECT_FALSE(reopened);

    const auto created = detail::EngineFactory::Create({test::store_id, root.string()}, backend);
    ASSERT_TRUE(created) << created.error().code << ": " << created.error().message;
    EXPECT_EQ((*created)
                  ->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}})
                  .disposition,
        CommitDisposition::Committed);
}

INSTANTIATE_TEST_SUITE_P(OutOfRangeCatalogLimits,
    StoreCreationLimits,
    testing::Values(InvalidLimitsCase{"NoOperations",
                        [](Limits& limits) {
                            limits.max_operations = 0;
                        }},
        InvalidLimitsCase{"TooManyCollections",
            [](Limits& limits) {
                limits.max_collections = 65;
            }},
        InvalidLimitsCase{"CatalogTooLarge",
            [](Limits& limits) {
                limits.max_catalog_bytes = (1LL << 40) + 1;
            }}),
    [](const testing::TestParamInfo<InvalidLimitsCase>& info) {
        return info.param.name;
    });

} // namespace
} // namespace glamdb
