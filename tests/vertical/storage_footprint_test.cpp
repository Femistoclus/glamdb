#include "tests/vertical/vertical_support.h"

#include <fstream>

namespace glamdb {
namespace {
TEST(StorageFootprint, CountsOwnedFilesDeduplicatesLinksAndRejectsSymlinks)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto opened = CreateStore({test::store_id, root.string()});
    ASSERT_TRUE(opened) << opened.error().message;
    auto engine = *opened;
    const auto before = engine->GetStorageFootprint();
    ASSERT_TRUE(before) << before.error().message;
    EXPECT_EQ(before->StoreID, test::store_id);
    EXPECT_GT(before->CatalogFileBytes, 0);
    EXPECT_EQ(before->CommittedDataFileBytes, 0);
    const auto orphan = root / "warehouse" / "orphan";
    std::ofstream(orphan) << "abc";
    std::filesystem::create_hard_link(orphan, root / "warehouse" / "same");
    const auto measured = engine->GetStorageFootprint();
    ASSERT_TRUE(measured) << measured.error().message;
    EXPECT_EQ(measured->DatabaseFileBytes, before->DatabaseFileBytes + 3);
    EXPECT_EQ(measured->HardlinkPaths, 1);
    EXPECT_EQ(measured->UnreferencedWarehouseFileBytes, 3);
    std::filesystem::create_symlink(orphan, root / "warehouse" / "redirect");
    const auto refused = engine->GetStorageFootprint();
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "PRECONDITION_FAILED");
}
} // namespace
} // namespace glamdb
