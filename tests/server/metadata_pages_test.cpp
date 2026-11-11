#include "apps/server/metadata.h"

#include <gtest/gtest.h>

#include <array>
#include <thread>

namespace glamdb::NServer {
namespace {
using namespace NTransport;
TJson Inventory(SnapshotSelector selector)
{
    (void)selector;
    return {{"identity", {{"collection_id", "collection"}, {"current_snapshot_id", "9007199254740993"}}},
        {"snapshot_id", "9007199254740993"},
        {"items", {{{"record_count", "9007199254740993"}}, {{"record_count", "1"}}, {{"record_count", "2"}}}}};
}
void FailureCode(const std::function<void()>& call, const std::string& code)
{
    try {
        call();
        FAIL() << "expected " << code;
    } catch (const TFailure& failure) {
        EXPECT_EQ(failure.Detail.code, code);
    }
}
} // namespace

TEST(MetadataPages, RetainsOneResolvedViewAndBindsAllParameters)
{
    TMetadata pages;
    int reads = 0;
    const auto read = [&](SnapshotSelector selector) {
        ++reads;
        return Inventory(selector);
    };
    const auto first = pages.Page("database", "collection", "files", {{"page_size", "1"}}, read);
    const std::map<std::string, std::string> query{{"page_size", "1"},
        {"cursor", first.at("next_cursor").get<std::string>()}};
    const auto second = pages.Page("database", "collection", "files", query, read);
    EXPECT_EQ(reads, 1);
    EXPECT_EQ(second.at("identity"), first.at("identity"));
    EXPECT_EQ(second.at("snapshot_id"), "9007199254740993");
    EXPECT_EQ(second.at("items").at(0).at("record_count"), "1");
    EXPECT_EQ(pages.Page("database", "collection", "files", query, read), second);
    auto changed = query;
    changed["page_size"] = "2";
    FailureCode(
        [&] {
            pages.Page("database", "collection", "files", changed, read);
        },
        "INVALID_ARGUMENT");
    changed = query;
    changed["snapshot_id"] = "initial";
    FailureCode(
        [&] {
            pages.Page("database", "collection", "files", changed, read);
        },
        "INVALID_ARGUMENT");
    FailureCode(
        [&] {
            pages.Page("database", "collection", "snapshots", query, read);
        },
        "INVALID_ARGUMENT");
    FailureCode(
        [&] {
            pages.Page("other", "collection", "files", query, read);
        },
        "NOT_FOUND");
    FailureCode(
        [&] {
            pages.Page("database", "other", "files", query, read);
        },
        "NOT_FOUND");
    changed = query;
    changed["cursor"] += "f";
    FailureCode(
        [&] {
            pages.Page("database", "collection", "files", changed, read);
        },
        "NOT_FOUND");
    const auto third = pages.Page("database",
        "collection",
        "files",
        {{"page_size", "1"}, {"cursor", second.at("next_cursor").get<std::string>()}},
        read);
    EXPECT_TRUE(third.at("next_cursor").is_null());
    EXPECT_EQ(reads, 1);
    pages.Clear("database");
    FailureCode(
        [&] {
            pages.Page("database", "collection", "files", query, read);
        },
        "NOT_FOUND");
}

TEST(MetadataPages, ExpiresAndReleasesDatabaseCapacity)
{
    TMetadata pages;
    auto first = pages.Page("database", "collection", "snapshots", {{"page_size", "1"}}, Inventory);
    for (size_t i = 1; i < 8; ++i) {
        pages.Page("database", "collection", "snapshots", {{"page_size", "1"}}, Inventory);
    }
    FailureCode(
        [&] {
            pages.Page("database", "collection", "snapshots", {{"page_size", "1"}}, Inventory);
        },
        "RESOURCE_EXHAUSTED");
    pages.Clear("database");
    FailureCode(
        [&] {
            pages.Page("database",
                "collection",
                "snapshots",
                {{"page_size", "1"}, {"cursor", first.at("next_cursor").get<std::string>()}},
                Inventory);
        },
        "NOT_FOUND");
    EXPECT_FALSE(
        pages.Page("database", "collection", "snapshots", {{"page_size", "1"}}, Inventory).at("next_cursor").is_null());
    for (const auto& durations : std::array{std::pair{std::chrono::milliseconds(10), std::chrono::milliseconds(1000)},
             std::pair{std::chrono::milliseconds(1000), std::chrono::milliseconds(10)}})
    {
        TMetadata expiring(durations.first, durations.second);
        const auto opened = expiring.Page("database", "collection", "files", {{"page_size", "1"}}, Inventory);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        FailureCode(
            [&] {
                expiring.Page("database",
                    "collection",
                    "files",
                    {{"page_size", "1"}, {"cursor", opened.at("next_cursor").get<std::string>()}},
                    Inventory);
            },
            "NOT_FOUND");
        EXPECT_FALSE(expiring.Page("database", "collection", "files", {{"page_size", "1"}}, Inventory)
                         .at("next_cursor")
                         .is_null());
    }
}

TEST(MetadataPages, ValidatesSelectorsBoundsAndSinglePagesWithoutRetainingCapacity)
{
    TMetadata pages;
    for (const auto& query : std::vector<std::map<std::string, std::string>>{{{"page_size", "0"}},
             {{"page_size", "1001"}},
             {{"page_size", "01"}},
             {{"snapshot_id", "00"}},
             {{"snapshot_id", "9223372036854775808"}},
             {{"unknown", "1"}}})
    {
        FailureCode(
            [&] {
                pages.Page("database", "collection", "files", query, Inventory);
            },
            "INVALID_ARGUMENT");
    }
    for (size_t i = 0; i < 100; ++i) {
        const auto result = pages.Page("database", "collection", "files", {}, Inventory);
        EXPECT_EQ(result.at("items").size(), 3);
        EXPECT_TRUE(result.at("next_cursor").is_null());
    }
}

TEST(MetadataPages, CompletedListingsAllowRepeatedRefreshesAndRecentReplay)
{
    TMetadata pages;
    for (size_t refresh = 0; refresh < 12; ++refresh) {
        for (const auto& kind : {"snapshots", "files"}) {
            const auto first = pages.Page("database", "collection", kind, {{"page_size", "2"}}, Inventory);
            const std::map<std::string, std::string> query{{"page_size", "2"},
                {"cursor", first.at("next_cursor").get<std::string>()}};
            const auto last = pages.Page("database", "collection", kind, query, Inventory);
            ASSERT_TRUE(last.at("next_cursor").is_null());
            EXPECT_EQ(last.at("items").at(0).at("record_count"), "2");
            EXPECT_EQ(pages.Page("database", "collection", kind, query, Inventory), last);
        }
    }
}

TEST(MetadataPages, DatabasePressureEvictsOnlyTheOldestCompletedInventory)
{
    TMetadata pages;
    const auto active = pages.Page("database", "collection", "files", {{"page_size", "2"}}, Inventory);
    std::vector<std::map<std::string, std::string>> queries;
    for (size_t i = 0; i < 7; ++i) {
        const auto first = pages.Page("database", "collection", "files", {{"page_size", "2"}}, Inventory);
        queries.push_back({{"page_size", "2"}, {"cursor", first.at("next_cursor").get<std::string>()}});
        EXPECT_TRUE(
            pages.Page("database", "collection", "files", queries.back(), Inventory).at("next_cursor").is_null());
    }
    EXPECT_FALSE(
        pages.Page("database", "collection", "files", {{"page_size", "2"}}, Inventory).at("next_cursor").is_null());
    FailureCode(
        [&] {
            pages.Page("database", "collection", "files", queries.front(), Inventory);
        },
        "NOT_FOUND");
    EXPECT_TRUE(pages.Page("database", "collection", "files", queries.back(), Inventory).at("next_cursor").is_null());
    EXPECT_TRUE(pages
                    .Page("database",
                        "collection",
                        "files",
                        {{"page_size", "2"}, {"cursor", active.at("next_cursor").get<std::string>()}},
                        Inventory)
                    .at("next_cursor")
                    .is_null());
}

TEST(MetadataPages, GlobalPressureReclaimsCompletedInventoriesAcrossDatabases)
{
    TMetadata pages;
    const auto active = pages.Page("active", "collection", "files", {{"page_size", "2"}}, Inventory);
    for (size_t i = 0; i < 63; ++i) {
        const auto database = std::to_string(i);
        const auto first = pages.Page(database, "collection", "files", {{"page_size", "2"}}, Inventory);
        EXPECT_TRUE(pages
                        .Page(database,
                            "collection",
                            "files",
                            {{"page_size", "2"}, {"cursor", first.at("next_cursor").get<std::string>()}},
                            Inventory)
                        .at("next_cursor")
                        .is_null());
    }
    EXPECT_FALSE(pages.Page("new", "collection", "files", {{"page_size", "2"}}, Inventory).at("next_cursor").is_null());
    EXPECT_TRUE(pages
                    .Page("active",
                        "collection",
                        "files",
                        {{"page_size", "2"}, {"cursor", active.at("next_cursor").get<std::string>()}},
                        Inventory)
                    .at("next_cursor")
                    .is_null());
}

TEST(MetadataPages, RetainedBytePressureReclaimsCompletedInventoryStorage)
{
    TMetadata pages;
    const auto read = [](SnapshotSelector selector) {
        auto inventory = Inventory(selector);
        inventory["items"][0]["padding"] = std::string(size_t{14} * 1024 * 1024, 'x');
        return inventory;
    };
    for (size_t i = 0; i < 6; ++i) {
        const auto first = pages.Page("database", "collection", "files", {{"page_size", "2"}}, read);
        EXPECT_TRUE(pages
                        .Page("database",
                            "collection",
                            "files",
                            {{"page_size", "2"}, {"cursor", first.at("next_cursor").get<std::string>()}},
                            read)
                        .at("next_cursor")
                        .is_null());
    }
}
} // namespace glamdb::NServer
