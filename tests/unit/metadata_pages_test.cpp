#include "core/engine/metadata_pages.h"

#include <gtest/gtest.h>

namespace glamdb::detail {
namespace {

TMetadataPages::TListing Collections()
{
    TCollectionPage result;
    result.CreationHighWaterMark = 2;
    result.Collections.resize(2);
    result.Collections[0].collection_id = "first";
    result.Collections[1].collection_id = "second";
    return result;
}

TEST(TMetadataPagesTest, AuthenticatesTokensAndBindsEveryParameter)
{
    TMetadataPages pages;
    int loads = 0;
    const auto load = [&]() -> Result<TMetadataPages::TListing> {
        ++loads;
        return Collections();
    };
    const auto first = pages.Read({.PageSize = 1, .AuthorizationContext = "alice"}, "collections", load);
    ASSERT_TRUE(first);
    const auto token = std::get<TCollectionPage>(*first).NextPageToken;
    ASSERT_FALSE(token.empty());
    for (const auto& request :
        {TMetadataPageRequest{.PageSize = 2, .PageToken = token, .AuthorizationContext = "alice"},
            TMetadataPageRequest{.PageSize = 1, .PageToken = token, .AuthorizationContext = "bob"},
            TMetadataPageRequest{.PageSize = 1, .PageToken = std::string(64, '0'), .AuthorizationContext = "alice"}})
    {
        const auto changed = pages.Read(request, "collections", load);
        ASSERT_FALSE(changed);
        EXPECT_EQ(changed.error().code, "INVALID_CURSOR");
    }
    const TMetadataPageRequest next{.PageSize = 1, .PageToken = token, .AuthorizationContext = "alice"};
    ASSERT_FALSE(pages.Read(next, "files", load));
    TMetadataPages foreign;
    ASSERT_FALSE(foreign.Read(next, "collections", load));
    const auto second = pages.Read(next, "collections", load);
    const auto replay = pages.Read(next, "collections", load);
    ASSERT_TRUE(second);
    ASSERT_TRUE(replay);
    const auto& page = std::get<TCollectionPage>(*second);
    ASSERT_EQ(page.Collections.size(), 1U);
    EXPECT_EQ(page.Collections.front().collection_id, "second");
    EXPECT_TRUE(page.NextPageToken.empty());
    EXPECT_EQ(page.Collections, std::get<TCollectionPage>(*replay).Collections);
    EXPECT_EQ(loads, 1);
}

TEST(TMetadataPagesTest, ExpiresAbsolutelyAndReclaimsCapacity)
{
    TMetadataPages pages({.Lifetime = std::chrono::seconds(10), .MaxSessions = 1});
    const auto start = TMetadataPages::TClock::now();
    const auto load = []() -> Result<TMetadataPages::TListing> {
        return Collections();
    };
    const auto first = pages.Read({.PageSize = 1}, "collections", load, start);
    ASSERT_TRUE(first);
    const TMetadataPageRequest next{.PageSize = 1, .PageToken = std::get<TCollectionPage>(*first).NextPageToken};
    ASSERT_TRUE(pages.Read(next, "collections", load, start + std::chrono::seconds(9)));
    const auto full = pages.Read({.PageSize = 1}, "collections", load, start + std::chrono::seconds(9));
    ASSERT_FALSE(full);
    EXPECT_EQ(full.error().code, "RESOURCE_EXHAUSTED");
    const auto expired = pages.Read(next, "collections", load, start + std::chrono::seconds(10));
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error().code, "INVALID_CURSOR");
    ASSERT_TRUE(pages.Read({.PageSize = 1}, "collections", load, start + std::chrono::seconds(10)));
}

TEST(TMetadataPagesTest, ValidatesBeforeLoadingAndRecoversFromFailures)
{
    TMetadataPages pages;
    int loads = 0;
    const auto fail = [&]() -> Result<TMetadataPages::TListing> {
        ++loads;
        return std::unexpected(ErrorDetail{"IO_ERROR", "Read failed."});
    };
    for (const auto& request : {TMetadataPageRequest{.PageSize = 0},
             TMetadataPageRequest{.PageSize = -1},
             TMetadataPageRequest{.PageSize = 1025},
             TMetadataPageRequest{.AuthorizationContext = std::string(1025, 'x')}})
    {
        const auto invalid = pages.Read(request, "collections", fail);
        ASSERT_FALSE(invalid);
        EXPECT_EQ(invalid.error().code, "INVALID_ARGUMENT");
    }
    EXPECT_EQ(loads, 0);
    for (int index = 0; index < 40; ++index) {
        const auto failed = pages.Read({.PageSize = 1}, "collections", fail);
        ASSERT_FALSE(failed);
        EXPECT_EQ(failed.error().code, "IO_ERROR");
    }
    ASSERT_TRUE(pages.Read({.PageSize = 1}, "collections", []() -> Result<TMetadataPages::TListing> {
        return Collections();
    }));
    TMetadataPages tiny({.MaxBytes = 1});
    const auto oversized = tiny.Read({.PageSize = 1}, "collections", []() -> Result<TMetadataPages::TListing> {
        return Collections();
    });
    ASSERT_FALSE(oversized);
    EXPECT_EQ(oversized.error().code, "RESOURCE_EXHAUSTED");
}

TEST(TMetadataPagesTest, ReservesSessionBeforeLoadingWithoutHoldingTheRegistryLock)
{
    TMetadataPages pages({.MaxSessions = 1});
    const auto first = pages.Read({.PageSize = 1}, "collections", [&]() -> Result<TMetadataPages::TListing> {
        const auto nested = pages.Read({.PageSize = 1}, "collections", []() -> Result<TMetadataPages::TListing> {
            ADD_FAILURE() << "Capacity must be reserved before loading";
            return Collections();
        });
        EXPECT_FALSE(nested);
        if (!nested) {
            EXPECT_EQ(nested.error().code, "RESOURCE_EXHAUSTED");
        }
        return Collections();
    });
    ASSERT_TRUE(first);
    const TMetadataPageRequest next{.PageSize = 1, .PageToken = std::get<TCollectionPage>(*first).NextPageToken};
    pages.Clear();
    const auto stale = pages.Read(next, "collections", []() -> Result<TMetadataPages::TListing> {
        ADD_FAILURE() << "Unknown cursors must never load a fresh view";
        return Collections();
    });
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error().code, "INVALID_CURSOR");
}

TEST(TMetadataPagesTest, EmptyAndSinglePagesDoNotConsumeSessionCapacity)
{
    TMetadataPages pages({.MaxSessions = 1});
    for (int index = 0; index < 40; ++index) {
        const auto empty = pages.Read({}, "collections", []() -> Result<TMetadataPages::TListing> {
            return TCollectionPage{};
        });
        ASSERT_TRUE(empty);
        EXPECT_TRUE(std::get<TCollectionPage>(*empty).NextPageToken.empty());
        const auto single = pages.Read({}, "collections", []() -> Result<TMetadataPages::TListing> {
            return Collections();
        });
        ASSERT_TRUE(single);
        EXPECT_EQ(std::get<TCollectionPage>(*single).Collections.size(), 2U);
        EXPECT_TRUE(std::get<TCollectionPage>(*single).NextPageToken.empty());
    }
}

void CheckAdmissionInvalidation(TMetadataPages& pages)
{
    const auto admitted = pages.GetGeneration();
    pages.Clear();
    const auto result = pages.Read(
        {.PageSize = 1},
        "collections",
        []() -> Result<TMetadataPages::TListing> {
            ADD_FAILURE() << "Invalidated admission must not load or retain a listing";
            return Collections();
        },
        TMetadataPages::TClock::now(),
        admitted);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, "INVALID_CURSOR");
    ASSERT_TRUE(pages.Read({.PageSize = 1}, "collections", []() -> Result<TMetadataPages::TListing> {
        return Collections();
    }));
}

TEST(TMetadataPagesTest, RejectsAdmissionCapturedBeforeInvalidation)
{
    TMetadataPages pages({.MaxSessions = 1});
    CheckAdmissionInvalidation(pages);
}

} // namespace
} // namespace glamdb::detail
