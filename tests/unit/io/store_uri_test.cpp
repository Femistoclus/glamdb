#include "core/io/store_uri.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>
#include <utility>

namespace NGlamDB::NIO {
namespace {

constexpr std::string_view KStore = "01234567-89ab-cdef-0123-456789abcdef";
constexpr std::string_view KCollection = "89abcdef-0123-4567-89ab-cdef01234567";
constexpr std::string_view KFileUuid = "fedcba98-7654-3210-fedc-ba9876543210";

std::string Table()
{
    return "store://" + std::string(KStore) + "/warehouse/" + std::string(KCollection);
}

std::string File(std::string_view directory, std::string_view leaf)
{
    return Table() + "/" + std::string(directory) + "/" + std::string(leaf);
}

TEST(StoreUri, CanonicalUuidGrammar)
{
    EXPECT_TRUE(IsCanonicalUuid(KStore));
    EXPECT_TRUE(IsCanonicalUuid("00000000-0000-0000-0000-000000000000"));
    EXPECT_TRUE(IsCanonicalUuid("ffffffff-ffff-ffff-ffff-ffffffffffff"));
    for (const auto invalid : {"",
             "0123456789abcdef0123456789abcdef",
             "01234567-89AB-cdef-0123-456789abcdef",
             "01234567_89ab-cdef-0123-456789abcdef",
             "01234567-89ab-cdef-0123-456789abcdeg",
             "01234567-89ab-cdef-0123-456789abcde",
             "01234567-89ab-cdef-0123-456789abcdef0"})
    {
        EXPECT_FALSE(IsCanonicalUuid(invalid)) << invalid;
    }
}

TEST(StoreUri, SeparatesTableAndFileLocations)
{
    const auto result = TStoreUri::ParseTable(Table(), KStore);
    ASSERT_TRUE(result);
    const auto& uri = result.Value();
    EXPECT_EQ(uri.String(), Table());
    EXPECT_EQ(uri.StoreId(), KStore);
    EXPECT_EQ(uri.CollectionId(), KCollection);
    EXPECT_EQ(uri.RelativePath(), "warehouse/" + std::string(KCollection));
    EXPECT_TRUE(uri.IsTable());
    EXPECT_EQ(uri.Kind(), EFileKind::Other);
    EXPECT_TRUE(uri.Directory().empty());
    EXPECT_TRUE(uri.Leaf().empty());
    EXPECT_FALSE(TStoreUri::Parse(Table(), KStore));
    EXPECT_FALSE(TStoreUri::ParseTable(Table() + "/", KStore));
    EXPECT_FALSE(TStoreUri::ParseTable(File("data", "a.parquet"), KStore));
}

TEST(StoreUri, RecognizesPinnedWriterFileNames)
{
    const std::pair<std::string, EFileKind> cases[]{
        {"data/a.parquet", EFileKind::Data},
        {"data/ключ.parquet", EFileKind::Data},
        {"metadata/" + std::string(KFileUuid) + "-m0.avro", EFileKind::Manifest},
        {"metadata/" + std::string(KFileUuid) + "-m123.avro", EFileKind::Manifest},
        {"metadata/snap-0-0-" + std::string(KFileUuid) + ".avro", EFileKind::ManifestList},
        {"metadata/snap-9223372036854775807-17-" + std::string(KFileUuid) + ".avro", EFileKind::ManifestList},
        {"metadata/snap--9223372036854775808-1-" + std::string(KFileUuid) + ".avro", EFileKind::ManifestList},
        {"metadata/00000-" + std::string(KFileUuid) + ".metadata.json", EFileKind::Metadata},
        {"metadata/100000-" + std::string(KFileUuid) + ".metadata.json", EFileKind::Metadata},
    };
    for (const auto& [suffix, expected_kind] : cases) {
        SCOPED_TRACE(suffix);
        const auto text = Table() + "/" + suffix;
        const auto result = TStoreUri::Parse(text, KStore);
        ASSERT_TRUE(result);
        const auto& uri = result.Value();
        EXPECT_EQ(uri.String(), text);
        EXPECT_EQ(uri.StoreId(), KStore);
        EXPECT_EQ(uri.CollectionId(), KCollection);
        EXPECT_EQ(uri.Kind(), expected_kind);
        EXPECT_FALSE(uri.IsTable());
        EXPECT_EQ(uri.Directory(), suffix.substr(0, suffix.find('/')));
        EXPECT_EQ(uri.Leaf(), suffix.substr(suffix.find('/') + 1));
        EXPECT_EQ(uri.RelativePath(), "warehouse/" + std::string(KCollection) + "/" + suffix);
    }
}

TEST(StoreUri, RejectsForeignStoreAndMalformedExpectedIdentity)
{
    EXPECT_EQ(TStoreUri::Parse(File("data", "a.parquet"), KCollection).Error(), std::errc::permission_denied);
    EXPECT_EQ(TStoreUri::ParseTable(Table(), KCollection).Error(), std::errc::permission_denied);
    EXPECT_EQ(TStoreUri::Parse(File("data", "a.parquet"), "").Error(), std::errc::invalid_argument);
}

TEST(StoreUri, RejectsMalformedOrEscapingLocations)
{
    const auto valid = File("data", "a.parquet");
    const std::string cases[]{
        "",
        "/warehouse/a.parquet",
        "file://" + valid.substr(8),
        "STORE://" + valid.substr(8),
        "store:///" + valid.substr(8),
        "store://" + std::string(KStore) + "/other/" + std::string(KCollection) + "/data/a.parquet",
        "store://" + std::string(KStore) + "/warehouse//data/a.parquet",
        "store://" + std::string(KStore) + "/warehouse/../data/a.parquet",
        Table() + "data/a.parquet",
        Table() + "//a.parquet",
        Table() + "/data",
        Table() + "/data/",
        Table() + "/data/.",
        Table() + "/data/..",
        File("data", "../a.parquet"),
        File("data", "a/b.parquet"),
        File("data", "a.parquet/"),
        File("data", "a\\b.parquet"),
        File("data", "%2e%2e.parquet"),
        File("data", "a%2fb.parquet"),
        File("data", "a?b.parquet"),
        File("data", "a#b.parquet"),
        valid + std::string(1, '\0'),
        File("data", std::string("a\0b.parquet", 11)),
    };
    for (const auto& value : cases) {
        EXPECT_EQ(TStoreUri::Parse(value, KStore).Error(), std::errc::invalid_argument) << value;
    }
}

TEST(StoreUri, RejectsUnknownOrMalformedFileKinds)
{
    const std::string cases[]{
        "data/.parquet",
        "data/a.PARQUET",
        "data/a.parquet.tmp",
        "other/a.parquet",
        "metadata/a.parquet",
        "metadata/a.avro",
        "metadata/0000-" + std::string(KFileUuid) + ".metadata.json",
        "metadata/0000x-" + std::string(KFileUuid) + ".metadata.json",
        "metadata/00000-invalid.metadata.json",
        "metadata/00000-" + std::string(KFileUuid) + ".metadata.json.gz",
        "metadata/" + std::string(KFileUuid) + "-m.avro",
        "metadata/" + std::string(KFileUuid) + "-m-1.avro",
        "metadata/" + std::string(KFileUuid) + "-m1x.avro",
        "metadata/snap-1--" + std::string(KFileUuid) + ".avro",
        "metadata/snap-1--1-" + std::string(KFileUuid) + ".avro",
        "metadata/snap-1-1-invalid.avro",
        "metadata/snap--1-" + std::string(KFileUuid) + ".avro",
        "metadata/snap-+1-1-" + std::string(KFileUuid) + ".avro",
        "metadata/snap-9223372036854775808-1-" + std::string(KFileUuid) + ".avro",
        "metadata/snap--9223372036854775809-1-" + std::string(KFileUuid) + ".avro",
    };
    for (const auto& suffix : cases) {
        EXPECT_EQ(TStoreUri::Parse(Table() + "/" + suffix, KStore).Error(), std::errc::invalid_argument) << suffix;
    }
}

TEST(StoreUri, OwnsInputAcrossCopiesAndMoves)
{
    auto text = File("data", "a.parquet");
    const auto result = TStoreUri::Parse(text, KStore);
    ASSERT_TRUE(result);
    auto copy = result.Value();
    auto moved = std::move(copy);
    text.clear();
    EXPECT_EQ(result.Value().String(), File("data", "a.parquet"));
    EXPECT_EQ(moved.String(), result.Value().String());
    EXPECT_EQ(moved.StoreId(), KStore);
    EXPECT_EQ(moved.CollectionId(), KCollection);
    EXPECT_EQ(moved.Directory(), "data");
    EXPECT_EQ(moved.Leaf(), "a.parquet");
}

}
}
