#include "core/io/sha256.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace NGlamDB::NIO {
namespace {

std::span<const std::byte> Bytes(std::string_view value)
{
    return std::as_bytes(std::span(value.data(), value.size()));
}

TEST(Sha256, KnownAnswerVectors)
{
    const std::pair<std::string_view, std::string_view> vectors[]{
        {"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
        {"abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
        {"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"},
    };
    for (const auto& [input, expected] : vectors) {
        SCOPED_TRACE(input);
        TSha256 hash;
        ASSERT_FALSE(hash.Update(Bytes(input)));
        EXPECT_EQ(HexDigest(hash.Finalize()), expected);
        EXPECT_EQ(hash.Size(), input.size());
    }
}

TEST(Sha256, MillionAs)
{
    const std::string block(1000, 'a');
    TSha256 hash;
    for (int i = 0; i < 1000; ++i) {
        ASSERT_FALSE(hash.Update(Bytes(block)));
    }
    EXPECT_EQ(hash.Size(), 1000000);
    EXPECT_EQ(HexDigest(hash.Finalize()), "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

std::vector<std::byte> BinaryInput(std::size_t size)
{
    std::vector<std::byte> result(size);
    for (std::size_t i = 0; i < size; ++i) {
        result[i] = static_cast<std::byte>((i * 37 + 11) % 256);
    }
    return result;
}

TEST(Sha256, BinaryPaddingBoundaries)
{
    const std::pair<std::size_t, std::string_view> vectors[]{
        {1, "e7cf46a078fed4fafd0b5e3aff144802b853f8ae459a4f0c14add3314b7cc3a6"},
        {55, "2900465fcb533e05a158fd2b3be0e5e3b03740d83060aa3580e0d98a96bf2384"},
        {56, "31454ff48ef36af2f08fd511bdc37d9d5855ac23e992e5ff5445cb6b7674a674"},
        {63, "5f6401b96532c36de4e65beec0409b69b1d181864c8009b7a04f43e5d56350d1"},
        {64, "94eb5de4943613fd048dc93393ab06877405faa39c11f53e9386083339833e7e"},
        {65, "fc518669b6eb4b4dd91827ecacef86689c725bd5bab888fd3b26dbb196eec954"},
        {119, "b0dc41b1a384e2f1203f0351b38fbeaafceef577ce1191d5bfc25da39f721eae"},
        {120, "5df24dd802ac26132ce608dcb5f09841eef039ee0f152acf98d26d17fe4e88e6"},
        {127, "0fe729ff19257bd6fec853acc2ea355f6b34b58e6c0f684c3e188fcdfcd9baae"},
        {128, "0aedd4856f8eba0963627336ad5144a9a7dbe12498e6066f0165fc97d8ddee4c"},
        {129, "4f1757ae4bffbae86d775b831765b75af154d52f7deaa46dd378051a2d3ad57f"},
    };
    for (const auto& [size, expected] : vectors) {
        SCOPED_TRACE(size);
        TSha256 hash;
        ASSERT_FALSE(hash.Update(BinaryInput(size)));
        EXPECT_EQ(HexDigest(hash.Finalize()), expected);
    }
}

TEST(Sha256, ChunkBoundariesDoNotAffectDigest)
{
    const auto input = BinaryInput(1024);
    for (const std::size_t chunk : {1, 7, 55, 56, 63, 64, 65, 127, 256, 1024}) {
        SCOPED_TRACE(chunk);
        TSha256 hash;
        auto remaining = std::span(input);
        while (!remaining.empty()) {
            const auto count = std::min(chunk, remaining.size());
            ASSERT_FALSE(hash.Update(remaining.first(count)));
            ASSERT_FALSE(hash.Update({}));
            remaining = remaining.subspan(count);
        }
        EXPECT_EQ(hash.Size(), input.size());
        EXPECT_EQ(HexDigest(hash.Finalize()), "ffbad8f947474cfdd5b2bb22d7e0bf5ee8ba2b7af859d0c2bb28622db6a4be47");
    }
}

TEST(Sha256, FinalizeIsAnIndependentSnapshot)
{
    TSha256 hash;
    ASSERT_FALSE(hash.Update(Bytes("a")));
    const auto first = hash.Finalize();
    EXPECT_EQ(HexDigest(first), "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb");
    EXPECT_EQ(hash.Finalize(), first);
    EXPECT_EQ(hash.Size(), 1);
    auto copy = hash;
    ASSERT_FALSE(hash.Update(Bytes("bc")));
    EXPECT_EQ(HexDigest(hash.Finalize()), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(copy.Finalize(), first);
    ASSERT_FALSE(copy.Update(Bytes("bc")));
    EXPECT_EQ(copy.Finalize(), hash.Finalize());
}

TEST(Sha256, DigestEncodingIsFixedAndExplicit)
{
    TSha256Digest digest{};
    EXPECT_EQ(HexDigest(digest), std::string(64, '0'));
    digest.fill(std::byte{0xff});
    EXPECT_EQ(HexDigest(digest), std::string(64, 'f'));
    EXPECT_EQ(TSha256::KAlgorithm, "sha256");
    EXPECT_EQ(TSha256::KVersion, 1);
}

}
}
