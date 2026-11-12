#include "core/io/sha256.h"
#include "core/io/sha256_blocks.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <random>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace NGlamDB::NIO {
namespace {

using TCompressor = void (*)(std::uint32_t*, const unsigned char*, std::size_t) noexcept;

void CheckIndependentVectors(TCompressor compress)
{
    // Literal digests are independent SHA-256 vectors for repeated 'a' bytes.
    const std::pair<std::size_t, std::string_view> vectors[]{
        {0, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
        {3, "9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0"},
        {55, "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318"},
        {56, "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a"},
        {63, "7d3e74a05d7db15bce4ad9ec0658ea98e3f06eeecf16b4c6fff2da457ddc2f34"},
        {64, "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb"},
        {65, "635361c48bb9eab14198e76ea8ab7f1a41685d6ad62aa9146d301d4f17eb0ae0"},
        {120, "2f3d335432c70b580af0e8e1b3674a7c020d683aa5f73aaaedfdc55af904c21c"},
        {128, "6836cf13bac400e9105071cd6af47084dfacad4e5e302c94bfed24e013afb73e"},
    };
    for (const auto& [size, expected] : vectors) {
        SCOPED_TRACE(size);
        // Leave one leading byte so the block input is deliberately unaligned.
        const auto paddedSize = (size + 9 + 63) / 64 * 64;
        std::vector<unsigned char> bytes(paddedSize + 1);
        std::fill_n(bytes.begin() + 1, size, 'a');
        bytes[size + 1] = 0x80;
        const auto bits = static_cast<std::uint64_t>(size) * 8;
        for (std::size_t i = 0; i < 8; ++i) {
            bytes[paddedSize - i] = static_cast<unsigned char>(bits >> (8 * i));
        }
        std::array<std::uint32_t, 8>
            state{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
        compress(state.data(), bytes.data() + 1, paddedSize / 64);
        TSha256Digest digest{};
        for (std::size_t i = 0; i < state.size(); ++i) {
            for (std::size_t j = 0; j < 4; ++j) {
                digest[4 * i + j] = static_cast<std::byte>(state[i] >> (24 - 8 * j));
            }
        }
        EXPECT_EQ(HexDigest(digest), expected);
    }
}

TEST(TSha256BlocksTest, PortableCompressionMatchesIndependentVectors)
{
    CheckIndependentVectors(NSha256::PortableBlocks);
}

TEST(TSha256BlocksTest, AcceleratedCompressionMatchesIndependentVectors)
{
#if defined(__x86_64__) && (defined(__clang__) || defined(__GNUC__))
#ifdef GLAMDB_SHA256_X86
    if (!NSha256::HasX86Instructions()) {
        GTEST_SKIP() << "CPU does not support SHA, SSSE3 and SSE4.1";
    }
    CheckIndependentVectors(NSha256::X86Blocks);
#else
    FAIL() << "The shared compressor has no runtime-gated x86 SHA implementation";
#endif
#elif defined(GLAMDB_SHA256_ARM)
    if (!NSha256::HasArmInstructions()) {
        GTEST_SKIP() << "CPU does not support ARM SHA2";
    }
    CheckIndependentVectors(NSha256::ArmBlocks);
#else
    GTEST_SKIP() << "No accelerated compressor on this platform";
#endif
}

std::vector<std::byte> RandomBytes(std::size_t size, std::mt19937 random)
{
    std::vector<std::byte> bytes(size);
    for (auto& byte : bytes) {
        byte = static_cast<std::byte>(random());
    }
    return bytes;
}

TEST(TSha256BlocksTest, ForcedCompressorsHandleUnalignedDataAndArbitraryStates)
{
    std::vector<TCompressor> compressors{NSha256::PortableBlocks, NSha256::CompressBlocks};
#ifdef GLAMDB_SHA256_X86
    if (NSha256::HasX86Instructions()) {
        compressors.push_back(NSha256::X86Blocks);
    }
#elif defined(GLAMDB_SHA256_ARM)
    if (NSha256::HasArmInstructions()) {
        compressors.push_back(NSha256::ArmBlocks);
    }
#endif
    std::mt19937 random(29);
    const auto data = RandomBytes(64 * 255 + 15, random);
    for (std::size_t offset = 0; offset < 16; ++offset) {
        const auto* bytes = reinterpret_cast<const unsigned char*>(data.data()) + offset;
        for (const std::size_t blocks : {0, 1, 2, 3, 4, 15, 16, 17, 255}) {
            SCOPED_TRACE(offset);
            SCOPED_TRACE(blocks);
            std::array<std::uint32_t, 8> initial{};
            for (auto& word : initial) {
                word = random();
            }
            auto expected = initial;
            NSha256::PortableBlocks(expected.data(), bytes, blocks);
            for (const auto compress : compressors) {
                // A valid uint32_t state with deliberately non-SIMD alignment.
                alignas(16) std::array<std::uint32_t, 10> storage{};
                storage.front() = 0x12345678;
                storage.back() = 0x87654321;
                std::copy(initial.begin(), initial.end(), storage.begin() + 1);
                const auto first = blocks / 2;
                compress(storage.data() + 1, bytes, first);
                compress(storage.data() + 1, bytes + 64 * first, blocks - first);
                EXPECT_TRUE(std::equal(expected.begin(), expected.end(), storage.begin() + 1));
                EXPECT_EQ(storage.front(), 0x12345678U);
                EXPECT_EQ(storage.back(), 0x87654321U);
                compress(storage.data() + 1, nullptr, 0);
                EXPECT_TRUE(std::equal(expected.begin(), expected.end(), storage.begin() + 1));
            }
        }
    }
}

// The dispatched compression (hardware where available) equals the portable one.
TEST(TSha256BlocksTest, DispatchedCompressionMatchesPortable)
{
    const auto data = RandomBytes(std::size_t{64} * 1031, std::mt19937(7));
    for (const std::size_t blocks : {0, 1, 2, 3, 4, 5, 15, 16, 17, 255, 1031}) {
        std::array<std::uint32_t, 8>
            portable{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
        auto dispatched = portable;
        const auto* bytes = reinterpret_cast<const unsigned char*>(data.data());
        NSha256::PortableBlocks(portable.data(), bytes, blocks);
        NSha256::CompressBlocks(dispatched.data(), bytes, blocks);
        EXPECT_EQ(dispatched, portable) << blocks << " blocks";
    }
}

// Streaming with arbitrary chunk boundaries equals hashing the whole input.
TEST(TSha256BlocksTest, ChunkedUpdatesMatchOneShotDigests)
{
    const auto data = RandomBytes(4099, std::mt19937(11));
    std::mt19937 random(13);
    for (std::size_t size = 0; size <= data.size(); size += size < 300 ? 1 : 97) {
        const std::span whole(data.data(), size);
        TSha256 oneShot;
        ASSERT_FALSE(oneShot.Update(whole));
        TSha256 chunked;
        for (std::size_t offset = 0; offset < size;) {
            const auto count = std::min<std::size_t>(size - offset, random() % 150);
            ASSERT_FALSE(chunked.Update(whole.subspan(offset, count)));
            offset += count;
        }
        EXPECT_EQ(chunked.Finalize(), oneShot.Finalize()) << size;
    }
}

} // namespace
} // namespace NGlamDB::NIO
