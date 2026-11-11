#include "core/io/sha256.h"
#include "core/io/sha256_blocks.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <random>
#include <span>
#include <vector>

namespace NGlamDB::NIO {
namespace {

std::vector<std::byte> RandomBytes(std::size_t size, std::uint32_t seed)
{
    std::mt19937 random(seed);
    std::vector<std::byte> bytes(size);
    for (auto& byte : bytes) {
        byte = static_cast<std::byte>(random());
    }
    return bytes;
}

// The dispatched compression (hardware where available) equals the portable one.
TEST(TSha256BlocksTest, DispatchedCompressionMatchesPortable)
{
    const auto data = RandomBytes(64 * 1031, 7);
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
    const auto data = RandomBytes(4099, 11);
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
