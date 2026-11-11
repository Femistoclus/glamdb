#include "core/io/sha256.h"

#include "core/io/sha256_blocks.h"

#include <algorithm>
#include <bit>
#include <limits>

namespace NGlamDB::NIO {
void TSha256::Transform(std::span<const std::byte, 64> block) noexcept
{
    NSha256::CompressBlocks(State_.data(), reinterpret_cast<const unsigned char*>(block.data()), 1);
}

std::error_code TSha256::Update(std::span<const std::byte> bytes) noexcept
{
    constexpr auto maxBytes = std::numeric_limits<std::uint64_t>::max() / 8;
    if (bytes.size() > maxBytes - Size_) {
        return std::make_error_code(std::errc::value_too_large);
    }
    if (bytes.empty()) {
        return {};
    }
    auto used = static_cast<std::size_t>(Size_ % Buffer_.size());
    Size_ += bytes.size();
    if (used != 0) {
        const auto count = std::min(Buffer_.size() - used, bytes.size());
        std::copy_n(bytes.begin(), count, Buffer_.begin() + used);
        bytes = bytes.subspan(count);
        used += count;
        if (used != Buffer_.size()) {
            return {};
        }
        Transform(Buffer_);
    }
    if (const auto blocks = bytes.size() / Buffer_.size(); blocks != 0) {
        NSha256::CompressBlocks(State_.data(), reinterpret_cast<const unsigned char*>(bytes.data()), blocks);
        bytes = bytes.subspan(blocks * Buffer_.size());
    }
    std::copy(bytes.begin(), bytes.end(), Buffer_.begin());
    return {};
}

TSha256Digest TSha256::Finalize() const noexcept
{
    auto copy = *this;
    const auto used = static_cast<std::size_t>(Size_ % Buffer_.size());
    copy.Buffer_[used] = std::byte{0x80};
    std::fill(copy.Buffer_.begin() + used + 1, copy.Buffer_.end(), std::byte{});
    if (used >= 56) {
        copy.Transform(copy.Buffer_);
        copy.Buffer_.fill(std::byte{});
    }
    const auto bits = Size_ * 8;
    for (std::size_t i = 0; i < 8; ++i) {
        copy.Buffer_[63 - i] = static_cast<std::byte>(bits >> (i * 8));
    }
    copy.Transform(copy.Buffer_);
    TSha256Digest result{};
    for (std::size_t i = 0; i < copy.State_.size(); ++i) {
        for (std::size_t j = 0; j < 4; ++j) {
            result[4 * i + j] = static_cast<std::byte>(copy.State_[i] >> (24 - 8 * j));
        }
    }
    return result;
}

std::string HexDigest(const TSha256Digest& digest)
{
    constexpr std::string_view digits = "0123456789abcdef";
    std::string result(digest.size() * 2, '0');
    for (std::size_t i = 0; i < digest.size(); ++i) {
        const auto value = std::to_integer<unsigned>(digest[i]);
        result[2 * i] = digits[value >> 4];
        result[2 * i + 1] = digits[value & 15];
    }
    return result;
}

std::uint64_t TSha256::Size() const noexcept
{
    return Size_;
}

}
