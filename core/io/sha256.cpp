#include "core/io/sha256.h"

#include <algorithm>
#include <bit>
#include <limits>

namespace NGlamDB::NIO {
namespace {

constexpr std::array<std::uint32_t, 64> KRoundConstants{0x428a2f98,
    0x71374491,
    0xb5c0fbcf,
    0xe9b5dba5,
    0x3956c25b,
    0x59f111f1,
    0x923f82a4,
    0xab1c5ed5,
    0xd807aa98,
    0x12835b01,
    0x243185be,
    0x550c7dc3,
    0x72be5d74,
    0x80deb1fe,
    0x9bdc06a7,
    0xc19bf174,
    0xe49b69c1,
    0xefbe4786,
    0x0fc19dc6,
    0x240ca1cc,
    0x2de92c6f,
    0x4a7484aa,
    0x5cb0a9dc,
    0x76f988da,
    0x983e5152,
    0xa831c66d,
    0xb00327c8,
    0xbf597fc7,
    0xc6e00bf3,
    0xd5a79147,
    0x06ca6351,
    0x14292967,
    0x27b70a85,
    0x2e1b2138,
    0x4d2c6dfc,
    0x53380d13,
    0x650a7354,
    0x766a0abb,
    0x81c2c92e,
    0x92722c85,
    0xa2bfe8a1,
    0xa81a664b,
    0xc24b8b70,
    0xc76c51a3,
    0xd192e819,
    0xd6990624,
    0xf40e3585,
    0x106aa070,
    0x19a4c116,
    0x1e376c08,
    0x2748774c,
    0x34b0bcb5,
    0x391c0cb3,
    0x4ed8aa4a,
    0x5b9cca4f,
    0x682e6ff3,
    0x748f82ee,
    0x78a5636f,
    0x84c87814,
    0x8cc70208,
    0x90befffa,
    0xa4506ceb,
    0xbef9a3f7,
    0xc67178f2};

}

void TSha256::Transform(std::span<const std::byte, 64> block) noexcept
{
    std::array<std::uint32_t, 64> words{};
    for (std::size_t i = 0; i < 16; ++i) {
        for (std::size_t j = 0; j < 4; ++j) {
            words[i] = (words[i] << 8) | std::to_integer<std::uint32_t>(block[4 * i + j]);
        }
    }
    for (std::size_t i = 16; i < words.size(); ++i) {
        const auto x = words[i - 15];
        const auto y = words[i - 2];
        const auto s0 = std::rotr(x, 7) ^ std::rotr(x, 18) ^ (x >> 3);
        const auto s1 = std::rotr(y, 17) ^ std::rotr(y, 19) ^ (y >> 10);
        words[i] = words[i - 16] + s0 + words[i - 7] + s1;
    }
    auto [a, b, c, d, e, f, g, h] = State_;
    for (std::size_t i = 0; i < words.size(); ++i) {
        const auto s1 = std::rotr(e, 6) ^ std::rotr(e, 11) ^ std::rotr(e, 25);
        const auto choose = (e & f) ^ (~e & g);
        const auto t1 = h + s1 + choose + KRoundConstants[i] + words[i];
        const auto s0 = std::rotr(a, 2) ^ std::rotr(a, 13) ^ std::rotr(a, 22);
        const auto majority = (a & b) ^ (a & c) ^ (b & c);
        const auto t2 = s0 + majority;
        h = g;
        g = f;
        f = e;
        e = d + t1;
        d = c;
        c = b;
        b = a;
        a = t1 + t2;
    }
    const std::array next{a, b, c, d, e, f, g, h};
    for (std::size_t i = 0; i < State_.size(); ++i) {
        State_[i] += next[i];
    }
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
    while (bytes.size() >= Buffer_.size()) {
        Transform(bytes.first<64>());
        bytes = bytes.subspan(Buffer_.size());
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
