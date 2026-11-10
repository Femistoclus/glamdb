#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <system_error>

namespace NGlamDB::NIO {

using TSha256Digest = std::array<std::byte, 32>;

class TSha256
{
public:
    static constexpr std::string_view KAlgorithm = "sha256";
    static constexpr std::uint16_t KVersion = 1;

    std::error_code Update(std::span<const std::byte> bytes) noexcept;
    TSha256Digest Finalize() const noexcept;
    std::uint64_t Size() const noexcept;

private:
    std::array<std::uint32_t, 8>
        State_{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    std::array<std::byte, 64> Buffer_{};
    std::uint64_t Size_ = 0;

    void Transform(std::span<const std::byte, 64> block) noexcept;
};

std::string HexDigest(const TSha256Digest& digest);

}
