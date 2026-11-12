#pragma once

// SHA-256 compression over whole 64-byte blocks, shared by the payload
// fingerprint and the durable-file receipts. Uses x86 SHA or ARMv8 SHA2
// instructions when the CPU reports them and a portable implementation otherwise.

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>

#if defined(__x86_64__) && (defined(__clang__) || defined(__GNUC__))
#include <cpuid.h>
#include <immintrin.h>
#define GLAMDB_SHA256_X86 1
#endif

#if defined(__aarch64__) && (defined(__clang__) || defined(__GNUC__))
#include <arm_neon.h>
#if defined(__linux__)
#include <sys/auxv.h>
#endif
#define GLAMDB_SHA256_ARM 1
#endif

namespace NGlamDB::NIO::NSha256 {

inline constexpr std::array<std::uint32_t, 64> RoundConstants{0x428a2f98,
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

inline void PortableBlocks(std::uint32_t* state, const unsigned char* data, std::size_t blocks) noexcept
{
    for (; blocks != 0; --blocks, data += 64) {
        std::uint32_t words[64];
        for (std::size_t i = 0; i < 16; ++i) {
            words[i] = (std::uint32_t{data[4 * i]} << 24) | (std::uint32_t{data[4 * i + 1]} << 16) |
                (std::uint32_t{data[4 * i + 2]} << 8) | std::uint32_t{data[4 * i + 3]};
        }
        for (std::size_t i = 16; i < 64; ++i) {
            const auto a = words[i - 15];
            const auto b = words[i - 2];
            words[i] = words[i - 16] + (std::rotr(a, 7) ^ std::rotr(a, 18) ^ (a >> 3)) + words[i - 7] +
                (std::rotr(b, 17) ^ std::rotr(b, 19) ^ (b >> 10));
        }
        auto a = state[0], b = state[1], c = state[2], d = state[3];
        auto e = state[4], f = state[5], g = state[6], h = state[7];
        for (std::size_t i = 0; i < 64; ++i) {
            const auto temp1 = h + (std::rotr(e, 6) ^ std::rotr(e, 11) ^ std::rotr(e, 25)) + ((e & f) ^ (~e & g)) +
                RoundConstants[i] + words[i];
            const auto temp2 = (std::rotr(a, 2) ^ std::rotr(a, 13) ^ std::rotr(a, 22)) + ((a & b) ^ (a & c) ^ (b & c));
            h = g;
            g = f;
            f = e;
            e = d + temp1;
            d = c;
            c = b;
            b = a;
            a = temp1 + temp2;
        }
        state[0] += a;
        state[1] += b;
        state[2] += c;
        state[3] += d;
        state[4] += e;
        state[5] += f;
        state[6] += g;
        state[7] += h;
    }
}

#ifdef GLAMDB_SHA256_X86
// Based on Jeffrey Walton's public-domain SHA-Intrinsics/sha256-x86.c,
// derived from Intel's and Sean Gulley's SHA extensions examples.
// https://github.com/noloader/SHA-Intrinsics/blob/master/sha256-x86.c
__attribute__((target("sha,ssse3,sse4.1"))) inline void
X86Blocks(std::uint32_t* state, const unsigned char* data, std::size_t blocks) noexcept
{
    // SHA256RNDS2 uses ABEF/CDGH rather than the ordinary ABCD/EFGH layout.
    auto abcd = _mm_shuffle_epi32(_mm_loadu_si128(reinterpret_cast<const __m128i*>(state)), 0xb1);
    auto cdgh = _mm_shuffle_epi32(_mm_loadu_si128(reinterpret_cast<const __m128i*>(state + 4)), 0x1b);
    auto abef = _mm_alignr_epi8(abcd, cdgh, 8);
    cdgh = _mm_blend_epi16(cdgh, abcd, 0xf0);
    const auto byteOrder = _mm_set_epi64x(0x0c0d0e0f08090a0b, 0x0405060700010203);
    for (; blocks != 0; --blocks, data += 64) {
        const auto savedAbef = abef;
        const auto savedCdgh = cdgh;
        __m128i message[4];
        for (std::size_t i = 0; i < 4; ++i) {
            message[i] = _mm_shuffle_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 16 * i)), byteOrder);
        }
        for (std::size_t group = 0; group < 16; ++group) {
            auto& words = message[group & 3];
            if (group >= 4) {
                words = _mm_sha256msg1_epu32(words, message[(group + 1) & 3]);
                words = _mm_add_epi32(words, _mm_alignr_epi8(message[(group + 3) & 3], message[(group + 2) & 3], 4));
                words = _mm_sha256msg2_epu32(words, message[(group + 3) & 3]);
            }
            auto schedule = _mm_add_epi32(words,
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(RoundConstants.data() + 4 * group)));
            cdgh = _mm_sha256rnds2_epu32(cdgh, abef, schedule);
            schedule = _mm_shuffle_epi32(schedule, 0x0e);
            abef = _mm_sha256rnds2_epu32(abef, cdgh, schedule);
        }
        abef = _mm_add_epi32(abef, savedAbef);
        cdgh = _mm_add_epi32(cdgh, savedCdgh);
    }
    abcd = _mm_shuffle_epi32(abef, 0x1b);
    cdgh = _mm_shuffle_epi32(cdgh, 0xb1);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(state), _mm_blend_epi16(abcd, cdgh, 0xf0));
    _mm_storeu_si128(reinterpret_cast<__m128i*>(state + 4), _mm_alignr_epi8(cdgh, abcd, 8));
}

inline bool HasX86Instructions() noexcept
{
    // Check every extension used by X86Blocks. CPUID is executed only once;
    // no global ISA flags or AVX/OSXSAVE state are needed for these XMM operations.
    static const bool supported = [] {
        unsigned eax = 0, ebx = 0, ecx = 0, edx = 0;
        constexpr unsigned required = bit_SSSE3 | bit_SSE4_1;
        if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx) || (ecx & required) != required) {
            return false;
        }
        return __get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx) && (ebx & bit_SHA) != 0;
    }();
    return supported;
}
#endif

#ifdef GLAMDB_SHA256_ARM
#if defined(__clang__)
__attribute__((target("sha2")))
#else
__attribute__((target("+crypto")))
#endif
inline void
ArmBlocks(std::uint32_t* state, const unsigned char* data, std::size_t blocks) noexcept
{
    auto abcd = vld1q_u32(state);
    auto efgh = vld1q_u32(state + 4);
    for (; blocks != 0; --blocks, data += 64) {
        const auto savedAbcd = abcd;
        const auto savedEfgh = efgh;
        uint32x4_t message[4];
        for (std::size_t i = 0; i < 4; ++i) {
            message[i] = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(data + 16 * i)));
        }
        for (std::size_t group = 0; group < 16; ++group) {
            auto& words = message[group & 3];
            if (group >= 4) {
                words = vsha256su1q_u32(vsha256su0q_u32(words, message[(group + 1) & 3]),
                    message[(group + 2) & 3],
                    message[(group + 3) & 3]);
            }
            const auto schedule = vaddq_u32(words, vld1q_u32(RoundConstants.data() + 4 * group));
            const auto previous = abcd;
            abcd = vsha256hq_u32(abcd, efgh, schedule);
            efgh = vsha256h2q_u32(efgh, previous, schedule);
        }
        abcd = vaddq_u32(abcd, savedAbcd);
        efgh = vaddq_u32(efgh, savedEfgh);
    }
    vst1q_u32(state, abcd);
    vst1q_u32(state + 4, efgh);
}

inline bool HasArmInstructions() noexcept
{
#if defined(__APPLE__)
    return true;
#elif defined(__linux__)
    static const bool supported = (getauxval(AT_HWCAP) & (1UL << 6)) != 0; // HWCAP_SHA2
    return supported;
#else
    return false;
#endif
}
#endif

// Compresses `blocks` consecutive 64-byte blocks into `state`.
inline void CompressBlocks(std::uint32_t* state, const unsigned char* data, std::size_t blocks) noexcept
{
#ifdef GLAMDB_SHA256_X86
    if (HasX86Instructions()) {
        X86Blocks(state, data, blocks);
        return;
    }
#endif
#ifdef GLAMDB_SHA256_ARM
    if (HasArmInstructions()) {
        ArmBlocks(state, data, blocks);
        return;
    }
#endif
    PortableBlocks(state, data, blocks);
}

} // namespace NGlamDB::NIO::NSha256
