#ifndef GLAMDB_CORE_API_CANONICAL_INL_H_
#error "Include canonical.h instead of canonical-inl.h"
#endif

#include <bit>
#include <cstring>
#include <type_traits>

namespace glamdb::detail {

template <typename T>
void TCanonicalHasher::AddNumber(T value)
{
    using U = std::make_unsigned_t<T>;
    if (sizeof(T) > Buffer_.size() - Used_) {
        Flush();
    }
    // A local cursor: char stores may alias the members.
    auto* out = Buffer_.data() + Used_;
    if constexpr (std::endian::native == std::endian::little) {
        std::memcpy(out, &value, sizeof(T));
    } else {
        auto bits = static_cast<uint64_t>(std::bit_cast<U>(value));
        for (size_t i = 0; i < sizeof(T); ++i) {
            out[i] = static_cast<char>(bits & 255);
            bits >>= 8;
        }
    }
    Used_ += sizeof(T);
    Bytes_ += sizeof(T);
}

inline uint64_t TCanonicalHasher::GetSize() const
{
    return Bytes_;
}

} // namespace glamdb::detail
