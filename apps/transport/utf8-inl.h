#ifndef GLAMDB_APPS_TRANSPORT_UTF8_INL_H_
#error "Include utf8.h instead of utf8-inl.h"
#endif

#include <cstdint>

namespace glamdb::NTransport {
inline size_t Utf8SequenceLength(std::string_view text, size_t i)
{
    const auto byte = [&](size_t at) {
        return at < text.size() ? static_cast<uint8_t>(text[at]) : uint8_t{0};
    };
    const auto within = [&](size_t at, uint8_t low, uint8_t high) {
        const auto value = byte(at);
        return value >= low && value <= high;
    };
    const auto lead = byte(i);
    if (lead >= 0xC2 && lead <= 0xDF) {
        return within(i + 1, 0x80, 0xBF) ? 2 : 0;
    }
    if (lead >= 0xE0 && lead <= 0xEF) {
        const uint8_t low = lead == 0xE0 ? 0xA0 : 0x80;
        const uint8_t high = lead == 0xED ? 0x9F : 0xBF;
        return within(i + 1, low, high) && within(i + 2, 0x80, 0xBF) ? 3 : 0;
    }
    if (lead >= 0xF0 && lead <= 0xF4) {
        const uint8_t low = lead == 0xF0 ? 0x90 : 0x80;
        const uint8_t high = lead == 0xF4 ? 0x8F : 0xBF;
        return within(i + 1, low, high) && within(i + 2, 0x80, 0xBF) && within(i + 3, 0x80, 0xBF) ? 4 : 0;
    }
    return 0;
}
} // namespace glamdb::NTransport
