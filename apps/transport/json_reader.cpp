#include "apps/transport/json_reader.h"

#include <clocale>

namespace glamdb::NTransport {
TJsonReader::TJsonReader(std::string_view input)
    : Input_(input)
{
    // Like nlohmann's lexer, read the locale's decimal point once per parse.
    const auto* locale = std::localeconv();
    if (locale != nullptr && locale->decimal_point != nullptr && locale->decimal_point[0] != '\0') {
        DecimalPoint_ = locale->decimal_point[0];
    }
}

int TJsonReader::Hex4()
{
    if (Input_.size() - Position_ < 4) {
        return -1;
    }
    int value = 0;
    for (int i = 0; i < 4; ++i) {
        const auto c = Input_[Position_++];
        int digit = 0;
        if (c >= '0' && c <= '9') {
            digit = c - '0';
        } else if (c >= 'A' && c <= 'F') {
            digit = c - 'A' + 10;
        } else if (c >= 'a' && c <= 'f') {
            digit = c - 'a' + 10;
        } else {
            return -1;
        }
        value = value * 16 + digit;
    }
    return value;
}

void TJsonReader::AppendUtf8(uint32_t codepoint)
{
    if (codepoint < 0x80) {
        Buffer_ += static_cast<char>(codepoint);
    } else if (codepoint <= 0x7FF) {
        Buffer_ += static_cast<char>(0xC0 | (codepoint >> 6));
        Buffer_ += static_cast<char>(0x80 | (codepoint & 0x3F));
    } else if (codepoint <= 0xFFFF) {
        Buffer_ += static_cast<char>(0xE0 | (codepoint >> 12));
        Buffer_ += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
        Buffer_ += static_cast<char>(0x80 | (codepoint & 0x3F));
    } else {
        Buffer_ += static_cast<char>(0xF0 | (codepoint >> 18));
        Buffer_ += static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F));
        Buffer_ += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
        Buffer_ += static_cast<char>(0x80 | (codepoint & 0x3F));
    }
}
} // namespace glamdb::NTransport
