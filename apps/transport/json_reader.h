#pragma once

// Internal streaming JSON reader for request bodies. It accepts exactly the
// language of nlohmann::json::sax_parse(input, handler) in strict mode, without
// comments, and calls the same handler callbacks in the same order, so handler
// failures (duplicate keys, complexity limits) are raised at the same point.
// Notable inherited behavior: an optional UTF-8 BOM at offset 0, a NUL byte
// between tokens ends the input, integers that overflow int64/uint64 become
// doubles, and doubles that overflow are syntax errors. It does not track
// positions or build diagnostics; every syntax error just returns false.

#include <algorithm>
#include <charconv>
#include <clocale>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

namespace glamdb::NTransport {
// Length of the well-formed UTF-8 sequence that starts at text[i], or 0. The
// accepted set is that of nlohmann's lexer and strict serializer.
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

class TJsonReader
{
public:
    explicit TJsonReader(std::string_view input)
        : Input_(input)
    {
        // Like nlohmann's lexer, read the locale's decimal point once per parse.
        const auto* locale = std::localeconv();
        if (locale != nullptr && locale->decimal_point != nullptr && locale->decimal_point[0] != '\0') {
            DecimalPoint_ = locale->decimal_point[0];
        }
    }

    // Returns false on a syntax error; handler exceptions propagate.
    template <class THandler>
    bool Parse(THandler& handler)
    {
        if (!Input_.empty() && static_cast<uint8_t>(Input_[0]) == 0xEF) {
            if (!Input_.starts_with("\xEF\xBB\xBF")) {
                return false;
            }
            Position_ = 3;
        }
        // Open containers: true for arrays, as in nlohmann's parser.
        std::vector<bool> states;
        auto token = Next();
        while (true) {
            switch (token) {
                case EToken::BeginObject:
                    handler.start_object(static_cast<size_t>(-1));
                    token = Next();
                    if (token == EToken::EndObject) {
                        handler.end_object();
                        break;
                    }
                    if (token != EToken::String) {
                        return false;
                    }
                    handler.key(Buffer_);
                    if (Next() != EToken::Colon) {
                        return false;
                    }
                    states.push_back(false);
                    token = Next();
                    continue;
                case EToken::BeginArray:
                    handler.start_array(static_cast<size_t>(-1));
                    token = Next();
                    if (token == EToken::EndArray) {
                        handler.end_array();
                        break;
                    }
                    states.push_back(true);
                    continue;
                case EToken::Float:
                    if (!std::isfinite(Float_)) {
                        return false;
                    }
                    handler.number_float(Float_, Number_);
                    break;
                case EToken::False:
                    handler.boolean(false);
                    break;
                case EToken::True:
                    handler.boolean(true);
                    break;
                case EToken::Null:
                    handler.null();
                    break;
                case EToken::Integer:
                    handler.number_integer(Integer_);
                    break;
                case EToken::Unsigned:
                    handler.number_unsigned(Unsigned_);
                    break;
                case EToken::String:
                    handler.string(Buffer_);
                    break;
                default:
                    return false;
            }
            // The value is complete: close containers until another value starts.
            while (true) {
                if (states.empty()) {
                    return Next() == EToken::End;
                }
                token = Next();
                if (token == EToken::Comma) {
                    if (!states.back()) {
                        if (Next() != EToken::String) {
                            return false;
                        }
                        handler.key(Buffer_);
                        if (Next() != EToken::Colon) {
                            return false;
                        }
                    }
                    token = Next();
                    break;
                }
                if (token != (states.back() ? EToken::EndArray : EToken::EndObject)) {
                    return false;
                }
                if (states.back()) {
                    handler.end_array();
                } else {
                    handler.end_object();
                }
                states.pop_back();
            }
        }
    }

private:
    enum class EToken : uint8_t {
        BeginObject,
        EndObject,
        BeginArray,
        EndArray,
        Colon,
        Comma,
        True,
        False,
        Null,
        String,
        Integer,
        Unsigned,
        Float,
        End,
        Error,
    };
    std::string_view Input_;
    size_t Position_ = 0;
    std::string Buffer_;
    std::string Number_;
    int64_t Integer_ = 0;
    uint64_t Unsigned_ = 0;
    double Float_ = 0;
    char DecimalPoint_ = '.';

    EToken Next()
    {
        while (Position_ < Input_.size()) {
            const auto c = Input_[Position_];
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                break;
            }
            ++Position_;
        }
        if (Position_ == Input_.size()) {
            return EToken::End;
        }
        const auto c = Input_[Position_++];
        switch (c) {
            case '{':
                return EToken::BeginObject;
            case '}':
                return EToken::EndObject;
            case '[':
                return EToken::BeginArray;
            case ']':
                return EToken::EndArray;
            case ':':
                return EToken::Colon;
            case ',':
                return EToken::Comma;
            case 't':
                return Literal("rue", EToken::True);
            case 'f':
                return Literal("alse", EToken::False);
            case 'n':
                return Literal("ull", EToken::Null);
            case '"':
                return String();
            case '\0':
                return EToken::End;
            default:
                if (c == '-' || (c >= '0' && c <= '9')) {
                    return Number(Position_ - 1);
                }
                return EToken::Error;
        }
    }

    EToken Literal(std::string_view rest, EToken token)
    {
        if (Input_.substr(Position_, rest.size()) != rest) {
            return EToken::Error;
        }
        Position_ += rest.size();
        return token;
    }

    int Hex4()
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

    void AppendUtf8(uint32_t codepoint)
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

    EToken String()
    {
        Buffer_.clear();
        auto run = Position_;
        while (true) {
            if (Position_ >= Input_.size()) {
                return EToken::Error;
            }
            const auto byte = static_cast<uint8_t>(Input_[Position_]);
            if (byte >= 0x20 && byte < 0x80 && byte != '"' && byte != '\\') {
                ++Position_;
                continue;
            }
            if (byte >= 0x80) {
                const auto length = Utf8SequenceLength(Input_, Position_);
                if (length == 0) {
                    return EToken::Error;
                }
                Position_ += length;
                continue;
            }
            Buffer_.append(Input_.data() + run, Position_ - run);
            ++Position_;
            if (byte == '"') {
                return EToken::String;
            }
            if (byte != '\\' || Position_ >= Input_.size()) {
                return EToken::Error;
            }
            switch (Input_[Position_++]) {
                case '"':
                    Buffer_ += '"';
                    break;
                case '\\':
                    Buffer_ += '\\';
                    break;
                case '/':
                    Buffer_ += '/';
                    break;
                case 'b':
                    Buffer_ += '\b';
                    break;
                case 'f':
                    Buffer_ += '\f';
                    break;
                case 'n':
                    Buffer_ += '\n';
                    break;
                case 'r':
                    Buffer_ += '\r';
                    break;
                case 't':
                    Buffer_ += '\t';
                    break;
                case 'u': {
                    const auto first = Hex4();
                    if (first < 0 || (first >= 0xDC00 && first <= 0xDFFF)) {
                        return EToken::Error;
                    }
                    auto codepoint = static_cast<uint32_t>(first);
                    if (first >= 0xD800 && first <= 0xDBFF) {
                        if (Input_.substr(Position_, 2) != "\\u") {
                            return EToken::Error;
                        }
                        Position_ += 2;
                        const auto second = Hex4();
                        if (second < 0xDC00 || second > 0xDFFF) {
                            return EToken::Error;
                        }
                        codepoint = (codepoint << 10) + static_cast<uint32_t>(second) - 0x35FDC00U;
                    }
                    AppendUtf8(codepoint);
                    break;
                }
                default:
                    return EToken::Error;
            }
            run = Position_;
        }
    }

    EToken Number(size_t start)
    {
        const auto digit = [&] {
            return Position_ < Input_.size() && Input_[Position_] >= '0' && Input_[Position_] <= '9';
        };
        const auto digits = [&] {
            while (digit()) {
                ++Position_;
            }
        };
        const bool negative = Input_[start] == '-';
        if (negative) {
            if (!digit()) {
                return EToken::Error;
            }
            ++Position_;
        }
        // A leading zero ends the integer part.
        if (Input_[Position_ - 1] != '0') {
            digits();
        }
        bool fractional = false;
        if (Position_ < Input_.size() && Input_[Position_] == '.') {
            ++Position_;
            if (!digit()) {
                return EToken::Error;
            }
            digits();
            fractional = true;
        }
        if (Position_ < Input_.size() && (Input_[Position_] == 'e' || Input_[Position_] == 'E')) {
            ++Position_;
            if (Position_ < Input_.size() && (Input_[Position_] == '+' || Input_[Position_] == '-')) {
                ++Position_;
            }
            if (!digit()) {
                return EToken::Error;
            }
            digits();
            fractional = true;
        }
        const auto* first = Input_.data() + start;
        const auto* last = Input_.data() + Position_;
        if (!fractional) {
            if (negative) {
                const auto parsed = std::from_chars(first, last, Integer_);
                if (parsed.ec == std::errc{}) {
                    return EToken::Integer;
                }
            } else {
                const auto parsed = std::from_chars(first, last, Unsigned_);
                if (parsed.ec == std::errc{}) {
                    return EToken::Unsigned;
                }
            }
        }
        // nlohmann converts the token with strtod, writing the current locale's
        // decimal point in place of '.'.
        Number_.assign(first, last);
        if (DecimalPoint_ != '.') {
            std::ranges::replace(Number_, '.', DecimalPoint_);
        }
        Float_ = std::strtod(Number_.c_str(), nullptr);
        return EToken::Float;
    }
};
} // namespace glamdb::NTransport
