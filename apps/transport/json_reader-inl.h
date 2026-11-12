#ifndef GLAMDB_APPS_TRANSPORT_JSON_READER_INL_H_
#error "Include json_reader.h instead of json_reader-inl.h"
#endif

#include "apps/transport/utf8.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <vector>

namespace glamdb::NTransport {
template <typename THandler>
bool TJsonReader::Parse(THandler& handler)
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

inline TJsonReader::EToken TJsonReader::Next()
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

inline TJsonReader::EToken TJsonReader::Literal(std::string_view rest, EToken token)
{
    if (Input_.substr(Position_, rest.size()) != rest) {
        return EToken::Error;
    }
    Position_ += rest.size();
    return token;
}

inline TJsonReader::EToken TJsonReader::String()
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

inline TJsonReader::EToken TJsonReader::Number(size_t start)
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
} // namespace glamdb::NTransport
