#pragma once

#include "storage/types.h"

#include <bit>
#include <stdexcept>
#include <string_view>
#include <type_traits>

namespace glamdb::detail {

struct Failure : std::runtime_error
{
    ErrorDetail detail;
    bool uncertain = false;
    int native_code = 0;
    std::optional<CommitPhase> phase;
    explicit Failure(ErrorDetail error, bool unknown = false)
        : std::runtime_error(error.message)
        , detail(std::move(error))
        , uncertain(unknown)
    { }
};

inline void Require(bool condition, std::string_view code, std::string_view message)
{
    if (!condition) {
        throw Failure({std::string(code), std::string(message), RetryHint::OperatorAction});
    }
}

struct Encoder
{
    std::string bytes;
    template <class T>
    void Number(T value)
    {
        using U = std::make_unsigned_t<T>;
        auto bits = static_cast<uint64_t>(std::bit_cast<U>(value));
        for (size_t i = 0; i < sizeof(T); ++i) {
            bytes.push_back(static_cast<char>(bits & 255));
            bits >>= 8;
        }
    }
    void Text(std::string_view text)
    {
        Require(text.size() <= 16ULL * 1024 * 1024, "RESOURCE_EXHAUSTED", "encoded string too large");
        Number(static_cast<uint32_t>(text.size()));
        bytes.append(text);
    }
    void OptionalNumber(std::optional<int64_t> value)
    {
        Number<uint8_t>(value.has_value() ? 1 : 0);
        if (value) {
            Number(*value);
        }
    }
};

struct Decoder
{
    std::string_view bytes;
    size_t position = 0;
    template <class T>
    T Number()
    {
        Require(position <= bytes.size() && sizeof(T) <= bytes.size() - position,
            "CORRUPTION",
            "truncated catalog record");
        using U = std::make_unsigned_t<T>;
        U value = 0;
        for (size_t i = 0; i < sizeof(T); ++i) {
            value |= static_cast<U>(static_cast<unsigned char>(bytes[position++])) << (i * 8);
        }
        return std::bit_cast<T>(value);
    }
    bool Boolean()
    {
        const auto value = Number<uint8_t>();
        Require(value <= 1, "CORRUPTION", "invalid boolean in catalog record");
        return value == 1;
    }
    std::string Text()
    {
        const auto size = Number<uint32_t>();
        Require(size <= bytes.size() - position, "CORRUPTION", "truncated catalog string");
        auto text = std::string(bytes.substr(position, size));
        position += size;
        return text;
    }
    std::optional<int64_t> OptionalNumber()
    {
        if (!Boolean()) {
            return std::nullopt;
        }
        return Number<int64_t>();
    }
    void End() const
    {
        Require(position == bytes.size(), "CORRUPTION", "unexpected catalog record suffix");
    }
};

} // namespace glamdb::detail
