#pragma once

// Internal streaming JSON reader for request bodies. It accepts exactly the
// language of nlohmann::json::sax_parse(input, handler) in strict mode, without
// comments, and calls the same handler callbacks in the same order, so handler
// failures (duplicate keys, complexity limits) are raised at the same point.
// Notable inherited behavior: an optional UTF-8 BOM at offset 0, a NUL byte
// between tokens ends the input, integers that overflow int64/uint64 become
// doubles, and doubles that overflow are syntax errors. It does not track
// positions or build diagnostics; every syntax error just returns false.

#include <cstdint>
#include <string>
#include <string_view>

namespace glamdb::NTransport {
class TJsonReader
{
public:
    explicit TJsonReader(std::string_view input);

    // Returns false on a syntax error; handler exceptions propagate.
    template <typename THandler>
    bool Parse(THandler& handler);

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

    // Token scanners stay inline: they run for every token of a request body.
    inline EToken Next();
    inline EToken Literal(std::string_view rest, EToken token);
    inline EToken String();
    inline EToken Number(size_t start);
    // Escape decoding is rare and stays out of line.
    int Hex4();
    void AppendUtf8(uint32_t codepoint);
};
} // namespace glamdb::NTransport

#define GLAMDB_APPS_TRANSPORT_JSON_READER_INL_H_
#include "json_reader-inl.h"
#undef GLAMDB_APPS_TRANSPORT_JSON_READER_INL_H_
