#pragma once

#include <cstddef>
#include <string_view>

namespace glamdb::NTransport {
// Length of the well-formed UTF-8 sequence that starts at text[i], or 0. The
// accepted set is that of nlohmann's lexer and strict serializer.
inline size_t Utf8SequenceLength(std::string_view text, size_t i);
} // namespace glamdb::NTransport

#define GLAMDB_APPS_TRANSPORT_UTF8_INL_H_
#include "utf8-inl.h"
#undef GLAMDB_APPS_TRANSPORT_UTF8_INL_H_
