#pragma once

#include "storage/metadata.h"
#include "storage/types.h"

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace glamdb::detail {

bool IsDefault(const WriteSettings& settings);
std::string_view CodecName(ParquetCodec codec);
std::optional<ParquetCodec> ParseCodecName(std::string_view name);
std::string_view EncodingName(ParquetEncoding encoding);
std::optional<ParquetEncoding> ParseEncodingName(std::string_view name);
std::string_view PolicyName(DictionaryPolicy policy);
std::optional<DictionaryPolicy> ParsePolicyName(std::string_view name);

// Structure only: every column names a distinct schema field and sets at least
// one option; codec, policy and encoding values are known; encodings fit their
// column types; no explicit encoding is combined with a dictionary; the
// dictionary page size limit is in range. Returns the canonical
// (schema-ordered) form. Codec availability and level ranges are build
// specific: see CheckWriteSettingsSupported.
Result<WriteSettings> CanonicalWriteSettings(const SchemaSpec& schema, const WriteSettings& settings);

// Every codec is available in this build and every level is in its range.
Status CheckWriteSettingsSupported(const WriteSettings& settings, std::span<const ParquetCodecCapability> codecs);

// Canonical settings only. Fields are stored by their stable field ID, so the
// blob is valid exactly for this schema.
Result<std::string> EncodeWriteSettings(const SchemaSpec& schema, const WriteSettings& settings);
Result<WriteSettings> DecodeWriteSettings(const SchemaSpec& schema, std::string_view bytes);

// The Parquet codecs of the linked Arrow build, intersected with GlamDB's
// documented allowlist and level ranges, in ParquetCodec order.
std::vector<ParquetCodecCapability> ParquetCodecCapabilities();

// Encodings with their field types, dictionary types and page size limits.
// Decimal uses plain for every precision; date supports the INT32 encodings.
void AddEncodingCapabilities(EngineCapabilities& capabilities);

struct TEffectiveColumnSettings
{
    ParquetCompression compression{};
    // Whether pages start dictionary encoded (they fall back to plain when
    // the dictionary outgrows its limit).
    bool dictionary = true;
    // Encoding of non-dictionary pages.
    ParquetEncoding encoding = ParquetEncoding::Plain;
};

// Column override, else collection default, else engine default, per field in
// schema order. An explicit column encoding turns the column's dictionary off.
std::vector<TEffectiveColumnSettings> EffectiveColumnSettings(const SchemaSpec& schema, const WriteSettings& settings);

} // namespace glamdb::detail
