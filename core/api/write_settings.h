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

// Structure only: every column names a distinct schema field and sets at least
// one option; codec values are known. Returns the canonical (schema-ordered)
// form. Availability and level ranges are build specific: see
// CheckWriteSettingsSupported.
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

// Collection override, else the engine default, per field (schema order).
std::vector<ParquetCompression> EffectiveCompression(const SchemaSpec& schema, const WriteSettings& settings);

} // namespace glamdb::detail
