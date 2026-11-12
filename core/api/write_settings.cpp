#include "core/api/write_settings.h"

#include "core/api/binary.h"

#include <arrow/util/compression.h>

#include <algorithm>
#include <array>
#include <format>

namespace glamdb::detail {
namespace {

constexpr std::string_view Magic("COLUMNAR-WRITE\0\0", 16);
constexpr uint16_t FormatVersion = 1;
constexpr size_t MaxEncodedBytes = 16384;

// Option tags of the settings blob. Tags are written in increasing order, each
// at most once; an unknown tag is a newer format and fails closed.
enum class ETag : uint8_t { Codec = 1, Level = 2 };

struct TCodecPolicy
{
    ParquetCodec Codec;
    arrow::Compression::type Arrow;
    // GlamDB's documented range, intersected with the build's range.
    std::optional<std::pair<int32_t, int32_t>> Levels;
};

// zstd's negative "fast" levels are excluded; 1..22 is the documented range.
constexpr std::array<TCodecPolicy, 5> Policies{{
    {ParquetCodec::Uncompressed, arrow::Compression::UNCOMPRESSED, std::nullopt},
    {ParquetCodec::Snappy, arrow::Compression::SNAPPY, std::nullopt},
    {ParquetCodec::Gzip, arrow::Compression::GZIP, std::pair{1, 9}},
    {ParquetCodec::Lz4Raw, arrow::Compression::LZ4, std::pair{1, 12}},
    {ParquetCodec::Zstd, arrow::Compression::ZSTD, std::pair{1, 22}},
}};

bool KnownCodec(ParquetCodec codec)
{
    return codec >= ParquetCodec::Uncompressed && codec <= ParquetCodec::Zstd;
}

ErrorDetail Invalid(std::string message)
{
    return {"INVALID_ARGUMENT", std::move(message), RetryHint::FixInput};
}

void CheckCompressionShape(const ParquetCompression& compression)
{
    if (!KnownCodec(compression.codec)) {
        throw Failure(Invalid("unknown Parquet compression codec"));
    }
}

void EncodeCompression(Encoder& out, const std::optional<ParquetCompression>& compression)
{
    if (!compression) {
        out.Number<uint8_t>(0);
        return;
    }
    out.Number<uint8_t>(compression->level ? 2 : 1);
    out.Number(static_cast<uint8_t>(ETag::Codec));
    out.Number(static_cast<uint8_t>(compression->codec));
    if (compression->level) {
        out.Number(static_cast<uint8_t>(ETag::Level));
        out.Number(*compression->level);
    }
}

std::optional<ParquetCompression> DecodeCompression(Decoder& in)
{
    const auto count = in.Number<uint8_t>();
    std::optional<ParquetCompression> result;
    uint8_t previous = 0;
    for (uint8_t i = 0; i < count; ++i) {
        const auto tag = in.Number<uint8_t>();
        Require(tag > previous, "CORRUPTION", "write settings options are not canonical");
        previous = tag;
        switch (static_cast<ETag>(tag)) {
            case ETag::Codec: {
                const auto codec = static_cast<ParquetCodec>(in.Number<uint8_t>());
                Require(KnownCodec(codec), "UNSUPPORTED_STORE_VERSION", "unknown Parquet codec in write settings");
                result = ParquetCompression{codec, std::nullopt};
                break;
            }
            case ETag::Level:
                Require(result.has_value(), "CORRUPTION", "write settings level without a codec");
                result->level = in.Number<int32_t>();
                break;
            default:
                throw Failure(
                    {"UNSUPPORTED_STORE_VERSION", "unknown write settings option", RetryHint::OperatorAction});
        }
    }
    Require(count == 0 || result.has_value(), "CORRUPTION", "write settings options are not canonical");
    return result;
}

} // namespace

bool IsDefault(const WriteSettings& settings)
{
    return !settings.compression && settings.columns.empty();
}

std::string_view CodecName(ParquetCodec codec)
{
    switch (codec) {
        case ParquetCodec::Uncompressed:
            return "uncompressed";
        case ParquetCodec::Snappy:
            return "snappy";
        case ParquetCodec::Gzip:
            return "gzip";
        case ParquetCodec::Lz4Raw:
            return "lz4_raw";
        case ParquetCodec::Zstd:
            return "zstd";
    }
    return "unknown";
}

std::optional<ParquetCodec> ParseCodecName(std::string_view name)
{
    for (const auto& policy : Policies) {
        if (CodecName(policy.Codec) == name) {
            return policy.Codec;
        }
    }
    return std::nullopt;
}

Result<WriteSettings> CanonicalWriteSettings(const SchemaSpec& schema, const WriteSettings& settings)
{
    try {
        WriteSettings result;
        if (settings.compression) {
            CheckCompressionShape(*settings.compression);
            result.compression = settings.compression;
        }
        Require(settings.columns.size() <= schema.fields.size(),
            "INVALID_ARGUMENT",
            "write settings name more columns than the schema has");
        std::vector<std::optional<ColumnWriteSettings>> byField(schema.fields.size());
        for (const auto& column : settings.columns) {
            const auto field = std::ranges::find(schema.fields, column.field, &FieldSpec::name);
            if (field == schema.fields.end()) {
                throw Failure(Invalid("write settings name an unknown column"));
            }
            auto& slot = byField[static_cast<size_t>(field - schema.fields.begin())];
            if (slot) {
                throw Failure(Invalid("write settings name a column more than once"));
            }
            if (!column.compression) {
                throw Failure(Invalid("a column write setting must set at least one option"));
            }
            CheckCompressionShape(*column.compression);
            slot = column;
        }
        for (auto& slot : byField) {
            if (slot) {
                result.columns.push_back(std::move(*slot));
            }
        }
        return result;
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

Status CheckWriteSettingsSupported(const WriteSettings& settings, std::span<const ParquetCodecCapability> codecs)
{
    const auto check = [&](const ParquetCompression& compression) -> Status {
        const auto capability = std::ranges::find(codecs, compression.codec, &ParquetCodecCapability::codec);
        if (capability == codecs.end()) {
            return std::unexpected(ErrorDetail{"DEPENDENCY_UNAVAILABLE",
                std::format("Parquet codec {} is not available in this build", CodecName(compression.codec)),
                RetryHint::OperatorAction});
        }
        if (!compression.level) {
            return {};
        }
        if (!capability->min_level || !capability->max_level) {
            return std::unexpected(
                Invalid(std::format("Parquet codec {} does not accept a level", CodecName(compression.codec))));
        }
        if (*compression.level < *capability->min_level || *compression.level > *capability->max_level) {
            return std::unexpected(Invalid(std::format("Parquet {} level must be in {}..{}",
                CodecName(compression.codec),
                *capability->min_level,
                *capability->max_level)));
        }
        return {};
    };
    if (settings.compression) {
        if (auto checked = check(*settings.compression); !checked) {
            return checked;
        }
    }
    for (const auto& column : settings.columns) {
        if (column.compression) {
            if (auto checked = check(*column.compression); !checked) {
                return checked;
            }
        }
    }
    return {};
}

Result<std::string> EncodeWriteSettings(const SchemaSpec& schema, const WriteSettings& settings)
{
    try {
        const auto canonical = CanonicalWriteSettings(schema, settings);
        if (!canonical) {
            return std::unexpected(canonical.error());
        }
        Require(*canonical == settings, "INVALID_ARGUMENT", "write settings are not canonical");
        Encoder out;
        out.bytes.assign(Magic);
        out.Number(FormatVersion);
        EncodeCompression(out, settings.compression);
        out.Number(static_cast<uint16_t>(settings.columns.size()));
        for (const auto& column : settings.columns) {
            const auto field = std::ranges::find(schema.fields, column.field, &FieldSpec::name);
            out.Number(static_cast<uint32_t>(field - schema.fields.begin() + 1));
            EncodeCompression(out, column.compression);
        }
        Require(out.bytes.size() <= MaxEncodedBytes, "INVALID_ARGUMENT", "write settings are too large");
        return out.bytes;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<WriteSettings> DecodeWriteSettings(const SchemaSpec& schema, std::string_view bytes)
{
    try {
        Require(bytes.size() <= MaxEncodedBytes && bytes.starts_with(Magic), "CORRUPTION", "invalid write settings");
        Decoder in{bytes.substr(Magic.size())};
        Require(in.Number<uint16_t>() == FormatVersion, "UNSUPPORTED_STORE_VERSION", "unknown write settings version");
        WriteSettings settings;
        settings.compression = DecodeCompression(in);
        const auto count = in.Number<uint16_t>();
        Require(count <= schema.fields.size(), "CORRUPTION", "invalid write settings column count");
        uint32_t previous = 0;
        for (uint16_t i = 0; i < count; ++i) {
            const auto id = in.Number<uint32_t>();
            Require(id > previous && id <= schema.fields.size(), "CORRUPTION", "invalid write settings field ID");
            previous = id;
            ColumnWriteSettings column{schema.fields[id - 1].name, DecodeCompression(in)};
            Require(column.compression.has_value(), "CORRUPTION", "empty column write settings");
            settings.columns.push_back(std::move(column));
        }
        in.End();
        const auto canonical = CanonicalWriteSettings(schema, settings);
        Require(canonical && *canonical == settings, "CORRUPTION", "invalid write settings");
        return settings;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

std::vector<ParquetCodecCapability> ParquetCodecCapabilities()
{
    std::vector<ParquetCodecCapability> result;
    for (const auto& policy : Policies) {
        if (!arrow::util::Codec::IsAvailable(policy.Arrow)) {
            continue;
        }
        ParquetCodecCapability capability{policy.Codec};
        if (policy.Levels) {
            const auto minimum = arrow::util::Codec::MinimumCompressionLevel(policy.Arrow);
            const auto maximum = arrow::util::Codec::MaximumCompressionLevel(policy.Arrow);
            const auto fallback = arrow::util::Codec::DefaultCompressionLevel(policy.Arrow);
            if (!arrow::util::Codec::SupportsCompressionLevel(policy.Arrow) || !minimum.ok() || !maximum.ok() ||
                !fallback.ok())
            {
                continue;
            }
            const auto low = std::max(policy.Levels->first, *minimum);
            const auto high = std::min(policy.Levels->second, *maximum);
            if (low > high || *fallback < low || *fallback > high) {
                continue;
            }
            capability.min_level = low;
            capability.max_level = high;
            capability.default_level = *fallback;
        }
        result.push_back(capability);
    }
    return result;
}

std::vector<ParquetCompression> EffectiveCompression(const SchemaSpec& schema, const WriteSettings& settings)
{
    const auto fallback = settings.compression.value_or(ParquetCompression{});
    std::vector<ParquetCompression> result(schema.fields.size(), fallback);
    for (const auto& column : settings.columns) {
        const auto field = std::ranges::find(schema.fields, column.field, &FieldSpec::name);
        if (field != schema.fields.end() && column.compression) {
            result[static_cast<size_t>(field - schema.fields.begin())] = *column.compression;
        }
    }
    return result;
}

} // namespace glamdb::detail
