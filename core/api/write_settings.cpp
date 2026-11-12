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
// at most once; an unknown tag is a newer format and fails closed. The page
// size limit belongs to the collection, the encoding to a column.
enum class ETag : uint8_t { Codec = 1, Level = 2, Dictionary = 3, Encoding = 4, DictionaryPageSizeLimit = 5 };

// The dictionary page size limit range accepted everywhere; Arrow's default.
constexpr int64_t MinDictionaryPageSizeLimit = 4096;
constexpr int64_t MaxDictionaryPageSizeLimit = 8LL << 20;
constexpr int64_t DefaultDictionaryPageSizeLimit = 1LL << 20;

struct TEncodingPolicy
{
    ParquetEncoding Encoding;
    std::string_view Name;
    std::array<bool, 7> Types; // String, Bool, Int64, Float64, TimestampUsUtc, Decimal, Date
};

constexpr std::array<TEncodingPolicy, 6> Encodings{{
    {ParquetEncoding::Plain, "plain", {true, true, true, true, true, true, true}},
    {ParquetEncoding::DeltaBinaryPacked, "delta_binary_packed", {false, false, true, false, true, false, true}},
    {ParquetEncoding::DeltaLengthByteArray,
        "delta_length_byte_array",
        {true, false, false, false, false, false, false}},
    {ParquetEncoding::DeltaByteArray, "delta_byte_array", {true, false, false, false, false, false, false}},
    {ParquetEncoding::ByteStreamSplit, "byte_stream_split", {false, false, true, true, true, false, true}},
    {ParquetEncoding::Rle, "rle", {false, true, false, false, false, false, false}},
}};

bool KnownEncoding(ParquetEncoding encoding)
{
    return encoding >= ParquetEncoding::Plain && encoding <= ParquetEncoding::Rle;
}

bool KnownPolicy(DictionaryPolicy policy)
{
    return policy >= DictionaryPolicy::Auto && policy <= DictionaryPolicy::Disabled;
}

bool Accepts(ParquetEncoding encoding, FieldType type)
{
    const auto index = static_cast<size_t>(type) - 1;
    return KnownEncoding(encoding) && index < Encodings.front().Types.size() &&
        Encodings[static_cast<size_t>(encoding) - 1].Types[index];
}

// Options of one settings level, in tag order.
struct TOptions
{
    std::optional<ParquetCompression> Compression;
    std::optional<DictionaryPolicy> Dictionary;
    std::optional<ParquetEncoding> Encoding;
    std::optional<int64_t> DictionaryPageSizeLimit;
};

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

void EncodeOptions(Encoder& out, const TOptions& options)
{
    const auto& compression = options.Compression;
    out.Number(static_cast<uint8_t>((compression ? 1 : 0) + (compression && compression->level ? 1 : 0) +
        (options.Dictionary ? 1 : 0) + (options.Encoding ? 1 : 0) + (options.DictionaryPageSizeLimit ? 1 : 0)));
    if (compression) {
        out.Number(static_cast<uint8_t>(ETag::Codec));
        out.Number(static_cast<uint8_t>(compression->codec));
        if (compression->level) {
            out.Number(static_cast<uint8_t>(ETag::Level));
            out.Number(*compression->level);
        }
    }
    if (options.Dictionary) {
        out.Number(static_cast<uint8_t>(ETag::Dictionary));
        out.Number(static_cast<uint8_t>(*options.Dictionary));
    }
    if (options.Encoding) {
        out.Number(static_cast<uint8_t>(ETag::Encoding));
        out.Number(static_cast<uint8_t>(*options.Encoding));
    }
    if (options.DictionaryPageSizeLimit) {
        out.Number(static_cast<uint8_t>(ETag::DictionaryPageSizeLimit));
        out.Number(*options.DictionaryPageSizeLimit);
    }
}

TOptions DecodeOptions(Decoder& in, bool column)
{
    const auto count = in.Number<uint8_t>();
    TOptions result;
    uint8_t previous = 0;
    for (uint8_t i = 0; i < count; ++i) {
        const auto tag = in.Number<uint8_t>();
        Require(tag > previous, "CORRUPTION", "write settings options are not canonical");
        previous = tag;
        switch (static_cast<ETag>(tag)) {
            case ETag::Codec: {
                const auto codec = static_cast<ParquetCodec>(in.Number<uint8_t>());
                Require(KnownCodec(codec), "UNSUPPORTED_STORE_VERSION", "unknown Parquet codec in write settings");
                result.Compression = ParquetCompression{codec, std::nullopt};
                break;
            }
            case ETag::Level:
                Require(result.Compression.has_value(), "CORRUPTION", "write settings level without a codec");
                result.Compression->level = in.Number<int32_t>();
                break;
            case ETag::Dictionary: {
                const auto policy = static_cast<DictionaryPolicy>(in.Number<uint8_t>());
                Require(KnownPolicy(policy),
                    "UNSUPPORTED_STORE_VERSION",
                    "unknown dictionary policy in write settings");
                result.Dictionary = policy;
                break;
            }
            case ETag::Encoding: {
                Require(column, "CORRUPTION", "collection write settings cannot set an encoding");
                const auto encoding = static_cast<ParquetEncoding>(in.Number<uint8_t>());
                Require(KnownEncoding(encoding),
                    "UNSUPPORTED_STORE_VERSION",
                    "unknown Parquet encoding in write settings");
                result.Encoding = encoding;
                break;
            }
            case ETag::DictionaryPageSizeLimit:
                Require(!column, "CORRUPTION", "column write settings cannot set a dictionary page size limit");
                result.DictionaryPageSizeLimit = in.Number<int64_t>();
                break;
            default:
                throw Failure(
                    {"UNSUPPORTED_STORE_VERSION", "unknown write settings option", RetryHint::OperatorAction});
        }
    }
    return result;
}

} // namespace

bool IsDefault(const WriteSettings& settings)
{
    return settings == WriteSettings{};
}

std::string_view EncodingName(ParquetEncoding encoding)
{
    return KnownEncoding(encoding) ? Encodings[static_cast<size_t>(encoding) - 1].Name : "unknown";
}

std::optional<ParquetEncoding> ParseEncodingName(std::string_view name)
{
    for (const auto& policy : Encodings) {
        if (policy.Name == name) {
            return policy.Encoding;
        }
    }
    return std::nullopt;
}

std::string_view PolicyName(DictionaryPolicy policy)
{
    switch (policy) {
        case DictionaryPolicy::Auto:
            return "auto";
        case DictionaryPolicy::Enabled:
            return "enabled";
        case DictionaryPolicy::Disabled:
            return "disabled";
    }
    return "unknown";
}

std::optional<DictionaryPolicy> ParsePolicyName(std::string_view name)
{
    for (const auto policy : {DictionaryPolicy::Auto, DictionaryPolicy::Enabled, DictionaryPolicy::Disabled}) {
        if (PolicyName(policy) == name) {
            return policy;
        }
    }
    return std::nullopt;
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
        if (settings.dictionary && !KnownPolicy(*settings.dictionary)) {
            throw Failure(Invalid("unknown dictionary policy"));
        }
        result.dictionary = settings.dictionary;
        if (settings.dictionary_page_size_limit &&
            (*settings.dictionary_page_size_limit < MinDictionaryPageSizeLimit ||
                *settings.dictionary_page_size_limit > MaxDictionaryPageSizeLimit))
        {
            throw Failure(Invalid(std::format("dictionary page size limit must be in {}..{} bytes",
                MinDictionaryPageSizeLimit,
                MaxDictionaryPageSizeLimit)));
        }
        result.dictionary_page_size_limit = settings.dictionary_page_size_limit;
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
            if (!column.compression && !column.dictionary && !column.encoding) {
                throw Failure(Invalid("a column write setting must set at least one option"));
            }
            if (column.compression) {
                CheckCompressionShape(*column.compression);
            }
            if (column.dictionary && !KnownPolicy(*column.dictionary)) {
                throw Failure(Invalid("unknown dictionary policy"));
            }
            if (column.encoding && !Accepts(*column.encoding, field->type)) {
                throw Failure(Invalid(std::format("Parquet encoding {} does not accept column {}",
                    EncodingName(*column.encoding),
                    column.field)));
            }
            const bool boolean = field->type == FieldType::Bool;
            if (column.dictionary == DictionaryPolicy::Enabled && boolean) {
                throw Failure(Invalid("dictionary encoding is not available for bool columns"));
            }
            // The pinned writer ignores an encoding while a dictionary is in
            // use, so an explicit encoding needs the dictionary off.
            if (column.encoding &&
                (column.dictionary == DictionaryPolicy::Enabled ||
                    (column.dictionary == DictionaryPolicy::Auto && !boolean)))
            {
                throw Failure(Invalid("a column with an explicit encoding cannot use a dictionary"));
            }
            slot = column;
        }
        if (settings.dictionary == DictionaryPolicy::Enabled) {
            for (size_t index = 0; index < schema.fields.size(); ++index) {
                const auto& column = byField[index];
                if (schema.fields[index].type == FieldType::Bool &&
                    (!column || (!column->dictionary && !column->encoding)))
                {
                    throw Failure(Invalid("an enabled dictionary policy needs its own policy for bool columns"));
                }
            }
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
        EncodeOptions(out,
            {settings.compression, settings.dictionary, std::nullopt, settings.dictionary_page_size_limit});
        out.Number(static_cast<uint16_t>(settings.columns.size()));
        for (const auto& column : settings.columns) {
            const auto field = std::ranges::find(schema.fields, column.field, &FieldSpec::name);
            out.Number(static_cast<uint32_t>(field - schema.fields.begin() + 1));
            EncodeOptions(out, {column.compression, column.dictionary, column.encoding, std::nullopt});
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
        const auto options = DecodeOptions(in, false);
        settings.compression = options.Compression;
        settings.dictionary = options.Dictionary;
        settings.dictionary_page_size_limit = options.DictionaryPageSizeLimit;
        const auto count = in.Number<uint16_t>();
        Require(count <= schema.fields.size(), "CORRUPTION", "invalid write settings column count");
        uint32_t previous = 0;
        for (uint16_t i = 0; i < count; ++i) {
            const auto id = in.Number<uint32_t>();
            Require(id > previous && id <= schema.fields.size(), "CORRUPTION", "invalid write settings field ID");
            previous = id;
            const auto decoded = DecodeOptions(in, true);
            settings.columns.push_back(
                {schema.fields[id - 1].name, decoded.Compression, decoded.Dictionary, decoded.Encoding});
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

void AddEncodingCapabilities(EngineCapabilities& capabilities)
{
    constexpr std::array<FieldType, 7> types{FieldType::String,
        FieldType::Bool,
        FieldType::Int64,
        FieldType::Float64,
        FieldType::TimestampUsUtc,
        FieldType::Decimal,
        FieldType::Date};
    for (const auto& policy : Encodings) {
        ParquetEncodingCapability encoding{policy.Encoding, {}};
        for (size_t index = 0; index < types.size(); ++index) {
            if (policy.Types[index]) {
                encoding.types.push_back(types[index]);
            }
        }
        capabilities.parquet_encodings.push_back(std::move(encoding));
    }
    capabilities.dictionary_types = {FieldType::String,
        FieldType::Int64,
        FieldType::Float64,
        FieldType::TimestampUsUtc,
        FieldType::Decimal,
        FieldType::Date};
    capabilities.min_dictionary_page_size_limit = MinDictionaryPageSizeLimit;
    capabilities.max_dictionary_page_size_limit = MaxDictionaryPageSizeLimit;
    capabilities.default_dictionary_page_size_limit = DefaultDictionaryPageSizeLimit;
}

std::vector<TEffectiveColumnSettings> EffectiveColumnSettings(const SchemaSpec& schema, const WriteSettings& settings)
{
    std::vector<TEffectiveColumnSettings> result;
    for (const auto& field : schema.fields) {
        const auto column = std::ranges::find(settings.columns, field.name, &ColumnWriteSettings::field);
        const auto* own = column == settings.columns.end() ? nullptr : &*column;
        TEffectiveColumnSettings effective;
        effective.compression = own && own->compression ? *own->compression
            : settings.compression                      ? *settings.compression
                                                        : ParquetCompression{};
        effective.encoding = own && own->encoding ? *own->encoding : ParquetEncoding::Plain;
        const auto policy = own && own->dictionary ? *own->dictionary
            : own && own->encoding                 ? DictionaryPolicy::Disabled
                                                   : settings.dictionary.value_or(DictionaryPolicy::Auto);
        effective.dictionary = policy != DictionaryPolicy::Disabled && field.type != FieldType::Bool;
        result.push_back(effective);
    }
    return result;
}

} // namespace glamdb::detail
