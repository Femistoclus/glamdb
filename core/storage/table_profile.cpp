#include "core/storage/table_profile.h"
#include "core/storage/iceberg_error_boundary.h"
#include "core/storage/snapshot_retention.h"

#include <arrow/util/utf8.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/type.h>

#include <algorithm>
#include <span>

namespace NGlamDB::NStorage {
using iceberg::Error;
namespace {

std::shared_ptr<iceberg::Type> IcebergType(const TFieldSpec& field)
{
    switch (field.Type) {
        case EFieldType::String:
            return iceberg::string();
        case EFieldType::Bool:
            return iceberg::boolean();
        case EFieldType::Int64:
            return iceberg::int64();
        case EFieldType::Float64:
            return iceberg::float64();
        case EFieldType::TimestampUsUtc:
            return iceberg::timestamp_tz();
        case EFieldType::Decimal:
            return iceberg::decimal(field.Precision, field.Scale);
        case EFieldType::Date:
            return iceberg::date();
    }
    return {};
}

}

iceberg::Result<TTableProfile> MakeTableProfile(const TSchemaSpec& spec)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<TTableProfile> {
        constexpr std::size_t maxNameBytes = 128;
        if (spec.Fields.empty() || spec.Fields.size() > MaxSchemaFields) {
            return iceberg::InvalidArgument("Schema must contain 1..128 fields");
        }
        arrow::util::InitializeUTF8();
        std::vector<iceberg::SchemaField> fields;
        fields.reserve(spec.Fields.size());
        bool hasKey = false;
        for (std::size_t index = 0; index < spec.Fields.size(); ++index) {
            const auto& field = spec.Fields[index];
            if (field.Name.empty() || field.Name.size() > maxNameBytes || field.Name.find('\0') != std::string::npos ||
                !arrow::util::ValidateUTF8(field.Name))
            {
                return iceberg::InvalidArgument("Invalid field name at index {}", index);
            }
            if (std::ranges::any_of(std::span(spec.Fields).first(index), [&](const TFieldSpec& previous) {
                    return previous.Name == field.Name;
                }))
            {
                return iceberg::InvalidArgument("Duplicate field name at index {}", index);
            }
            if (field.Type == EFieldType::Decimal) {
                if (field.Precision < 1 || field.Precision > 38 || field.Scale < 0 || field.Scale > field.Precision) {
                    return iceberg::InvalidArgument("Invalid decimal precision or scale at index {}", index);
                }
            } else if (field.Precision != 0 || field.Scale != 0) {
                return iceberg::InvalidArgument("Only decimal fields accept precision and scale at index {}", index);
            }
            auto type = IcebergType(field);
            if (!type) {
                return iceberg::InvalidArgument("Unsupported field type at index {}", index);
            }
            if (field.Name == "key") {
                if (field.Type != EFieldType::String || field.Nullable) {
                    return iceberg::InvalidArgument("key must be a required string");
                }
                hasKey = true;
            }
            fields.emplace_back(static_cast<std::int32_t>(index + 1), field.Name, std::move(type), field.Nullable);
        }
        auto schema = iceberg::Schema::Make(std::move(fields), 0, std::vector<std::int32_t>{});
        if (!schema) {
            return std::unexpected(std::move(schema.error()));
        }
        TTableProfile profile{std::shared_ptr<iceberg::Schema>(std::move(*schema)),
            {{"commit.retry.num-retries", "0"},
                {"write.metadata.delete-after-commit.enabled", "false"},
                {"write.metadata.compression-codec", "none"},
                {"write.metadata.metrics.default", "truncate(16)"}}};
        if (hasKey) {
            profile.Properties.emplace("write.metadata.metrics.column.key", "full");
        }
        return profile;
    });
}

iceberg::Status CheckTableProfile(const TTableProfile& profile)
{
    return CatchIcebergExceptions([&]() -> iceberg::Status {
        if (!profile.Schema || profile.Schema->fields().size() > MaxSchemaFields) {
            return iceberg::ValidationFailed("Invalid table profile schema.");
        }
        TSchemaSpec spec;
        spec.Fields.reserve(profile.Schema->fields().size());
        for (const auto& field : profile.Schema->fields()) {
            if (!field.type() || !field.doc().empty()) {
                return iceberg::ValidationFailed("Unsupported table profile field.");
            }
            TFieldSpec specField{std::string(field.name()), {}, field.optional()};
            switch (field.type()->type_id()) {
                case iceberg::TypeId::kString:
                    specField.Type = EFieldType::String;
                    break;
                case iceberg::TypeId::kBoolean:
                    specField.Type = EFieldType::Bool;
                    break;
                case iceberg::TypeId::kLong:
                    specField.Type = EFieldType::Int64;
                    break;
                case iceberg::TypeId::kDouble:
                    specField.Type = EFieldType::Float64;
                    break;
                case iceberg::TypeId::kTimestampTz:
                    specField.Type = EFieldType::TimestampUsUtc;
                    break;
                case iceberg::TypeId::kDecimal: {
                    const auto& decimal = static_cast<const iceberg::DecimalType&>(*field.type());
                    specField.Type = EFieldType::Decimal;
                    specField.Precision = decimal.precision();
                    specField.Scale = decimal.scale();
                    break;
                }
                case iceberg::TypeId::kDate:
                    specField.Type = EFieldType::Date;
                    break;
                default:
                    return iceberg::ValidationFailed("Unsupported table profile field type.");
            }
            spec.Fields.push_back(std::move(specField));
        }
        ICEBERG_ASSIGN_OR_RAISE(auto expected, MakeTableProfile(spec));
        auto properties = profile.Properties;
        ICEBERG_RETURN_UNEXPECTED(StripRetentionProperties(properties));
        if (*profile.Schema != *expected.Schema || properties != expected.Properties) {
            return iceberg::ValidationFailed("Table profile differs from the fixed schema and properties.");
        }
        return {};
    });
}

}
