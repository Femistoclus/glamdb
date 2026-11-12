#include "core/storage/iceberg_error_boundary.h"
#include "core/storage/table_profile.h"

#include <gtest/gtest.h>
#include <iceberg/exception.h>
#include <iceberg/metrics_config.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>
#include <iceberg/type.h>

#include <array>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace NGlamDB::NStorage {
namespace {

void ExpectInvalid(const TSchemaSpec& spec)
{
    const auto profile = MakeTableProfile(spec);
    ASSERT_FALSE(profile);
    EXPECT_EQ(profile.error().kind, iceberg::ErrorKind::kInvalidArgument);
    EXPECT_FALSE(profile.error().message.empty());
}

TEST(TableProfile, MapsAllTypesAndAssignsIdsInInputOrder)
{
    const TSchemaSpec spec{{{"text", EFieldType::String, true},
        {"enabled", EFieldType::Bool, false},
        {"count", EFieldType::Int64, true},
        {"ratio", EFieldType::Float64, false},
        {"time", EFieldType::TimestampUsUtc, true}}};
    const auto profile = MakeTableProfile(spec);
    ASSERT_TRUE(profile) << profile.error().message;
    EXPECT_TRUE(CheckTableProfile(*profile));
    const auto fields = profile->Schema->fields();
    ASSERT_EQ(fields.size(), spec.Fields.size());
    const std::array expected{iceberg::TypeId::kString,
        iceberg::TypeId::kBoolean,
        iceberg::TypeId::kLong,
        iceberg::TypeId::kDouble,
        iceberg::TypeId::kTimestampTz};
    for (std::size_t index = 0; index < fields.size(); ++index) {
        EXPECT_EQ(fields[index].field_id(), index + 1);
        EXPECT_EQ(fields[index].name(), spec.Fields[index].Name);
        EXPECT_EQ(fields[index].optional(), spec.Fields[index].Nullable);
        EXPECT_EQ(fields[index].type()->type_id(), expected[index]);
    }
    EXPECT_EQ(profile->Schema->schema_id(), 0);
    EXPECT_TRUE(profile->Schema->IdentifierFieldIds().empty());
    EXPECT_TRUE(profile->Schema->Validate(2));
    const auto timestamp = std::dynamic_pointer_cast<iceberg::TimestampBase>(fields[4].type());
    ASSERT_NE(timestamp, nullptr);
    EXPECT_TRUE(timestamp->is_zoned());
    EXPECT_EQ(timestamp->time_unit(), iceberg::TimeUnit::kMicrosecond);
}

TEST(TableProfile, RejectsEmptyOversizedAndUnknownTypeSchemas)
{
    ExpectInvalid({});
    TSchemaSpec spec;
    for (std::size_t index = 0; index < 128; ++index) {
        spec.Fields.push_back({"field" + std::to_string(index), EFieldType::Int64});
    }
    const auto profile = MakeTableProfile(spec);
    ASSERT_TRUE(profile);
    EXPECT_EQ(profile->Schema->fields().back().field_id(), 128);
    spec.Fields.push_back({"field128", EFieldType::Int64});
    ExpectInvalid(spec);
    ExpectInvalid({{{"untyped"}}});
    ExpectInvalid({{{"invalid", static_cast<EFieldType>(255)}}});
}

TEST(TableProfile, RecoversNativeDecimalAndDateParameters)
{
    auto profile = MakeTableProfile({{{"amount", EFieldType::String}, {"day", EFieldType::Int64, true}}});
    ASSERT_TRUE(profile);
    profile->Schema = std::make_shared<iceberg::Schema>(
        std::vector{iceberg::SchemaField::MakeRequired(1, "amount", iceberg::decimal(38, 2)),
            iceberg::SchemaField::MakeOptional(2, "day", iceberg::date())});
    const auto checked = CheckTableProfile(*profile);
    EXPECT_TRUE(checked) << checked.error().message;
}

TEST(TableProfile, CreatesNativeDecimalAndDateWithExactParameters)
{
    const auto profile = MakeTableProfile({{{"amount", EFieldType::Decimal, false, 38, 2},
        {"fraction", EFieldType::Decimal, true, 38, 38},
        {"whole", EFieldType::Decimal, true, 1, 0},
        {"day", EFieldType::Date, true}}});
    ASSERT_TRUE(profile) << profile.error().message;
    ASSERT_TRUE(CheckTableProfile(*profile));
    const auto fields = profile->Schema->fields();
    ASSERT_EQ(fields.size(), 4);
    for (std::size_t index = 0; index < fields.size(); ++index) {
        EXPECT_EQ(fields[index].field_id(), index + 1);
    }
    const auto& decimal = static_cast<const iceberg::DecimalType&>(*fields[0].type());
    EXPECT_EQ(decimal.precision(), 38);
    EXPECT_EQ(decimal.scale(), 2);
    EXPECT_EQ(fields[3].type()->type_id(), iceberg::TypeId::kDate);
}

TEST(TableProfile, RejectsInvalidDecimalParametersAndParametersOnOtherTypes)
{
    for (const auto& [precision, scale] : std::array<std::pair<std::int32_t, std::int32_t>, 7>{
             {{0, 0}, {-1, 0}, {39, 0}, {9, -1}, {9, 10}, {38, 39}, {1, 2}}})
    {
        ExpectInvalid({{{"amount", EFieldType::Decimal, true, precision, scale}}});
    }
    for (const auto type : {EFieldType::String,
             EFieldType::Bool,
             EFieldType::Int64,
             EFieldType::Float64,
             EFieldType::TimestampUsUtc,
             EFieldType::Date})
    {
        ExpectInvalid({{{"value", type, true, 1, 0}}});
        ExpectInvalid({{{"value", type, true, 0, 1}}});
    }
    ExpectInvalid({{{"key", EFieldType::Decimal, false, 9, 2}}});
    ExpectInvalid({{{"key", EFieldType::Date}}});
}

TEST(TableProfile, RejectsUnsupportedRecoveredDecimalScale)
{
    auto profile = MakeTableProfile({{{"amount", EFieldType::String}}});
    ASSERT_TRUE(profile);
    for (const int scale : {-1, 10}) {
        profile->Schema = std::make_shared<iceberg::Schema>(
            std::vector{iceberg::SchemaField::MakeRequired(1, "amount", iceberg::decimal(9, scale))});
        EXPECT_FALSE(CheckTableProfile(*profile));
    }
}

TEST(TableProfile, ValidatesNamesAsExactUtf8Bytes)
{
    const std::string invalidNames[]{"",
        std::string(129, 'a'),
        std::string("a\0b", 3),
        std::string("\xc0\xaf", 2),
        std::string("\xed\xa0\x80", 3),
        std::string("\xf4\x90\x80\x80", 4),
        std::string("\xe2\x82", 2),
        std::string("\x80", 1)};
    for (const auto& name : invalidNames) {
        ExpectInvalid({{{name, EFieldType::String}}});
    }
    ExpectInvalid({{{"same", EFieldType::String}, {"same", EFieldType::Int64}}});
    std::string unicodeName;
    for (int i = 0; i < 64; ++i) {
        unicodeName += "я";
    }
    ASSERT_EQ(unicodeName.size(), 128);
    EXPECT_TRUE(MakeTableProfile({{{unicodeName, EFieldType::String}}}));
    unicodeName += "я";
    ExpectInvalid({{{unicodeName, EFieldType::String}}});
    const TSchemaSpec distinct{{{"name", EFieldType::String},
        {"Name", EFieldType::String},
        {"é", EFieldType::String},
        {"e\xcc\x81", EFieldType::String},
        {std::string(128, 'x'), EFieldType::String}}};
    const auto profile = MakeTableProfile(distinct);
    ASSERT_TRUE(profile);
    for (std::size_t index = 0; index < distinct.Fields.size(); ++index) {
        EXPECT_EQ(profile->Schema->fields()[index].name(), distinct.Fields[index].Name);
    }
}

TEST(TableProfile, RequiresStringKeyWithoutIdentifierOrUniquenessSemantics)
{
    ExpectInvalid({{{"key", EFieldType::String, true}}});
    for (const auto type : {EFieldType::Bool, EFieldType::Int64, EFieldType::Float64, EFieldType::TimestampUsUtc}) {
        ExpectInvalid({{{"key", type}}});
    }
    const auto profile = MakeTableProfile({{{"key", EFieldType::String}}});
    ASSERT_TRUE(profile);
    EXPECT_TRUE(profile->Schema->IdentifierFieldIds().empty());
    EXPECT_FALSE(profile->Schema->fields()[0].optional());
    EXPECT_TRUE(MakeTableProfile({{{"Key", EFieldType::Int64, true}}}));
}

TEST(TableProfile, ProducesOnlyTheFixedProfileAndOptionalFullKeyMetrics)
{
    const std::unordered_map<std::string, std::string> expected{{"commit.retry.num-retries", "0"},
        {"write.metadata.delete-after-commit.enabled", "false"},
        {"write.metadata.compression-codec", "none"},
        {"write.metadata.metrics.default", "truncate(16)"}};
    const auto withoutKey = MakeTableProfile({{{"value", EFieldType::String}}});
    ASSERT_TRUE(withoutKey);
    EXPECT_EQ(withoutKey->Properties, expected);
    const auto withKey = MakeTableProfile({{{"key", EFieldType::String}, {"value", EFieldType::String, true}}});
    ASSERT_TRUE(withKey);
    auto expectedWithKey = expected;
    expectedWithKey.emplace("write.metadata.metrics.column.key", "full");
    EXPECT_EQ(withKey->Properties, expectedWithKey);
    const auto config = iceberg::MetricsConfig::Make(withKey->Properties);
    ASSERT_TRUE(config);
    EXPECT_EQ((*config)->ColumnMode("key").kind, iceberg::MetricsMode::Kind::kFull);
    EXPECT_EQ((*config)->ColumnMode("value").kind, iceberg::MetricsMode::Kind::kTruncate);
    EXPECT_EQ((*config)->ColumnMode("value").TruncateLength(), 16);
    const auto defaultConfig = iceberg::MetricsConfig::Make(withoutKey->Properties);
    ASSERT_TRUE(defaultConfig);
    EXPECT_EQ((*defaultConfig)->ColumnMode("key").kind, iceberg::MetricsMode::Kind::kTruncate);
}

TEST(TableProfile, OwnsItsSchemaAndPropertiesIndependently)
{
    TSchemaSpec spec{{{"key", EFieldType::String}, {"value", EFieldType::Bool, true}}};
    auto first = MakeTableProfile(spec);
    ASSERT_TRUE(first);
    spec.Fields[0].Name = "renamed";
    spec.Fields[1].Nullable = false;
    auto second = MakeTableProfile(spec);
    ASSERT_TRUE(second);
    EXPECT_EQ(first->Schema->fields()[0].name(), "key");
    EXPECT_TRUE(first->Schema->fields()[1].optional());
    first->Properties.clear();
    EXPECT_EQ(second->Schema->fields()[0].name(), "renamed");
    EXPECT_FALSE(second->Schema->fields()[1].optional());
    EXPECT_EQ(second->Properties.size(), 4);
}

TEST(TableProfile, RejectsExtendedMissingAndChangedProperties)
{
    for (const auto& name : {"key", "value"}) {
        const auto canonical = MakeTableProfile({{{name, EFieldType::String}}});
        ASSERT_TRUE(canonical);
        EXPECT_TRUE(CheckTableProfile(*canonical));
        auto extended = *canonical;
        extended.Properties.emplace("application.property", "custom");
        EXPECT_FALSE(CheckTableProfile(extended));
        for (const auto& [key, value] : canonical->Properties) {
            auto missing = *canonical;
            missing.Properties.erase(key);
            EXPECT_FALSE(CheckTableProfile(missing)) << key;
            auto changed = *canonical;
            changed.Properties[key] = value + "-changed";
            EXPECT_FALSE(CheckTableProfile(changed)) << key;
        }
    }
}

TEST(TableProfile, RejectsNonCanonicalSchemaIdentityAndExtensions)
{
    const auto canonical = MakeTableProfile({{{"key", EFieldType::String}, {"value", EFieldType::Int64, true}}});
    ASSERT_TRUE(canonical);
    auto profile = *canonical;
    profile.Schema.reset();
    EXPECT_FALSE(CheckTableProfile(profile));
    const auto fields = canonical->Schema->fields();
    const std::vector<iceberg::SchemaField> original(fields.begin(), fields.end());
    profile.Schema = std::make_shared<iceberg::Schema>(original, 1);
    EXPECT_FALSE(CheckTableProfile(profile));
    auto identifiers = iceberg::Schema::Make(original, 0, std::vector<std::int32_t>{1});
    ASSERT_TRUE(identifiers);
    profile.Schema = std::move(*identifiers);
    EXPECT_FALSE(CheckTableProfile(profile));
    for (const auto& replacement : {iceberg::SchemaField(3, "key", iceberg::string(), false),
             original[0].WithDoc("custom"),
             original[0].WithType(iceberg::int32()),
             original[0].WithType(nullptr),
             original[0].AsOptional()})
    {
        auto changed = original;
        changed[0] = replacement;
        profile.Schema = std::make_shared<iceberg::Schema>(std::move(changed));
        EXPECT_FALSE(CheckTableProfile(profile));
    }
}

TEST(TableProfile, CreatesValidV2MetadataWithTheExactProfile)
{
    const TSchemaSpec spec{{{"key", EFieldType::String}, {"time", EFieldType::TimestampUsUtc, true}}};
    const auto profile = MakeTableProfile(spec);
    ASSERT_TRUE(profile);
    for (const auto& [key, value] : profile->Properties) {
        EXPECT_FALSE(iceberg::TableProperties::reserved_properties().contains(key));
    }
    const auto metadata = iceberg::TableMetadata::Make(*profile->Schema,
        *iceberg::PartitionSpec::Unpartitioned(),
        *iceberg::SortOrder::Unsorted(),
        "store://test/warehouse/table",
        profile->Properties);
    ASSERT_TRUE(metadata) << metadata.error().message;
    EXPECT_EQ((*metadata)->format_version, 2);
    EXPECT_FALSE((*metadata)->table_uuid.empty());
    EXPECT_EQ((*metadata)->properties, iceberg::TableProperties::FromMap(profile->Properties));
    EXPECT_TRUE((*metadata)->snapshots.empty());
    EXPECT_EQ((*metadata)->last_sequence_number, 0);
    const auto schema = (*metadata)->Schema();
    ASSERT_TRUE(schema);
    ASSERT_EQ((*schema)->fields().size(), spec.Fields.size());
    for (std::size_t index = 0; index < spec.Fields.size(); ++index) {
        EXPECT_EQ((*schema)->fields()[index], profile->Schema->fields()[index]);
    }
}

TEST(IcebergErrorBoundary, PreservesValuesAndReportedErrors)
{
    auto value = CatchIcebergExceptions([]() -> iceberg::Result<std::unique_ptr<int>> {
        return std::make_unique<int>(17);
    });
    ASSERT_TRUE(value);
    EXPECT_EQ(**value, 17);
    auto error = CatchIcebergExceptions([]() -> iceberg::Result<int> {
        return iceberg::IOError("original");
    });
    ASSERT_FALSE(error);
    EXPECT_EQ(error.error().kind, iceberg::ErrorKind::kIOError);
    EXPECT_EQ(error.error().message, "original");
    EXPECT_TRUE(CatchIcebergExceptions([]() -> iceberg::Status {
        return {};
    }));
}

TEST(IcebergErrorBoundary, ConvertsStandardAndUnknownExceptions)
{
    auto standard = CatchIcebergExceptions([]() -> iceberg::Result<int> {
        throw iceberg::IcebergError("upstream failure");
    });
    ASSERT_FALSE(standard);
    EXPECT_EQ(standard.error().kind, iceberg::ErrorKind::kUnknownError);
    EXPECT_EQ(standard.error().message, "upstream failure");
    auto unknown = CatchIcebergExceptions([]() -> iceberg::Status {
        throw 42;
    });
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().kind, iceberg::ErrorKind::kUnknownError);
    EXPECT_FALSE(unknown.error().message.empty());
}

}
}
