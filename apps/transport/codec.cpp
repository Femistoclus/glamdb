#include "apps/transport/codec.h"
#include "apps/transport/json_reader.h"
#include "core/api/canonical.h"
#include "core/api/write_settings.h"
#include "core/scan/predicate.h"
#include "storage/scalars.h"

#include <array>
#include <bit>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <limits>
#include <unordered_set>

namespace glamdb::NTransport {
namespace {
constexpr size_t MaxJsonDepth = 160;
const std::vector<std::string> TypeNames{"string", "bool", "int64", "float64", "timestamp_us_utc", "decimal", "date"};

// A single SAX pass constructs the DOM and rejects decoded duplicate keys.
// Each frame owns its container; array growth cannot invalidate child frames.
class TJsonParser final : public nlohmann::json_sax<TJson>
{
public:
    TJson Result;
    bool null() override
    {
        return Add(nullptr);
    }
    bool boolean(bool value) override
    {
        return Add(value);
    }
    bool number_integer(number_integer_t value) override
    {
        return Add(value);
    }
    bool number_unsigned(number_unsigned_t value) override
    {
        return Add(value);
    }
    bool number_float(number_float_t value, const string_t&) override
    {
        return Add(value);
    }
    bool string(string_t& value) override
    {
        return Add(std::move(value));
    }
    bool binary(binary_t&) override
    {
        return false;
    }
    bool start_object(size_t) override
    {
        return Start(TJson::object());
    }
    bool start_array(size_t) override
    {
        return Start(TJson::array());
    }
    bool key(string_t& value) override
    {
        Visit();
        auto& frame = Stack_.back();
        Require(!frame.Value.contains(value), "duplicate JSON key");
        frame.Value[value] = nullptr;
        frame.Key = std::move(value);
        return true;
    }
    bool end_object() override
    {
        return End();
    }
    bool end_array() override
    {
        return End();
    }
    bool parse_error(size_t, const std::string&, const TJson::exception&) override
    {
        return false;
    }

private:
    struct TFrame
    {
        TJson Value;
        std::string Key;
    };
    std::vector<TFrame> Stack_;
    size_t Nodes_ = 0;
    void Visit()
    {
        // Accommodate all 10,000 rows of the maximum-width schema and
        // depth-64 compound scan predicates.
        Require(Stack_.size() <= MaxJsonDepth && ++Nodes_ <= MaxJsonEvents, "JSON complexity limit exceeded");
    }
    bool Start(TJson value)
    {
        Visit();
        Stack_.push_back({std::move(value), {}});
        return true;
    }
    bool Add(TJson value)
    {
        Visit();
        if (Stack_.empty()) {
            Result = std::move(value);
        } else if (Stack_.back().Value.is_array()) {
            Stack_.back().Value.push_back(std::move(value));
        } else {
            Stack_.back().Value.at(Stack_.back().Key) = std::move(value);
        }
        return true;
    }
    bool End()
    {
        auto value = std::move(Stack_.back().Value);
        Stack_.pop_back();
        return Add(std::move(value));
    }
};

// TJsonParser with the same events, depth/event accounting and duplicate-key
// checks, except that a top-level object's "rows" value is recorded as
// TJsonRows nodes. Keys are interned once; duplicate keys of row objects (the
// elements of a "rows" array) are detected with per-key row stamps.
class TAppendJsonParser final : public nlohmann::json_sax<TJson>
{
public:
    TJson Result;
    explicit TAppendJsonParser(TJsonRows& rows)
        : Rows_(rows)
    { }
    bool null() override
    {
        return Scalar(nullptr, TJsonRows::EKind::Null, 0);
    }
    bool boolean(bool value) override
    {
        return Scalar(value, TJsonRows::EKind::Bool, value ? 1 : 0);
    }
    bool number_integer(number_integer_t value) override
    {
        return Scalar(value, TJsonRows::EKind::Integer, std::bit_cast<uint64_t>(value));
    }
    bool number_unsigned(number_unsigned_t value) override
    {
        return Scalar(value, TJsonRows::EKind::Unsigned, value);
    }
    bool number_float(number_float_t value, const string_t&) override
    {
        return Scalar(value, TJsonRows::EKind::Float, std::bit_cast<uint64_t>(value));
    }
    bool string(string_t& value) override
    {
        if (!Capturing()) {
            return Add(std::move(value));
        }
        Visit();
        Emit({.Payload = Rows_.Strings.size(), .Size = static_cast<uint32_t>(value.size())}, TJsonRows::EKind::String);
        Rows_.Strings += value;
        return true;
    }
    bool binary(binary_t&) override
    {
        return false;
    }
    bool start_object(size_t) override
    {
        return Start(TJson::object(), TJsonRows::EKind::Object);
    }
    bool start_array(size_t) override
    {
        return Start(TJson::array(), TJsonRows::EKind::Array);
    }
    bool key(string_t& value) override
    {
        Visit();
        if (!Capture_.empty()) {
            CaptureKey(value);
            return true;
        }
        auto& frame = Stack_.back();
        Require(!frame.Value.contains(value), "duplicate JSON key");
        frame.Value[value] = nullptr;
        CaptureNext_ = Stack_.size() == 1 && value == "rows";
        frame.Key = std::move(value);
        return true;
    }
    bool end_object() override
    {
        return End();
    }
    bool end_array() override
    {
        return End();
    }
    bool parse_error(size_t, const std::string&, const TJson::exception&) override
    {
        return false;
    }

private:
    static constexpr uint32_t NoRow = 0;
    struct TFrame
    {
        TJson Value;
        std::string Key;
    };
    struct TCaptureFrame
    {
        size_t Node = 0;
        uint32_t Row = NoRow;
        uint32_t Members = 0;
        std::unordered_set<uint32_t> Seen{};
    };
    TJsonRows& Rows_;
    std::vector<TFrame> Stack_;
    std::vector<TCaptureFrame> Capture_;
    std::vector<uint32_t> RowStamps_;
    std::vector<uint32_t> PreviousRowKeys_;
    size_t Nodes_ = 0;
    uint32_t RowSerial_ = 0;
    uint32_t PendingKey_ = 0;
    bool CaptureNext_ = false;

    bool Capturing() const
    {
        return CaptureNext_ || !Capture_.empty();
    }
    void Visit()
    {
        Require(Stack_.size() + Capture_.size() <= MaxJsonDepth && ++Nodes_ <= MaxJsonEvents,
            "JSON complexity limit exceeded");
    }
    void Emit(TJsonRows::TNode node, TJsonRows::EKind kind)
    {
        node.Kind = kind;
        if (Capture_.empty()) {
            CaptureNext_ = false;
            Rows_.Present = true;
        } else {
            auto& parent = Rows_.Nodes[Capture_.back().Node];
            ++parent.Size;
            if (parent.Kind == TJsonRows::EKind::Object) {
                node.Key = PendingKey_;
            }
        }
        Rows_.Nodes.push_back(node);
    }
    template <class T>
    bool Scalar(T value, TJsonRows::EKind kind, uint64_t payload)
    {
        if (!Capturing()) {
            return Add(TJson(value));
        }
        Visit();
        Emit({.Payload = payload}, kind);
        return true;
    }
    bool Start(TJson value, TJsonRows::EKind kind)
    {
        Visit();
        if (!Capturing()) {
            Stack_.push_back({std::move(value), {}});
            return true;
        }
        // Elements of a top-level "rows" array are the row objects.
        const bool row = kind == TJsonRows::EKind::Object && Capture_.size() == 1 &&
            Rows_.Nodes[Capture_.front().Node].Kind == TJsonRows::EKind::Array;
        const auto index = Rows_.Nodes.size();
        Emit({}, kind);
        Capture_.push_back({.Node = index, .Row = row ? ++RowSerial_ : NoRow});
        return true;
    }
    void CaptureKey(string_t& value)
    {
        auto& frame = Capture_.back();
        uint32_t id = 0;
        // Rows usually repeat the previous row's key order; check that before hashing.
        const auto position = frame.Members++;
        if (frame.Row != NoRow && position < PreviousRowKeys_.size() &&
            *Rows_.KeyNames[PreviousRowKeys_[position]] == value)
        {
            id = PreviousRowKeys_[position];
        } else {
            const auto it = Rows_.Keys.find(value);
            if (it != Rows_.Keys.end()) {
                id = it->second;
            } else {
                id = static_cast<uint32_t>(Rows_.KeyNames.size());
                const auto inserted = Rows_.Keys.emplace(std::move(value), id).first;
                Rows_.KeyNames.push_back(&inserted->first);
                RowStamps_.push_back(NoRow);
            }
            if (frame.Row != NoRow) {
                if (position >= PreviousRowKeys_.size()) {
                    PreviousRowKeys_.resize(position + 1);
                }
                PreviousRowKeys_[position] = id;
            }
        }
        if (frame.Row != NoRow) {
            Require(RowStamps_[id] != frame.Row, "duplicate JSON key");
            RowStamps_[id] = frame.Row;
        } else {
            Require(frame.Seen.insert(id).second, "duplicate JSON key");
        }
        PendingKey_ = id;
    }
    bool Add(TJson value)
    {
        Visit();
        if (Stack_.empty()) {
            Result = std::move(value);
        } else if (Stack_.back().Value.is_array()) {
            Stack_.back().Value.push_back(std::move(value));
        } else {
            Stack_.back().Value.at(Stack_.back().Key) = std::move(value);
        }
        return true;
    }
    bool End()
    {
        if (!Capture_.empty()) {
            const auto node = Capture_.back().Node;
            Capture_.pop_back();
            Visit();
            Rows_.Nodes[node].Payload = Rows_.Nodes.size();
            return true;
        }
        auto value = std::move(Stack_.back().Value);
        Stack_.pop_back();
        return Add(std::move(value));
    }
};

int64_t ParseTimestampText(std::string_view text)
{
    Require(text.size() >= 20 && text.size() <= 32 && text[4] == '-' && text[7] == '-' && text[10] == 'T' &&
            text[13] == ':' && text[16] == ':',
        "invalid UTC microsecond timestamp");
    const auto number = [&](size_t start, size_t count) {
        int result = 0;
        Require(start + count <= text.size(), "invalid timestamp digits");
        for (size_t i = start; i < start + count; ++i) {
            Require(text[i] >= '0' && text[i] <= '9', "invalid timestamp digits");
            result = result * 10 + text[i] - '0';
        }
        return result;
    };
    const auto year = number(0, 4), hour = number(11, 2), minute = number(14, 2), second = number(17, 2);
    const auto date = std::chrono::year(year) / number(5, 2) / number(8, 2);
    Require(date.ok() && year >= 1 && hour < 24 && minute < 60 && second < 60, "invalid timestamp date/time");
    size_t position = 19;
    int fraction = 0;
    if (text[position] == '.') {
        const auto start = ++position;
        while (position < text.size() && text[position] >= '0' && text[position] <= '9') {
            ++position;
        }
        Require(position > start && position - start <= 6, "invalid timestamp fraction");
        fraction = number(start, position - start);
        for (size_t i = position - start; i < 6; ++i) {
            fraction *= 10;
        }
    }
    int offset = 0;
    if (position < text.size() && text[position] == 'Z') {
        Require(position + 1 == text.size(), "invalid timestamp zone");
    } else {
        Require(position + 6 == text.size() && (text[position] == '+' || text[position] == '-') &&
                text[position + 3] == ':',
            "invalid timestamp zone");
        const auto hours = number(position + 1, 2), minutes = number(position + 4, 2);
        Require(hours < 24 && minutes < 60, "invalid timestamp offset");
        offset = (hours * 60 + minutes) * (text[position] == '+' ? 1 : -1);
    }
    const auto instant = std::chrono::sys_days(date) + std::chrono::hours(hour) +
        std::chrono::minutes(minute - offset) + std::chrono::seconds(second) + std::chrono::microseconds(fraction);
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(instant.time_since_epoch()).count();
    Require(micros >= -62135596800000000LL && micros <= 253402300799999999LL, "timestamp outside years 0001..9999");
    return micros;
}

int64_t ParseTimestamp(const TJson& value)
{
    Require(value.is_string(), "timestamp must be an RFC3339 string");
    return ParseTimestampText(value.get_ref<const std::string&>());
}

int64_t ParseInt64Text(std::string_view text)
{
    int64_t result = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
    Require(!text.empty() && parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size(), "invalid int64");
    return result;
}

std::shared_ptr<arrow::DataType> ArrowType(const FieldSpec& field)
{
    switch (field.type) {
        case FieldType::String:
            return arrow::utf8();
        case FieldType::Bool:
            return arrow::boolean();
        case FieldType::Int64:
            return arrow::int64();
        case FieldType::Float64:
            return arrow::float64();
        case FieldType::TimestampUsUtc:
            return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
        case FieldType::Decimal:
            return arrow::decimal128(field.precision, field.scale);
        case FieldType::Date:
            return arrow::date32();
    }
    throw TFailure({"INVALID_ARGUMENT", "unsupported field type"});
}

Decimal DecimalValue(const FieldSpec& field, const TJson& value)
{
    Require(value.is_string(), "decimal values require decimal strings");
    return Unwrap(ParseDecimal(value.get_ref<const std::string&>(), field.precision, field.scale));
}

Date DateValue(const TJson& value)
{
    Require(value.is_string(), "date values require YYYY-MM-DD strings");
    return Unwrap(ParseDate(value.get_ref<const std::string&>()));
}

std::shared_ptr<arrow::Scalar> ParseScalar(const FieldSpec& field, const TJson& value)
{
    switch (field.type) {
        case FieldType::String:
            Require(value.is_string(), "expected string");
            return std::make_shared<arrow::StringScalar>(value.get<std::string>());
        case FieldType::Bool:
            Require(value.is_boolean(), "expected boolean");
            return std::make_shared<arrow::BooleanScalar>(value.get<bool>());
        case FieldType::Int64:
            return std::make_shared<arrow::Int64Scalar>(ParseInt64(value));
        case FieldType::Float64:
            Require(value.is_number() && std::isfinite(value.get<double>()), "expected finite number");
            return std::make_shared<arrow::DoubleScalar>(value.get<double>());
        case FieldType::TimestampUsUtc:
            return std::make_shared<arrow::TimestampScalar>(ParseTimestamp(value), ArrowType(field));
        case FieldType::Decimal: {
            const auto decimal = DecimalValue(field, value);
            return std::make_shared<arrow::Decimal128Scalar>(
                arrow::Decimal128(decimal.coefficient_high, decimal.coefficient_low),
                ArrowType(field));
        }
        case FieldType::Date:
            return std::make_shared<arrow::Date32Scalar>(DateValue(value).days_since_epoch);
    }
    throw TFailure({"INVALID_ARGUMENT", "unsupported scalar type"});
}

OptionalLiteral ParseOptionalLiteral(const FieldSpec& field, const TJson& value)
{
    if (value.is_null()) {
        return std::nullopt;
    }
    if (field.type == FieldType::Decimal || field.type == FieldType::Date) {
        if (field.type == FieldType::Decimal) {
            CheckKeys(value, {"type", "value", "precision", "scale"});
            Require(value.contains("type") && value["type"] == "decimal" && value.contains("value") &&
                    value.contains("precision") && value["precision"].is_number_integer() &&
                    value["precision"] == field.precision && value.contains("scale") &&
                    value["scale"].is_number_integer() && value["scale"] == field.scale,
                "decimal literal parameters must match its field");
            return value["value"].is_null() ? OptionalLiteral{} : OptionalLiteral{DecimalValue(field, value["value"])};
        }
        CheckKeys(value, {"type", "value"});
        Require(value.contains("type") && value["type"] == "date" && value.contains("value"),
            "date literal requires type and value");
        return value["value"].is_null() ? OptionalLiteral{} : OptionalLiteral{DateValue(value["value"])};
    }
    const auto scalar = ParseScalar(field, value);
    switch (field.type) {
        case FieldType::String:
            return Literal{scalar->ToString()};
        case FieldType::Bool:
            return Literal{static_cast<const arrow::BooleanScalar&>(*scalar).value};
        case FieldType::Int64:
            return Literal{static_cast<const arrow::Int64Scalar&>(*scalar).value};
        case FieldType::Float64: {
            const auto number = static_cast<const arrow::DoubleScalar&>(*scalar).value;
            return Literal{number == 0.0 ? 0.0 : number};
        }
        case FieldType::TimestampUsUtc:
            return Literal{TimestampUs{static_cast<const arrow::TimestampScalar&>(*scalar).value}};
        case FieldType::Decimal:
        case FieldType::Date:
            break;
    }
    throw TFailure({"INVALID_ARGUMENT", "unsupported literal type"});
}

const FieldSpec& FindPredicateField(const SchemaSpec& schema, std::string_view name)
{
    const auto it = std::ranges::find_if(schema.fields, [&](const auto& field) {
        return field.name == name;
    });
    Require(it != schema.fields.end(), "unknown predicate field");
    return *it;
}

Predicate ParsePredicateTree(const SchemaSpec& schema, const TJson& root)
{
    struct TParseFrame
    {
        const TJson* Json = nullptr;
        size_t Depth = 0;
        Predicate::Kind CombineKind = Predicate::Kind::True;
        size_t ChildCount = 0;
        bool Combine = false;
    };

    std::vector<TParseFrame> parseStack;
    std::vector<Predicate> valueStack;
    parseStack.push_back(TParseFrame{.Json = &root, .Depth = 0, .Combine = false});

    size_t nodeCount = 1;
    size_t literalCount = 0;

    while (!parseStack.empty()) {
        const auto frame = parseStack.back();
        parseStack.pop_back();

        if (frame.Combine) {
            Require(valueStack.size() >= frame.ChildCount, "invalid predicate structure");
            if (frame.CombineKind == Predicate::Kind::Not) {
                auto arg = std::move(valueStack.back());
                valueStack.pop_back();
                valueStack.push_back(Predicate::Not(std::move(arg)));
            } else {
                const auto start = valueStack.size() - frame.ChildCount;
                std::vector<Predicate> args;
                args.reserve(frame.ChildCount);
                for (size_t i = start; i < valueStack.size(); ++i) {
                    args.push_back(std::move(valueStack[i]));
                }
                valueStack.erase(valueStack.begin() + static_cast<std::ptrdiff_t>(start), valueStack.end());
                valueStack.push_back(frame.CombineKind == Predicate::Kind::And ? Predicate::And(std::move(args))
                                                                               : Predicate::Or(std::move(args)));
            }
            continue;
        }

        const auto& node = *frame.Json;
        Require(node.is_object(), "predicate must be an object");
        Require(frame.Depth <= detail::MaxPredicateDepth, "predicate exceeds maximum depth");
        Require(node.contains("op") && node["op"].is_string(), "predicate op must be a string");
        const auto& op = node["op"].get_ref<const std::string&>();

        if (op == "true") {
            CheckKeys(node, {"op"});
            valueStack.push_back(Predicate::True());
            continue;
        }
        if (op == "and" || op == "or") {
            CheckKeys(node, {"op", "args"});
            Require(node.contains("args") && node["args"].is_array() && !node["args"].empty(),
                "and/or predicate requires a non-empty args array");
            const auto& args = node["args"];
            Require(args.size() <= detail::MaxPredicateNodes - nodeCount, "predicate exceeds maximum node count");
            nodeCount += args.size();
            parseStack.push_back(TParseFrame{
                .CombineKind = op == "and" ? Predicate::Kind::And : Predicate::Kind::Or,
                .ChildCount = args.size(),
                .Combine = true,
            });
            for (size_t i = args.size(); i > 0; --i) {
                parseStack.push_back(TParseFrame{
                    .Json = &args[i - 1],
                    .Depth = frame.Depth + 1,
                    .Combine = false,
                });
            }
            continue;
        }
        if (op == "not") {
            CheckKeys(node, {"op", "arg"});
            Require(node.contains("arg"), "not predicate requires arg");
            Require(nodeCount < detail::MaxPredicateNodes, "predicate exceeds maximum node count");
            ++nodeCount;
            parseStack.push_back(TParseFrame{
                .CombineKind = Predicate::Kind::Not,
                .ChildCount = 1,
                .Combine = true,
            });
            parseStack.push_back(TParseFrame{
                .Json = &node["arg"],
                .Depth = frame.Depth + 1,
                .Combine = false,
            });
            continue;
        }
        if (op == "is_null" || op == "is_not_null") {
            CheckKeys(node, {"op", "field"});
            Require(node.contains("field") && node["field"].is_string(), "predicate field must be a string");
            const auto& name = node["field"].get_ref<const std::string&>();
            (void)FindPredicateField(schema, name);
            valueStack.push_back(op == "is_null" ? Predicate::IsNull(name) : Predicate::IsNotNull(name));
            continue;
        }
        if (op == "in") {
            CheckKeys(node, {"op", "field", "values"});
            Require(node.contains("field") && node["field"].is_string(), "predicate field must be a string");
            Require(node.contains("values") && node["values"].is_array(), "in predicate requires a values array");
            const auto& name = node["field"].get_ref<const std::string&>();
            const auto& field = FindPredicateField(schema, name);
            const auto& rawValues = node["values"];
            Require(rawValues.size() <= detail::MaxPredicateLiterals - literalCount,
                "predicate exceeds maximum literal count");
            literalCount += rawValues.size();
            std::vector<OptionalLiteral> values;
            values.reserve(rawValues.size());
            for (const auto& item : rawValues) {
                values.push_back(ParseOptionalLiteral(field, item));
            }
            valueStack.push_back(Predicate::In(name, std::move(values)));
            continue;
        }
        const std::array<std::string_view, 6> comparisons{"eq", "ne", "lt", "le", "gt", "ge"};
        const auto opIt = std::ranges::find(comparisons, op);
        Require(opIt != comparisons.end(), "unsupported predicate operator", "UNSUPPORTED_OPERATION");
        CheckKeys(node, {"op", "field", "value"});
        Require(node.contains("field") && node["field"].is_string(), "predicate field must be a string");
        Require(node.contains("value"), "comparison predicate requires value");
        Require(literalCount < detail::MaxPredicateLiterals, "predicate exceeds maximum literal count");
        ++literalCount;
        const auto& name = node["field"].get_ref<const std::string&>();
        const auto& field = FindPredicateField(schema, name);
        const auto cmpOp = static_cast<CompareOp>(opIt - comparisons.begin());
        auto literal = ParseOptionalLiteral(field, node["value"]);
        valueStack.push_back(
            literal ? Predicate::Compare(name, cmpOp, std::move(*literal)) : Predicate::CompareNull(name, cmpOp));
    }

    Require(valueStack.size() == 1, "invalid predicate structure");
    return std::move(valueStack.back());
}

void CheckArrow(const arrow::Status& status)
{
    if (!status.ok()) [[unlikely]] {
        Fail(status.ToString(), "RESOURCE_EXHAUSTED");
    }
}
} // namespace

void Fail(std::string_view message, std::string_view code)
{
    throw TFailure({std::string(code), detail::BoundedDiagnostic(message), ErrorPolicy(code).Hint});
}

TJson ParseJson(std::string_view bytes)
{
    Require(bytes.size() <= MaxRequestBytes, "request is too large", "PAYLOAD_TOO_LARGE");
    TJsonParser parser;
    Require(TJson::sax_parse(bytes, &parser), "invalid JSON request");
    return std::move(parser.Result);
}

TJson ParseJson(std::string_view bytes, TJsonRows& rows)
{
    Require(bytes.size() <= MaxRequestBytes, "request is too large", "PAYLOAD_TOO_LARGE");
    rows = {};
    TAppendJsonParser parser(rows);
    Require(TJsonReader(bytes).Parse(parser), "invalid JSON request");
    return std::move(parser.Result);
}

void CheckKeys(const TJson& value, std::initializer_list<std::string_view> keys)
{
    Require(value.is_object(), "expected JSON object");
    for (const auto& [key, ignored] : value.items()) {
        (void)ignored;
        Require(std::find(keys.begin(), keys.end(), key) != keys.end(), "unknown request field");
    }
}

int64_t ParseInt64(const TJson& value)
{
    Require(value.is_string(), "int64 and snapshot IDs require decimal strings");
    return ParseInt64Text(value.get_ref<const std::string&>());
}

SchemaSpec ParseSchema(const TJson& value)
{
    CheckKeys(value, {"fields"});
    const auto& fields = value.at("fields");
    Require(fields.is_array() && !fields.empty() && fields.size() <= MaxSchemaFields, "invalid schema fields");
    SchemaSpec result;
    for (const auto& field : fields) {
        CheckKeys(field, {"name", "type", "nullable", "field_id", "precision", "scale"});
        const auto name = field.at("type").get<std::string>();
        const auto typeIt = std::find(TypeNames.begin(), TypeNames.end(), name);
        Require(typeIt != TypeNames.end(), "unsupported field type");
        if (field.contains("field_id")) {
            Require(field["field_id"].is_number_integer() && field["field_id"] == result.fields.size() + 1,
                "field IDs must follow declaration order");
        }
        FieldSpec spec{field.at("name").get<std::string>(),
            static_cast<FieldType>(1 + typeIt - TypeNames.begin()),
            field.value("nullable", false)};
        if (spec.type == FieldType::Decimal) {
            Require(field.contains("precision") && field["precision"].is_number_integer() && field["precision"] >= 1 &&
                    field["precision"] <= 38 && field.contains("scale") && field["scale"].is_number_integer() &&
                    field["scale"] >= 0 && field["scale"] <= field["precision"],
                "decimal requires integer precision 1..38 and scale 0..precision");
            spec.precision = field["precision"].get<int32_t>();
            spec.scale = field["scale"].get<int32_t>();
        } else {
            Require(!field.contains("precision") && !field.contains("scale"),
                "precision and scale are only valid for decimal fields");
        }
        result.fields.push_back(std::move(spec));
    }
    return result;
}

namespace {
ParquetCompression ParseCompression(const TJson& value)
{
    CheckKeys(value, {"codec", "level"});
    Require(value.contains("codec") && value["codec"].is_string(), "Parquet compression requires a codec name");
    const auto codec = detail::ParseCodecName(value["codec"].get<std::string>());
    Require(codec.has_value(), "unknown Parquet compression codec");
    ParquetCompression result{*codec, std::nullopt};
    if (value.contains("level") && !value["level"].is_null()) {
        const auto& level = value["level"];
        Require(level.is_number_integer() && level.get<int64_t>() >= std::numeric_limits<int32_t>::min() &&
                level.get<int64_t>() <= std::numeric_limits<int32_t>::max(),
            "Parquet compression level must be an integer");
        result.level = static_cast<int32_t>(level.get<int64_t>());
    }
    return result;
}
std::optional<DictionaryPolicy> ParsePolicy(const TJson& value)
{
    if (!value.contains("dictionary") || value["dictionary"].is_null()) {
        return std::nullopt;
    }
    Require(value["dictionary"].is_string(), "dictionary policy requires a name");
    const auto policy = detail::ParsePolicyName(value["dictionary"].get<std::string>());
    Require(policy.has_value(), "unknown dictionary policy");
    return policy;
}
} // namespace

WriteSettings ParseWriteSettings(const TJson& value)
{
    CheckKeys(value, {"parquet"});
    WriteSettings result;
    if (!value.contains("parquet") || value["parquet"].is_null()) {
        return result;
    }
    const auto& parquet = value["parquet"];
    CheckKeys(parquet, {"compression", "dictionary", "dictionary_page_size_limit", "columns"});
    if (parquet.contains("compression") && !parquet["compression"].is_null()) {
        result.compression = ParseCompression(parquet["compression"]);
    }
    result.dictionary = ParsePolicy(parquet);
    if (parquet.contains("dictionary_page_size_limit") && !parquet["dictionary_page_size_limit"].is_null()) {
        result.dictionary_page_size_limit = ParseInt64(parquet["dictionary_page_size_limit"]);
    }
    if (parquet.contains("columns") && !parquet["columns"].is_null()) {
        const auto& columns = parquet["columns"];
        Require(columns.is_array() && columns.size() <= MaxSchemaFields,
            "write settings columns require an array of 0..128 items");
        for (const auto& column : columns) {
            CheckKeys(column, {"field", "compression", "dictionary", "encoding"});
            Require(column.contains("field") && column["field"].is_string(), "column write settings require a field");
            ColumnWriteSettings item{column["field"].get<std::string>(), std::nullopt};
            if (column.contains("compression") && !column["compression"].is_null()) {
                item.compression = ParseCompression(column["compression"]);
            }
            item.dictionary = ParsePolicy(column);
            if (column.contains("encoding") && !column["encoding"].is_null()) {
                Require(column["encoding"].is_string(), "Parquet encoding requires a name");
                item.encoding = detail::ParseEncodingName(column["encoding"].get<std::string>());
                Require(item.encoding.has_value(), "unknown Parquet encoding");
            }
            result.columns.push_back(std::move(item));
        }
    }
    return result;
}

std::shared_ptr<arrow::RecordBatch> ParseRows(const SchemaSpec& schema, const TJson& rows)
{
    Require(rows.is_array() && !rows.empty() && rows.size() <= 10000, "append requires 1..10000 rows");
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    arrow::FieldVector fields;
    for (const auto& field : schema.fields) {
        const auto type = ArrowType(field);
        fields.push_back(arrow::field(field.name, type, field.nullable));
        std::unique_ptr<arrow::ArrayBuilder> builder;
        CheckArrow(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder));
        CheckArrow(builder->Reserve(static_cast<int64_t>(rows.size())));
        builders.push_back(std::move(builder));
    }
    for (const auto& row : rows) {
        Require(row.is_object(), "row must be an object");
        size_t matched = 0;
        for (size_t i = 0; i < schema.fields.size(); ++i) {
            const auto& field = schema.fields[i];
            const auto value = row.find(field.name);
            matched += value != row.end();
            if (value == row.end() || value->is_null()) {
                Require(field.nullable, "missing or null required field");
                CheckArrow(builders[i]->AppendNull());
            } else {
                auto& builder = *builders[i];
                switch (field.type) {
                    case FieldType::String:
                        Require(value->is_string(), "expected string");
                        CheckArrow(
                            static_cast<arrow::StringBuilder&>(builder).Append(value->get_ref<const std::string&>()));
                        break;
                    case FieldType::Bool:
                        Require(value->is_boolean(), "expected boolean");
                        CheckArrow(static_cast<arrow::BooleanBuilder&>(builder).Append(value->get<bool>()));
                        break;
                    case FieldType::Int64:
                        CheckArrow(static_cast<arrow::Int64Builder&>(builder).Append(ParseInt64(*value)));
                        break;
                    case FieldType::Float64:
                        Require(value->is_number() && std::isfinite(value->get<double>()), "expected finite number");
                        CheckArrow(static_cast<arrow::DoubleBuilder&>(builder).Append(value->get<double>()));
                        break;
                    case FieldType::TimestampUsUtc:
                        CheckArrow(static_cast<arrow::TimestampBuilder&>(builder).Append(ParseTimestamp(*value)));
                        break;
                    case FieldType::Decimal: {
                        const auto decimal = DecimalValue(field, *value);
                        CheckArrow(static_cast<arrow::Decimal128Builder&>(builder).Append(
                            arrow::Decimal128(decimal.coefficient_high, decimal.coefficient_low)));
                        break;
                    }
                    case FieldType::Date:
                        CheckArrow(
                            static_cast<arrow::Date32Builder&>(builder).Append(DateValue(*value).days_since_epoch));
                        break;
                }
            }
        }
        Require(matched == row.size(), "unknown row field");
    }
    arrow::ArrayVector arrays;
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        CheckArrow(builder->Finish(&array));
        arrays.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), static_cast<int64_t>(rows.size()), arrays);
}

std::shared_ptr<arrow::RecordBatch> ParseRows(const SchemaSpec& schema, const TJsonRows& rows)
{
    using EKind = TJsonRows::EKind;
    const auto& nodes = rows.Nodes;
    const bool isArray = rows.Present && nodes.front().Kind == EKind::Array;
    const size_t count = isArray ? nodes.front().Size : 0;
    Require(isArray && count != 0 && count <= 10000, "append requires 1..10000 rows");
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    arrow::FieldVector fields;
    constexpr auto NoKey = std::numeric_limits<uint32_t>::max();
    std::vector<uint32_t> fieldKeys;
    for (const auto& field : schema.fields) {
        const auto type = ArrowType(field);
        fields.push_back(arrow::field(field.name, type, field.nullable));
        std::unique_ptr<arrow::ArrayBuilder> builder;
        CheckArrow(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder));
        CheckArrow(builder->Reserve(static_cast<int64_t>(count)));
        builders.push_back(std::move(builder));
        const auto key = rows.Keys.find(field.name);
        fieldKeys.push_back(key == rows.Keys.end() ? NoKey : key->second);
    }
    const auto next = [&](size_t index) {
        const auto kind = nodes[index].Kind;
        return kind == EKind::Array || kind == EKind::Object ? static_cast<size_t>(nodes[index].Payload) : index + 1;
    };
    // Member node of each key in the current row; a stale stamp means the key is absent.
    std::vector<size_t> members(rows.KeyNames.size());
    std::vector<uint32_t> stamps(rows.KeyNames.size(), 0);
    size_t index = 1;
    for (uint32_t row = 1; row <= count; ++row) {
        const auto& item = nodes[index];
        Require(item.Kind == EKind::Object, "row must be an object");
        const auto end = static_cast<size_t>(item.Payload);
        for (auto member = index + 1; member < end; member = next(member)) {
            members[nodes[member].Key] = member;
            stamps[nodes[member].Key] = row;
        }
        size_t matched = 0;
        for (size_t i = 0; i < schema.fields.size(); ++i) {
            const auto& field = schema.fields[i];
            const auto key = fieldKeys[i];
            const bool present = key != NoKey && stamps[key] == row;
            matched += present;
            const auto* value = present ? &nodes[members[key]] : nullptr;
            if (value == nullptr || value->Kind == EKind::Null) {
                Require(field.nullable, "missing or null required field");
                CheckArrow(builders[i]->AppendNull());
                continue;
            }
            const auto text = [&] {
                return std::string_view(rows.Strings).substr(static_cast<size_t>(value->Payload), value->Size);
            };
            auto& builder = *builders[i];
            switch (field.type) {
                case FieldType::String:
                    Require(value->Kind == EKind::String, "expected string");
                    CheckArrow(static_cast<arrow::StringBuilder&>(builder).Append(text()));
                    break;
                case FieldType::Bool:
                    Require(value->Kind == EKind::Bool, "expected boolean");
                    CheckArrow(static_cast<arrow::BooleanBuilder&>(builder).Append(value->Payload != 0));
                    break;
                case FieldType::Int64:
                    Require(value->Kind == EKind::String, "int64 and snapshot IDs require decimal strings");
                    CheckArrow(static_cast<arrow::Int64Builder&>(builder).Append(ParseInt64Text(text())));
                    break;
                case FieldType::Float64: {
                    // Same conversions as TJson::get<double>().
                    double number = std::numeric_limits<double>::quiet_NaN();
                    if (value->Kind == EKind::Float) {
                        number = std::bit_cast<double>(value->Payload);
                    } else if (value->Kind == EKind::Integer) {
                        number = static_cast<double>(std::bit_cast<int64_t>(value->Payload));
                    } else if (value->Kind == EKind::Unsigned) {
                        number = static_cast<double>(value->Payload);
                    }
                    Require(std::isfinite(number), "expected finite number");
                    CheckArrow(static_cast<arrow::DoubleBuilder&>(builder).Append(number));
                    break;
                }
                case FieldType::TimestampUsUtc:
                    Require(value->Kind == EKind::String, "timestamp must be an RFC3339 string");
                    CheckArrow(static_cast<arrow::TimestampBuilder&>(builder).Append(ParseTimestampText(text())));
                    break;
                case FieldType::Decimal: {
                    Require(value->Kind == EKind::String, "decimal values require decimal strings");
                    const auto decimal = Unwrap(ParseDecimal(text(), field.precision, field.scale));
                    CheckArrow(static_cast<arrow::Decimal128Builder&>(builder).Append(
                        arrow::Decimal128(decimal.coefficient_high, decimal.coefficient_low)));
                    break;
                }
                case FieldType::Date:
                    Require(value->Kind == EKind::String, "date values require YYYY-MM-DD strings");
                    CheckArrow(
                        static_cast<arrow::Date32Builder&>(builder).Append(Unwrap(ParseDate(text())).days_since_epoch));
                    break;
            }
        }
        Require(matched == item.Size, "unknown row field");
        index = end;
    }
    arrow::ArrayVector arrays;
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        CheckArrow(builder->Finish(&array));
        arrays.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), static_cast<int64_t>(count), arrays);
}

ScanRequest ParseScan(std::string collectionID, const SchemaSpec& schema, const TJson& body)
{
    CheckKeys(body, {"snapshot_id", "projection", "predicate", "limit", "batch_rows"});
    ScanRequest result;
    result.collection_id = std::move(collectionID);
    result.batch_rows = 1024;
    if (body.contains("snapshot_id")) {
        const auto& value = body["snapshot_id"];
        result.selector = value.is_null() || value == "initial" ? SnapshotSelector::InitialEmpty()
                                                                : SnapshotSelector::Id(ParseInt64(value));
    }
    if (body.contains("projection") && !body["projection"].is_null()) {
        const auto& rawProjection = body["projection"];
        Require(rawProjection.is_array(), "projection must be an array of field names");
        std::vector<std::string> fields;
        fields.reserve(rawProjection.size());
        for (const auto& item : rawProjection) {
            Require(item.is_string(), "projection field name must be a string");
            const auto& name = item.get_ref<const std::string&>();
            (void)FindPredicateField(schema, name);
            Require(std::ranges::find(fields, name) == fields.end(), "projection repeats a field");
            fields.push_back(name);
        }
        result.projection = std::move(fields);
    }
    for (const auto key : {"limit", "batch_rows"}) {
        if (!body.contains(key)) {
            continue;
        }
        Require(body[key].is_number_integer() && body[key] >= 0 && body[key] <= std::numeric_limits<int64_t>::max(),
            "invalid scan bound");
        const auto value = body[key].get<int64_t>();
        if (std::string_view(key) == "limit") {
            result.limit = value;
        } else {
            Require(value >= 1 && value <= 65536, "batch_rows must be 1..65536");
            result.batch_rows = value;
        }
    }
    if (!body.contains("predicate") || body["predicate"].is_null()) {
        return result;
    }
    result.predicate = ParsePredicateTree(schema, body["predicate"]);
    const auto bound = detail::BindPredicate(result.predicate, schema);
    if (!bound) {
        throw TFailure(bound.error());
    }
    return result;
}

TJson EncodeSchema(const arrow::Schema& schema)
{
    auto fields = TJson::array();
    for (const auto& field : schema.fields()) {
        std::string type;
        switch (field->type()->id()) {
            case arrow::Type::STRING:
                type = "string";
                break;
            case arrow::Type::BOOL:
                type = "bool";
                break;
            case arrow::Type::INT64:
                type = "int64";
                break;
            case arrow::Type::DOUBLE:
                type = "float64";
                break;
            case arrow::Type::TIMESTAMP:
                type = "timestamp_us_utc";
                break;
            case arrow::Type::DECIMAL128:
                type = "decimal";
                break;
            case arrow::Type::DATE32:
                type = "date";
                break;
            default:
                throw TFailure({"INTERNAL", "unsupported output type", RetryHint::OperatorAction});
        }
        TJson item{{"name", field->name()}, {"type", type}, {"nullable", field->nullable()}};
        if (field->type()->id() == arrow::Type::DECIMAL128) {
            const auto& decimal = static_cast<const arrow::Decimal128Type&>(*field->type());
            item["precision"] = decimal.precision();
            item["scale"] = decimal.scale();
        }
        if (field->metadata()) {
            const auto id = field->metadata()->Get("PARQUET:field_id");
            if (id.ok()) {
                item["field_id"] = std::stoi(*id);
            }
        }
        fields.push_back(std::move(item));
    }
    return {{"fields", fields}};
}

TJson EncodeIdentity(const BaseIdentity& identity)
{
    return {{"store_id", identity.store_id},
        {"collection_id", identity.collection_id},
        {"table_uuid", identity.table_uuid},
        {"metadata_location", identity.metadata_location},
        {"current_snapshot_id",
            identity.current_snapshot_id ? TJson(std::to_string(*identity.current_snapshot_id)) : TJson(nullptr)},
        {"schema_id", identity.schema_id},
        {"spec_id", identity.spec_id}};
}

TJson EncodeError(const ErrorDetail& error)
{
    const std::vector<std::string> hints{"SameIdAfterReconcile", "NewOperationId", "FixInput", "OperatorAction"};
    return {{"error",
        {{"code", error.code},
            {"message", detail::BoundedDiagnostic(error.message)},
            {"retry_hint", hints.at(static_cast<size_t>(error.retry_hint))}}}};
}

TJson EncodeHealth(const StoreHealth& health)
{
    const std::array
        names{"recovering", "ready", "recovery_required", "storage_failed", "unavailable", "stopping", "closed"};
    return {{"state", names.at(static_cast<size_t>(health.state))},
        {"read_available", health.read_available},
        {"write_ready", health.write_ready},
        {"storage_failure_latched", health.storage_failure_latched},
        {"durability_qualified", health.durability_qualified},
        {"backend", "local"},
        {"mock", false},
        {"durability_validation", "designed"}};
}

int ErrorStatus(std::string_view code)
{
    return ErrorPolicy(code).Status;
}
} // namespace glamdb::NTransport
