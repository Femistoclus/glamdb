#pragma once

#include "storage/engine.h"

#include <arrow/api.h>

#include <bit>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <spawn.h>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace NHistoryOracle {
inline void Check(bool condition, const char* message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}
inline void HexBytes(std::string& output, std::string_view value)
{
    for (const unsigned char byte : value) {
        output += "0123456789abcdef"[byte >> 4];
        output += "0123456789abcdef"[byte & 15];
    }
}
inline void HexInteger(std::string& output, uint64_t value)
{
    for (int shift = 60; shift >= 0; shift -= 4) {
        output += "0123456789abcdef"[(value >> shift) & 15];
    }
}
// Canonical numeric bits and exact string bytes; independent of core codecs
// and the loader's decimal parser. Hex framing cannot contain a newline.
inline std::string SourceRow(const std::vector<std::string_view>& fields, const glamdb::SchemaSpec& schema)
{
    Check(fields.size() == schema.fields.size(), "oracle column count differs");
    std::string row;
    for (size_t column = 0; column < fields.size(); ++column) {
        const auto value = std::string(fields[column]);
        size_t consumed = 0;
        if (schema.fields[column].type == glamdb::FieldType::Int64) {
            const auto number = std::stoll(value, &consumed);
            Check(consumed == value.size(), "invalid oracle integer");
            HexInteger(row, static_cast<uint64_t>(number));
        } else if (schema.fields[column].type == glamdb::FieldType::Float64) {
            const auto number = std::stod(value, &consumed);
            Check(consumed == value.size(), "invalid oracle floating point value");
            HexInteger(row, std::bit_cast<uint64_t>(number == 0.0 ? 0.0 : number));
        } else {
            HexBytes(row, value);
        }
        row += '|';
    }
    return row;
}
inline std::string ScanRow(const arrow::RecordBatch& batch, int64_t index, const glamdb::SchemaSpec& schema)
{
    std::string row;
    for (size_t column = 0; column < schema.fields.size(); ++column) {
        const auto& array = *batch.column(static_cast<int>(column));
        Check(!array.IsNull(index), "unexpected NULL");
        if (schema.fields[column].type == glamdb::FieldType::Int64) {
            HexInteger(row, static_cast<uint64_t>(static_cast<const arrow::Int64Array&>(array).Value(index)));
        } else if (schema.fields[column].type == glamdb::FieldType::Float64) {
            HexInteger(row, std::bit_cast<uint64_t>(static_cast<const arrow::DoubleArray&>(array).Value(index)));
        } else {
            HexBytes(row, static_cast<const arrow::StringArray&>(array).GetView(index));
        }
        row += '|';
    }
    return row;
}
inline void Sort(const std::filesystem::path& path, const std::filesystem::path& workspace)
{
    std::string file = path.string(), directory = workspace.string();
    std::array<char*, 9> arguments{const_cast<char*>("sort"),
        const_cast<char*>("-S"),
        const_cast<char*>("64M"),
        const_cast<char*>("--parallel=1"),
        const_cast<char*>("-T"),
        directory.data(),
        const_cast<char*>("-o"),
        file.data(),
        nullptr};
    // sort -o FILE reads stdin when no input is supplied. Give it the file
    // explicitly; sorting in place is supported by GNU sort.
    std::vector<char*> argv(arguments.begin(), arguments.end() - 1);
    argv.push_back(file.data());
    argv.push_back(nullptr);
    pid_t child = 0;
    Check(posix_spawnp(&child, "sort", nullptr, nullptr, argv.data(), environ) == 0, "cannot start sort");
    int status = 0;
    pid_t waited = 0;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    Check(waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 0, "oracle sort failed");
}
struct TRowFiles
{
    std::filesystem::path Expected;
    std::filesystem::path Actual;
};
inline void Compare(const TRowFiles& files)
{
    std::ifstream source(files.Expected), scan(files.Actual);
    Check(source.good() && scan.good(), "cannot read sorted oracle");
    std::string left, right;
    while (std::getline(source, left)) {
        Check(static_cast<bool>(std::getline(scan, right)) && left == right, "exact row multiset differs");
    }
    Check(!std::getline(scan, right) && !source.bad() && !scan.bad(), "oracle row count or read differs");
}
} // namespace NHistoryOracle
