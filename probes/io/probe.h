#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace NGlamDB::NProbe {

struct TOptions
{
    std::string Case;
    std::filesystem::path Root;
    std::filesystem::path ReceiptDirectory;
    std::uint64_t WriteOrdinal = 0;
};

std::string Quote(std::string_view value);
std::vector<std::string> Cases();
int Run(const TOptions& options);

}
