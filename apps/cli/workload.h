#pragma once

#include <CLI/CLI.hpp>
#include <optional>

namespace glamdb::NClient {
void AddWorkloadCommands(CLI::App& app, std::optional<int>& exitCode);
} // namespace glamdb::NClient
