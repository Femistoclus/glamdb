#include "probes/io/probe.h"

#include <algorithm>
#include <charconv>
#include <iostream>
#include <stdexcept>
#include <string_view>

int main(int argc, char** argv)
{
    using namespace NGlamDB::NProbe;
    try {
        if (argc == 2 && std::string_view(argv[1]) == "--list-cases") {
            std::cout << '[';
            bool first = true;
            for (const auto& name : Cases()) {
                std::cout << (first ? "" : ",") << Quote(name);
                first = false;
            }
            std::cout << "]\n";
            return 0;
        }
        TOptions options;
        for (int index = 1; index < argc; index += 2) {
            if (index + 1 >= argc) {
                throw std::invalid_argument("Every option requires a value.");
            }
            const std::string_view flag = argv[index];
            const std::string_view value = argv[index + 1];
            if (flag == "--case") {
                options.Case = value;
            } else if (flag == "--root") {
                options.Root = value;
            } else if (flag == "--receipt-dir") {
                options.ReceiptDirectory = value;
            } else if (flag == "--write-ordinal") {
                const auto parsed = std::from_chars(value.data(), value.data() + value.size(), options.WriteOrdinal);
                if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() || !options.WriteOrdinal) {
                    throw std::invalid_argument("Invalid write ordinal.");
                }
            } else {
                throw std::invalid_argument("Unknown option.");
            }
        }
        const auto cases = Cases();
        if (std::ranges::find(cases, options.Case) == cases.end() || !options.Root.is_absolute() ||
            !options.ReceiptDirectory.is_absolute() || options.Root != options.Root.lexically_normal() ||
            std::filesystem::exists(options.Root) || !std::filesystem::is_directory(options.Root.parent_path()))
        {
            throw std::invalid_argument(
                "Require a known case, absent absolute root with existing parent, and receipt directory.");
        }
        const auto relative = options.ReceiptDirectory.lexically_relative(options.Root);
        if (relative.empty() || *relative.begin() != "..") {
            throw std::invalid_argument("Receipts must be outside the store root.");
        }
        if ((options.Case.ends_with("-footer") || options.Case.ends_with("-trailer")) && !options.WriteOrdinal) {
            throw std::invalid_argument("Footer/trailer cases require a calibrated --write-ordinal.");
        }
        return Run(options);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
}
