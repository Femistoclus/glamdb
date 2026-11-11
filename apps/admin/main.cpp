#include "storage/engine.h"

#include <CLI/CLI.hpp>

#include <iostream>
#include <string>

int main(int argc, char** argv)
try {
    CLI::App app{"GlamDB offline store administration"};
    app.require_subcommand(1);
    std::string root;
    std::string id;
    auto* create = app.add_subcommand("create-store", "Create an empty store");
    auto* inspect = app.add_subcommand("inspect-store", "Inspect an existing store");
    for (auto* command : {create, inspect}) {
        command->add_option("--root", root, "Absolute store path")->required();
        command->add_option("--store-id", id, "Store UUID")->required();
    }
    if (argc == 1) {
        std::cout << app.help(argv[0]);
        return 0;
    }
    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        const auto code = app.exit(error);
        return code == 0 ? 0 : 2;
    }
    const auto opened = create->parsed() ? glamdb::CreateStore({id, root}) : glamdb::OpenExisting({root, id});
    if (!opened) {
        std::cerr << opened.error().code << ": " << opened.error().message << '\n';
        return 1;
    }
    const auto& info = (*opened)->GetStoreInfo();
    // StoreInfo contains validated UUID/profile tokens, never arbitrary
    // strings.
    std::cout << "{\"store_id\":\"" << info.store_id << "\",\"profile_id\":\"" << info.profile_id
              << "\",\"format_version\":" << info.format_version << ",\"durability_validation\":\"designed\"}\n";
    const auto closed = (*opened)->Shutdown(std::chrono::milliseconds(5000));
    if (!closed) {
        std::cerr << closed.error().code << ": " << closed.error().message << '\n';
        return 1;
    }
    std::cout.flush();
    if (!std::cout) {
        std::cerr << "output failed; store operation may have completed\n";
        return 1;
    }
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
} catch (...) {
    return 100;
}
