#include "storage/engine.h"

#include <iostream>
#include <string_view>

namespace {
void Usage() {
    std::cout << "GlamDB catalog MVP\n"
                 "  glamdb create-store --root ABSOLUTE_PATH --store-id UUID\n"
                 "  glamdb inspect-store --root ABSOLUTE_PATH --store-id UUID\n";
}
} // namespace

int main(int argc, char **argv) try {
    if (argc == 1 || (argc == 2 && std::string_view(argv[1]) == "--help")) {
        Usage();
        return 0;
    }
    const std::string_view command = argv[1];
    if (argc != 6 || (command != "create-store" && command != "inspect-store")) {
        Usage();
        return 2;
    }
    std::string root;
    std::string id;
    for (int i = 2; i < argc; i += 2) {
        const std::string_view option = argv[i];
        if (option == "--root" && root.empty()) {
            root = argv[i + 1];
        } else if (option == "--store-id" && id.empty()) {
            id = argv[i + 1];
        } else {
            Usage();
            return 2;
        }
    }
    const auto opened = command == "create-store" ? glamdb::CreateStore({id, root}) : glamdb::OpenExisting({root, id});
    if (!opened) {
        std::cerr << opened.error().code << ": " << opened.error().message << '\n';
        return 1;
    }
    const auto &info = (*opened)->GetStoreInfo();
    // StoreInfo contains validated UUID/profile tokens, never arbitrary
    // strings.
    std::cout << "{\"store_id\":\"" << info.store_id << "\",\"profile_id\":\"" << info.profile_id
              << "\",\"format_version\":" << info.format_version << ",\"durability_validation\":\"designed\"}\n";
    const auto closed = (*opened)->Shutdown(std::chrono::milliseconds(5000));
    if (!closed) {
        std::cerr << closed.error().code << ": " << closed.error().message << '\n';
        return 1;
    }
    return 0;
} catch (...) {
    return 100;
}
