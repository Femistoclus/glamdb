#include <storage/engine.h>

#include <filesystem>

int main(int argc, char** argv)
try {
    if (argc != 2) {
        return 1;
    }
    const std::filesystem::path root = argv[1];
    const std::string id = "00000000-0000-0000-0000-000000000001";
    {
        const auto created = glamdb::CreateStore({id, root.string()});
        if (!created || (*created)->GetStoreInfo().store_id != id || !(*created)->ListCollections()->empty()) {
            return 2;
        }
        if (!(*created)->Shutdown(std::chrono::milliseconds(1000))) {
            return 3;
        }
    }
    const auto opened = glamdb::OpenExisting({root.string(), id});
    if (!opened || (*opened)->GetCollection("00112233-4455-6677-8899-aabbccddeeff").has_value()) {
        return 4;
    }
    const auto outcome =
        (*opened)->FindAppendOutcome("00112233-4455-6677-8899-aabbccddeeff", "00000000-0000-0000-0000-000000000003");
    if (outcome || outcome.error().code != "COLLECTION_NOT_FOUND") {
        return 6;
    }
    return (*opened)->Shutdown(std::chrono::milliseconds(1000)) ? 0 : 5;
} catch (...) {
    return 100;
}
