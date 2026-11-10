#include <storage/engine.h>

#include <arrow/type.h>

#include <filesystem>
#include <string_view>

int main(int argc, char** argv)
try {
    if (argc != 3 || (std::string_view(argv[2]) != "ON" && std::string_view(argv[2]) != "OFF")) {
        return 1;
    }
    const bool expectStorage = std::string_view(argv[2]) == "ON";
    const std::filesystem::path root = argv[1];
    const std::string id = "00000000-0000-0000-0000-000000000001";
    const std::string collectionID = "00112233-4455-6677-8899-aabbccddeeff";
    const glamdb::SchemaSpec schema{{{"key", glamdb::FieldType::String, false}}};
    {
        const auto created = glamdb::CreateStore({id, root.string()});
        if (!created || (*created)->GetStoreInfo().store_id != id) {
            return 2;
        }
        const auto collections = (*created)->ListCollections();
        if (!collections || !collections->empty()) {
            return 2;
        }
        if (expectStorage &&
            (*created)->CreateCollection({collectionID, "Consumer", schema}).disposition !=
                glamdb::CommitDisposition::Committed)
        {
            return 7;
        }
        if (!(*created)->Shutdown(std::chrono::milliseconds(1000))) {
            return 3;
        }
    }
    const auto opened = glamdb::OpenExisting({root.string(), id});
    if (!opened) {
        return 4;
    }
    if (expectStorage) {
        const auto resolved = (*opened)->ResolveCollection("Consumer");
        if (!resolved || resolved->collection_id != collectionID || resolved->name != "Consumer" ||
            resolved->schema != schema || resolved->current_snapshot_id.has_value())
        {
            return 8;
        }
        const auto expectedSchema = arrow::schema({arrow::field("key", arrow::utf8(), false)});
        const auto actualSchema = (*opened)->Schema(collectionID);
        if (!actualSchema || !*actualSchema || !(*actualSchema)->Equals(*expectedSchema)) {
            return 9;
        }
        const auto cursor = (*opened)->Scan({.collection_id = collectionID,
            .predicate = glamdb::Predicate::Compare("key", glamdb::CompareOp::Eq, std::string("unused")),
            .limit = 0});
        if (!cursor || !*cursor) {
            return 10;
        }
        const auto& info = (*cursor)->Info();
        if (!info.output_schema || !info.output_schema->Equals(*expectedSchema) ||
            info.selector_resolved != glamdb::SnapshotSelector::InitialEmpty() || info.identity.store_id != id ||
            info.identity.collection_id != collectionID || info.identity.table_uuid != resolved->table_uuid ||
            info.identity.current_snapshot_id.has_value() || info.identity.metadata_location.empty())
        {
            return 11;
        }
        const auto next = (*cursor)->Next();
        if (!next || *next) {
            return 12;
        }
    } else {
        if ((*opened)->GetCollection(collectionID).has_value()) {
            return 4;
        }
        const auto outcome = (*opened)->FindAppendOutcome(collectionID, "00000000-0000-0000-0000-000000000003");
        if (outcome || outcome.error().code != "COLLECTION_NOT_FOUND") {
            return 6;
        }
    }
    return (*opened)->Shutdown(std::chrono::milliseconds(1000)) ? 0 : 5;
} catch (...) {
    return 100;
}
