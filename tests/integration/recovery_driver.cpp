#include "apps/transport/outcome_json.h"
#include "core/engine/engine_factory.h"
#include "tests/support/arrow_batch.h"
#include "tests/support/checked.h"
#include "tests/support/fake_storage.h"
#include "tests/support/temp_dir.h"

#include <iostream>
#include <sqlite3.h>
#include <unistd.h>

int main(int argc, char** argv)
try {
    using namespace glamdb;
    if (argc != 3) {
        return 2;
    }
    const std::string mode = argv[1];
    const std::filesystem::path root = argv[2];
    auto backend = std::make_shared<test::FakeStorage>(root);
    auto opened = mode == "create" ? detail::EngineFactory::Create({test::store_id, root.string()}, backend)
                                   : detail::EngineFactory::Open({root.string(), test::store_id}, backend);
    if (!opened) {
        std::cerr << opened.error().code << ": " << opened.error().message << '\n';
        return 1;
    }
    auto engine = *opened;
    if (mode == "hold") {
        std::cout << "locked\n" << std::flush;
        std::cin.get();
        return 0;
    }
    if (mode == "hot-journal") {
        if (!engine->Shutdown(std::chrono::milliseconds(1000))) {
            return 3;
        }
        sqlite3* database = nullptr;
        if (sqlite3_open_v2((root / "catalog.sqlite").c_str(), &database, SQLITE_OPEN_READWRITE, nullptr) != SQLITE_OK)
        {
            return 4;
        }
        const auto sql = [&](const char* text) {
            return sqlite3_exec(database, text, nullptr, nullptr, nullptr) == SQLITE_OK;
        };
        if (!sql("PRAGMA synchronous=EXTRA") || !sql("PRAGMA cache_size=1") || !sql("BEGIN IMMEDIATE") ||
            !sql("CREATE TABLE interrupted_spill(value BLOB)"))
        {
            return 5;
        }
        for (int i = 0; i < 200; ++i) {
            if (!sql("INSERT INTO interrupted_spill VALUES(zeroblob(4096))")) {
                return 6;
            }
        }
        _exit(79);
    }
    if (mode == "create") {
        const auto collection =
            engine->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}});
        if (collection.disposition != CommitDisposition::Committed) {
            return 7;
        }
    }
    std::string operation = test::operation_id;
    if (mode == "bound-crash" || mode == "lookup-bound") {
        operation = "00000000-0000-0000-0000-000000000007";
    }
    if (mode == "pointer-crash" || mode == "lookup-pointer") {
        operation = "00000000-0000-0000-0000-000000000008";
    }
    if (mode == "bound-crash") {
        backend->on_prepare = [] {
            _exit(77);
        };
    }
    if (mode == "pointer-crash") {
        backend->on_pointer = [] {
            _exit(78);
        };
    }
    AppendOutcome outcome;
    if (mode.starts_with("lookup-")) {
        const auto found = engine->FindAppendOutcome(test::collection_id, operation);
        if (!found || !found->has_value()) {
            return 8;
        }
        outcome = test::Present(found.value());
    } else {
        outcome = engine->AppendDurable({test::collection_id,
            operation,
            {mode == "read" || mode == "create" ? SnapshotPreconditionKind::NoSnapshot : SnapshotPreconditionKind::Any,
                0},
            {test::Batch()}});
    }
    const auto json = json_adapter::ToJson(outcome);
    if (!json) {
        return 9;
    }
    std::cout << json->body << '\n';
    return engine->Shutdown(std::chrono::milliseconds(1000)) ? 0 : 10;
} catch (...) {
    return 100;
}
