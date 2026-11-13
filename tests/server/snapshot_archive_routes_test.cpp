#include "apps/server/metadata.h"
#include "apps/server/routes.h"
#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

#include <format>

namespace glamdb::NServer {
namespace {
using NTransport::TJson;

constexpr char KvCollection[] = "00112233-4455-6677-8899-aabbccddee01";

std::string Op(int64_t index)
{
    return std::format("00000000-0000-4000-8000-{:012}", index);
}

// Identities name the current root (location, head), which every append
// changes; the selected snapshot is compared through snapshot_id instead.
TJson WithoutIdentity(TJson value)
{
    if (value.is_object()) {
        value.erase("identity");
        for (auto& [key, item] : value.items()) {
            (void)key;
            item = WithoutIdentity(std::move(item));
        }
    } else if (value.is_array()) {
        for (auto& item : value) {
            item = WithoutIdentity(std::move(item));
        }
    }
    return value;
}

// The production local backend over a fault-injecting SQLite VFS, exercised
// only through the HTTP routes.
class TSnapshotArchiveRoutesTest : public ::testing::Test
{
protected:
    test::TempDir Directory_;
    test::JournalFaultVfs Vfs_;
    std::shared_ptr<Engine> Engine_;
    std::optional<TRoutes> Routes_;

    void SetUp() override
    {
        const detail::BackendFactory factory =
            [](const std::filesystem::path& root,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            auto backend = detail::TLocalStorageBackend::Open(root, info, {});
            if (!backend) {
                return std::unexpected(backend.error());
            }
            return std::shared_ptr<detail::StorageBackend>(std::move(*backend));
        };
        auto created = detail::EngineFactory::CreateWith(
            {.store_id = test::store_id, .absolute_root = (Directory_.path / "store").string()},
            factory,
            Vfs_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        Routes_.emplace(Engine_, std::filesystem::path{}, "secret");
    }
    void TearDown() override
    {
        Vfs_.Disarm();
        Routes_.reset();
        if (Engine_) {
            [[maybe_unused]] auto stopped = Engine_->Shutdown(std::chrono::seconds(5));
        }
    }
    NTransport::TResponse Call(std::string method, std::string target, std::optional<TJson> body = std::nullopt)
    {
        NTransport::TRequest request{std::move(method), std::move(target), {{"authorization", "Bearer secret"}}, {}};
        if (body) {
            request.Headers["content-type"] = "application/json";
            request.Body = body->dump();
        }
        return Routes_->Handle(request);
    }
    TJson Json(std::string method, std::string target, std::optional<TJson> body = std::nullopt, int status = 200)
    {
        const auto response = Call(std::move(method), std::move(target), std::move(body));
        EXPECT_EQ(response.Status, status) << response.Body;
        return NTransport::ParseJson(response.Body);
    }
    static std::string Path(std::string_view suffix = {}, std::string_view collection = test::collection_id)
    {
        return std::string("/collections/") + std::string(collection) + std::string(suffix);
    }
    TJson Create(TJson extra = TJson::object(), int status = 200)
    {
        TJson body{{"collection_id", test::collection_id},
            {"name", "events"},
            {"schema", {{"fields", {{{"name", "n"}, {"type", "int64"}, {"nullable", true}}}}}}};
        body.update(extra);
        return Json("POST", "/collections", body, status);
    }
    std::string Append(int64_t index)
    {
        const auto value = Json("POST",
            Path("/append"),
            TJson{{"operation_id", Op(index)}, {"rows", {{{"n", std::to_string(index)}}, {{"n", nullptr}}}}});
        return value.at("result").at("snapshot_id").get<std::string>();
    }
    // Answers of one selected snapshot that must not depend on where it lives.
    // Identity names the current root, so it is compared separately.
    TJson Answers(const std::string& snapshot)
    {
        auto stats = Json("GET", Path("/stats?snapshot_id=" + snapshot));
        // The count of retained snapshots is reported only for the current view.
        stats.erase("retained_snapshots");
        auto files = Json("GET", Path("/files?page_size=1000&snapshot_id=" + snapshot));
        auto details = TJson::array();
        for (const auto& file : files.at("items")) {
            details.push_back(
                Json("GET", Path("/files/" + file.at("file_id").get<std::string>() + "?snapshot_id=" + snapshot)));
        }
        auto scan = Json("POST", Path("/scan"), TJson{{"snapshot_id", snapshot}});
        auto explain = Json("POST", Path("/explain"), TJson{{"snapshot_id", snapshot}});
        return WithoutIdentity({{"stats", stats},
            {"files", files.at("items")},
            {"files_snapshot", files.at("snapshot_id")},
            {"details", details},
            {"rows", scan.at("rows")},
            {"scan_snapshot", scan.at("snapshot_id")},
            {"explain", explain}});
    }
};

TEST_F(TSnapshotArchiveRoutesTest, CapabilitiesAdvertiseTheArchiveOfTheProductionBackend)
{
    const auto value = Json("GET", "/capabilities");
    EXPECT_TRUE(value.at("features").at("snapshot_archive").get<bool>());
    EXPECT_EQ(InstalledCapabilities().snapshot_archive, Engine_->GetCapabilities().snapshot_archive);
    EngineCapabilities none;
    EXPECT_FALSE(CapabilitiesJson(none, std::nullopt, false, {}).at("features").at("snapshot_archive").get<bool>());
}

TEST_F(TSnapshotArchiveRoutesTest, CreationRetriesUpdatesAndRefusalsMapCoreErrors)
{
    const auto created = Create({{"hot_snapshots", "3"}});
    EXPECT_EQ(created.at("hot_snapshots"), "3");
    EXPECT_EQ(created.at("hot_snapshots_defaulted"), false);
    EXPECT_EQ(Create({{"hot_snapshots", "3"}}), created);
    // Omitted and null both select the default (100), "0" no archive: every
    // other effective value is a mismatch.
    for (const auto& different : {TJson{{"hot_snapshots", "4"}},
             TJson::object(),
             TJson{{"hot_snapshots", nullptr}},
             TJson{{"hot_snapshots", "0"}},
             TJson{{"hot_snapshots", "100"}}})
    {
        const auto mismatch = Create(different, 409);
        EXPECT_EQ(mismatch.at("error").at("code"), "SNAPSHOT_ARCHIVE_MISMATCH") << different;
        EXPECT_EQ(mismatch.at("outcome"), "definitely_not_committed");
    }
    for (const auto& invalid : {TJson("1099511627777"), TJson("-1"), TJson(3), TJson("3x"), TJson(true)}) {
        auto body = TJson{{"hot_snapshots", invalid}};
        const auto response = Call("POST", Path("/snapshot-archive"), body);
        EXPECT_EQ(response.Status, 400) << invalid << response.Body;
        EXPECT_EQ(NTransport::ParseJson(response.Body)["error"]["code"], "INVALID_ARGUMENT");
        EXPECT_EQ(Create(body, 400).at("error").at("code"), "INVALID_ARGUMENT") << invalid;
    }
    EXPECT_EQ(Json("POST", Path("/snapshot-archive"), TJson::object(), 400).at("error").at("code"), "INVALID_ARGUMENT");
    EXPECT_EQ(Json("POST", Path("/snapshot-archive"), TJson{{"hot_snapshots", "2"}, {"extra", 1}}, 400)
                  .at("error")
                  .at("code"),
        "INVALID_ARGUMENT");
    EXPECT_EQ(
        Json("POST", Path("/snapshot-archive/maintenance"), TJson{{"hot_snapshots", "2"}}, 400).at("error").at("code"),
        "INVALID_ARGUMENT");
    EXPECT_EQ(Json("POST", Path("/snapshot-archive?x=1"), TJson{{"hot_snapshots", "2"}}, 400).at("error").at("code"),
        "INVALID_ARGUMENT");
    for (const auto& [method, suffix] : {std::pair{"GET", "/snapshot-archive/maintenance"},
             std::pair{"POST", "/snapshot-archive/other"},
             std::pair{"GET", "/snapshot-archive/other"}})
    {
        const auto response = Call(method,
            Path(suffix),
            std::string(method) == "POST" ? std::optional<TJson>(TJson::object()) : std::nullopt);
        EXPECT_EQ(response.Status, 404) << suffix;
    }
    EXPECT_EQ(Call("PUT", Path("/snapshot-archive"), TJson{{"hot_snapshots", "2"}}).Status, 405);

    // Updating is idempotent and moves nothing; the created value still
    // decides repeated creation.
    const auto updated = Json("POST", Path("/snapshot-archive"), TJson{{"hot_snapshots", "5"}});
    EXPECT_EQ(updated.at("hot_snapshots"), "5");
    EXPECT_EQ(Json("POST", Path("/snapshot-archive"), TJson{{"hot_snapshots", "5"}}), updated);
    EXPECT_EQ(Json("GET", Path()), updated);
    EXPECT_EQ(Create({{"hot_snapshots", "3"}}).at("hot_snapshots"), "5");
    EXPECT_EQ(Create({{"hot_snapshots", "5"}}, 409).at("error").at("code"), "SNAPSHOT_ARCHIVE_MISMATCH");
    const auto status = Json("GET", Path("/snapshot-archive"));
    EXPECT_EQ(status,
        TJson({{"hot_snapshots", "5"},
            {"root_snapshots", "0"},
            {"archived_snapshots", "0"},
            {"expired_snapshots", "0"},
            {"archive_pending", "0"}}));
    // "0" and null both stop archiving; the effective value is reported as "0".
    for (const auto& stop : {TJson("0"), TJson(nullptr)}) {
        const auto stopped = Json("POST", Path("/snapshot-archive"), TJson{{"hot_snapshots", stop}});
        EXPECT_EQ(stopped.at("hot_snapshots"), "0");
        EXPECT_EQ(stopped.at("hot_snapshots_defaulted"), false);
        EXPECT_EQ(Json("GET", Path("/snapshot-archive")).at("hot_snapshots"), "0");
    }
    EXPECT_EQ(Create({{"hot_snapshots", "3"}}).at("hot_snapshots"), "0");

    const TJson kv{{"collection_id", KvCollection},
        {"name", "kv"},
        {"mode", "kv"},
        {"schema", {{"fields", {{{"name", "key"}, {"type", "string"}, {"nullable", false}}}}}}};
    auto refused = kv;
    refused["hot_snapshots"] = "2";
    EXPECT_EQ(Json("POST", "/collections", refused, 409).at("error").at("code"), "COLLECTION_MODE_MISMATCH");
    EXPECT_EQ(Json("GET", Path({}, KvCollection), std::nullopt, 404).at("error").at("code"), "COLLECTION_NOT_FOUND");
    // KV collections never get the default; omitted and "0" are the same creation.
    const auto kvCreated = Json("POST", "/collections", kv);
    EXPECT_EQ(kvCreated.at("hot_snapshots"), "0");
    EXPECT_EQ(kvCreated.at("hot_snapshots_defaulted"), false);
    auto kvExplicit = kv;
    kvExplicit["hot_snapshots"] = "0";
    EXPECT_EQ(Json("POST", "/collections", kvExplicit), kvCreated);
    EXPECT_EQ(Json("POST", Path("/snapshot-archive", KvCollection), TJson{{"hot_snapshots", "2"}}, 409)
                  .at("error")
                  .at("code"),
        "COLLECTION_MODE_MISMATCH");
    EXPECT_EQ(
        Json("POST", Path("/snapshot-archive/maintenance", KvCollection), TJson::object(), 409).at("error").at("code"),
        "COLLECTION_MODE_MISMATCH");
    // Stopping is the KV collection's only (and unchanged) state.
    for (const auto& stop : {TJson("0"), TJson(nullptr)}) {
        EXPECT_EQ(Json("POST", Path("/snapshot-archive", KvCollection), TJson{{"hot_snapshots", stop}}), kvCreated);
    }
    EXPECT_EQ(Json("GET", Path("/snapshot-archive", KvCollection)).at("hot_snapshots"), "0");
    EXPECT_EQ(Json("GET", Path("/snapshot-archive", "00112233-4455-6677-8899-aabbccddee02"), std::nullopt, 404)
                  .at("error")
                  .at("code"),
        "COLLECTION_NOT_FOUND");
}

TEST_F(TSnapshotArchiveRoutesTest, OmittedSettingDefaultsAndRepeatedCreationComparesEffectiveValues)
{
    const auto events = [](std::string_view id, std::string_view name, TJson extra) {
        TJson body{{"collection_id", id},
            {"name", name},
            {"schema", {{"fields", {{{"name", "n"}, {"type", "int64"}, {"nullable", true}}}}}}};
        body.update(extra);
        return body;
    };
    constexpr char Defaulted[] = "00112233-4455-6677-8899-aabbccddee11";
    constexpr char Hundred[] = "00112233-4455-6677-8899-aabbccddee12";
    constexpr char NoArchive[] = "00112233-4455-6677-8899-aabbccddee13";

    // Omitted: the default, reported as an effective value plus the flag.
    const auto defaulted = Json("POST", "/collections", events(Defaulted, "defaulted", TJson::object()));
    EXPECT_EQ(defaulted.at("hot_snapshots"), "100");
    EXPECT_EQ(defaulted.at("hot_snapshots_defaulted"), true);
    EXPECT_EQ(Json("GET", Path({}, Defaulted)), defaulted);
    // A repeated create matches by effective value: omitted, null and an
    // explicit 100 are the same creation; "0" and any other value are not.
    for (const auto& same : {TJson::object(), TJson{{"hot_snapshots", nullptr}}, TJson{{"hot_snapshots", "100"}}}) {
        EXPECT_EQ(Json("POST", "/collections", events(Defaulted, "defaulted", same)), defaulted) << same;
    }
    for (const auto& different : {TJson{{"hot_snapshots", "0"}}, TJson{{"hot_snapshots", "99"}}}) {
        EXPECT_EQ(Json("POST", "/collections", events(Defaulted, "defaulted", different), 409).at("error").at("code"),
            "SNAPSHOT_ARCHIVE_MISMATCH")
            << different;
    }
    const auto status = Json("GET", Path("/snapshot-archive", Defaulted));
    EXPECT_EQ(status.at("hot_snapshots"), "100");
    EXPECT_EQ(status.at("archive_pending"), "0");

    // Explicit 100 is not defaulted: an omitted value matches only a
    // still-defaulted collection, whatever the numeric default is.
    const auto hundred = Json("POST", "/collections", events(Hundred, "hundred", {{"hot_snapshots", "100"}}));
    EXPECT_EQ(hundred.at("hot_snapshots"), "100");
    EXPECT_EQ(hundred.at("hot_snapshots_defaulted"), false);
    EXPECT_EQ(Json("POST", "/collections", events(Hundred, "hundred", {{"hot_snapshots", "100"}})), hundred);
    EXPECT_EQ(Json("POST", "/collections", events(Hundred, "hundred", TJson::object()), 409).at("error").at("code"),
        "SNAPSHOT_ARCHIVE_MISMATCH");

    // Explicit opt-out: "0" matches only "0".
    const auto none = Json("POST", "/collections", events(NoArchive, "none", {{"hot_snapshots", "0"}}));
    EXPECT_EQ(none.at("hot_snapshots"), "0");
    EXPECT_EQ(none.at("hot_snapshots_defaulted"), false);
    EXPECT_EQ(Json("POST", "/collections", events(NoArchive, "none", {{"hot_snapshots", "0"}})), none);
    for (const auto& different : {TJson::object(), TJson{{"hot_snapshots", nullptr}}, TJson{{"hot_snapshots", "100"}}})
    {
        EXPECT_EQ(Json("POST", "/collections", events(NoArchive, "none", different), 409).at("error").at("code"),
            "SNAPSHOT_ARCHIVE_MISMATCH")
            << different;
    }
    EXPECT_EQ(Json("GET", Path("/snapshot-archive", NoArchive)),
        TJson({{"hot_snapshots", "0"},
            {"root_snapshots", "0"},
            {"archived_snapshots", "0"},
            {"expired_snapshots", "0"},
            {"archive_pending", "0"}}));

    // Any update, even to the same value, makes the setting explicit: an
    // omitted value no longer matches, an explicit value is compared with the
    // value the collection was created with.
    const auto explicitHundred = Json("POST", Path("/snapshot-archive", Defaulted), TJson{{"hot_snapshots", "100"}});
    EXPECT_EQ(explicitHundred.at("hot_snapshots"), "100");
    EXPECT_EQ(explicitHundred.at("hot_snapshots_defaulted"), false);
    EXPECT_EQ(Json("POST", "/collections", events(Defaulted, "defaulted", TJson::object()), 409).at("error").at("code"),
        "SNAPSHOT_ARCHIVE_MISMATCH");
    const auto stopped = Json("POST", Path("/snapshot-archive", Defaulted), TJson{{"hot_snapshots", "0"}});
    EXPECT_EQ(stopped.at("hot_snapshots"), "0");
    EXPECT_EQ(Json("POST", "/collections", events(Defaulted, "defaulted", TJson::object()), 409).at("error").at("code"),
        "SNAPSHOT_ARCHIVE_MISMATCH");
    EXPECT_EQ(Json("POST", "/collections", events(Defaulted, "defaulted", {{"hot_snapshots", "0"}}), 409)
                  .at("error")
                  .at("code"),
        "SNAPSHOT_ARCHIVE_MISMATCH");
    // A response can be repeated as the create request of a collection that
    // was never updated.
    for (const auto& [id, name, info] : {std::tuple{Hundred, "hundred", hundred}, std::tuple{NoArchive, "none", none}})
    {
        EXPECT_EQ(Json("POST", "/collections", events(id, name, {{"hot_snapshots", info.at("hot_snapshots")}})), info);
    }
}

TEST_F(TSnapshotArchiveRoutesTest, ArchivedSnapshotsAnswerOverHttpExactlyAsWhileHot)
{
    Create({{"hot_snapshots", "2"}});
    constexpr int64_t Appends = 40;
    std::vector<std::string> snapshots;
    std::map<int64_t, TJson> hot;
    for (int64_t index = 0; index < Appends; ++index) {
        snapshots.push_back(Append(index));
        if (index % 7 == 0) {
            hot[index] = Answers(snapshots.back());
        }
        const auto status = Json("GET", Path("/snapshot-archive"));
        const auto root = std::stoll(status.at("root_snapshots").get<std::string>());
        // At most hot + min(hot, 16) snapshots stay in the root.
        EXPECT_LE(root, 4) << index;
        EXPECT_EQ(root + std::stoll(status.at("archived_snapshots").get<std::string>()), index + 1);
    }
    const auto status = Json("GET", Path("/snapshot-archive"));
    EXPECT_GT(std::stoll(status.at("archived_snapshots").get<std::string>()), Appends - 5);
    for (const auto& [index, answers] : hot) {
        EXPECT_EQ(Answers(snapshots[index]), answers) << index;
        EXPECT_EQ(answers.at("rows").size(), 2 * (index + 1));
    }
    // Complete history in keyset pages, archived entries first.
    std::vector<std::string> listed;
    auto page = Json("GET", Path("/snapshots?page_size=7"));
    const auto identity = page.at("identity");
    for (;;) {
        EXPECT_EQ(page.at("identity"), identity);
        for (const auto& item : page.at("items")) {
            listed.push_back(item.at("snapshot_id").get<std::string>());
            EXPECT_EQ(item.at("operation_id"), Op(static_cast<int64_t>(listed.size()) - 1));
        }
        if (page.at("next_cursor").is_null()) {
            break;
        }
        page = Json("GET", Path("/snapshots?page_size=7&cursor=" + page.at("next_cursor").get<std::string>()));
    }
    EXPECT_EQ(listed, snapshots);
    EXPECT_EQ(Json("GET", Path("/stats")).at("retained_snapshots"), std::to_string(Appends));
    // History selected at an archived snapshot ends there.
    const auto selected = Json("GET", Path("/snapshots?page_size=1000&snapshot_id=" + snapshots[3]));
    EXPECT_EQ(selected.at("items").size(), 4U);
    EXPECT_EQ(selected.at("snapshot_id"), snapshots[3]);
    // A precondition on an archived snapshot is not the current one.
    const auto stale = Json("POST",
        Path("/append"),
        TJson{{"operation_id", Op(Appends)}, {"rows", {{{"n", "1"}}}}, {"expected_snapshot", snapshots[0]}},
        412);
    EXPECT_EQ(stale.at("error").at("code"), "PRECONDITION_FAILED");
    EXPECT_EQ(Json("GET", Path("/stats?snapshot_id=12345"), std::nullopt, 404).at("error").at("code"), "NOT_FOUND");

    // Maintenance after lowering the setting: bounded, idempotent, reported.
    Json("POST", Path("/snapshot-archive"), TJson{{"hot_snapshots", "1"}});
    const auto before = Json("GET", Path("/snapshot-archive"));
    const auto moved = Json("POST", Path("/snapshot-archive/maintenance"), TJson::object());
    EXPECT_EQ(moved.at("root_snapshots"), "1");
    EXPECT_EQ(moved.at("archive_pending"), "0");
    EXPECT_EQ(moved.at("snapshots_archived"), before.at("archive_pending"));
    EXPECT_EQ(moved.at("archived_snapshots"), std::to_string(Appends - 1));
    auto again = Json("POST", Path("/snapshot-archive/maintenance"), TJson::object());
    EXPECT_EQ(again.at("snapshots_archived"), "0");
    again.erase("snapshots_archived");
    EXPECT_EQ(again, Json("GET", Path("/snapshot-archive")));
    for (const auto& [index, answers] : hot) {
        EXPECT_EQ(Answers(snapshots[index]), answers) << index;
    }
}

TEST_F(TSnapshotArchiveRoutesTest, UncertainCommitsReportRecoveryRequiredAndRetryAfterReconcile)
{
    Create({{"hot_snapshots", "4"}});
    std::vector<std::string> snapshots;
    for (int64_t index = 0; index < 6; ++index) {
        snapshots.push_back(Append(index));
    }
    for (const bool afterUnlink : {false, true}) {
        // The setting change is one catalog transaction.
        Vfs_.Arm(1, afterUnlink);
        const auto target = afterUnlink ? "1" : "2";
        const auto lost = Json("POST", Path("/snapshot-archive"), TJson{{"hot_snapshots", target}}, 503);
        EXPECT_EQ(lost.at("error").at("code"), "RECOVERY_REQUIRED");
        EXPECT_EQ(lost.at("error").at("retry_hint"), "SameIdAfterReconcile");
        Vfs_.Disarm();
        EXPECT_EQ(Json("GET", Path("/snapshot-archive"), std::nullopt, 503).at("error").at("code"),
            "RECOVERY_REQUIRED");
        ASSERT_TRUE(Engine_->Reconcile());
        EXPECT_EQ(Json("GET", Path()).at("hot_snapshots"), afterUnlink ? TJson(target) : TJson("4"));
        EXPECT_EQ(Json("POST", Path("/snapshot-archive"), TJson{{"hot_snapshots", target}}).at("hot_snapshots"),
            target);
    }
    const auto pending = Json("GET", Path("/snapshot-archive")).at("archive_pending");
    EXPECT_NE(pending, "0");
    for (const bool afterUnlink : {false, true}) {
        Vfs_.Arm(1, afterUnlink);
        const auto lost = Json("POST", Path("/snapshot-archive/maintenance"), TJson::object(), 503);
        EXPECT_EQ(lost.at("error").at("code"), "RECOVERY_REQUIRED");
        Vfs_.Disarm();
        ASSERT_TRUE(Engine_->Reconcile());
        const auto status = Json("GET", Path("/snapshot-archive"));
        EXPECT_EQ(status.at("archive_pending"), afterUnlink ? TJson("0") : pending);
        const auto retried = Json("POST", Path("/snapshot-archive/maintenance"), TJson::object());
        EXPECT_EQ(retried.at("archive_pending"), "0");
        EXPECT_EQ(retried.at("snapshots_archived"), afterUnlink ? "0" : pending.get<std::string>());
        if (!afterUnlink) {
            // Re-create a backlog for the after-unlink case.
            Json("POST", Path("/snapshot-archive"), TJson{{"hot_snapshots", "0"}});
            snapshots.push_back(Append(static_cast<int64_t>(snapshots.size())));
            snapshots.push_back(Append(static_cast<int64_t>(snapshots.size())));
            Json("POST", Path("/snapshot-archive"), TJson{{"hot_snapshots", "1"}});
        }
    }
    const auto status = Json("GET", Path("/snapshot-archive"));
    EXPECT_EQ(status.at("root_snapshots"), "1");
    EXPECT_EQ(std::stoll(status.at("archived_snapshots").get<std::string>()) + 1,
        static_cast<int64_t>(snapshots.size()));
    for (size_t index = 0; index < snapshots.size(); ++index) {
        EXPECT_EQ(Json("POST", Path("/scan"), TJson{{"snapshot_id", snapshots[index]}}).at("rows").size(),
            2 * (index + 1));
    }
}
} // namespace
} // namespace glamdb::NServer
