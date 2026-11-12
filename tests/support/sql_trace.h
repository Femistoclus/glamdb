#pragma once

#include <sqlite3.h>

#include <cstdint>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace glamdb::test {

// One statement finished on a traced connection.
struct SqlStatement
{
    std::string sql;
    int64_t rows = 0;
    int64_t vm_steps = 0;        // SQLITE_STMTSTATUS_VM_STEP
    int64_t full_scan_steps = 0; // SQLITE_STMTSTATUS_FULLSCAN_STEP
    int64_t page_reads = 0;      // pager cache hits and misses, including the statement's preparation
};

struct SqlTotals
{
    int64_t statements = 0;
    int64_t rows = 0;
    int64_t vm_steps = 0;
    int64_t full_scan_steps = 0;
    int64_t page_reads = 0;
};

inline SqlTotals Sum(const std::vector<SqlStatement>& statements)
{
    SqlTotals totals;
    for (const auto& statement : statements) {
        ++totals.statements;
        totals.rows += statement.rows;
        totals.vm_steps += statement.vm_steps;
        totals.full_scan_steps += statement.full_scan_steps;
        totals.page_reads += statement.page_reads;
    }
    return totals;
}

inline int64_t CountContaining(const std::vector<SqlStatement>& statements, std::string_view fragment)
{
    int64_t count = 0;
    for (const auto& statement : statements) {
        count += statement.sql.find(fragment) != std::string::npos ? 1 : 0;
    }
    return count;
}

// Observes the SQLite work of production code without production hooks. While
// an instance is alive, an auto extension installs a ROW/PROFILE trace on every
// new connection whose main database file is named `file_name`; connections
// opened earlier stay untraced. One instance at a time.
class SqlTrace
{
public:
    explicit SqlTrace(std::string file_name = "catalog.sqlite")
    {
        auto& state = Shared();
        {
            std::lock_guard lock(state.mutex);
            if (state.active) {
                throw std::logic_error("only one SqlTrace may be active");
            }
            state.Reset(true, "/" + std::move(file_name));
        }
        if (sqlite3_auto_extension(reinterpret_cast<void (*)()>(&Install)) != SQLITE_OK) {
            throw std::runtime_error("cannot register the SQLite trace");
        }
    }
    ~SqlTrace()
    {
        sqlite3_cancel_auto_extension(reinterpret_cast<void (*)()>(&Install));
        auto& state = Shared();
        std::lock_guard lock(state.mutex);
        state.Reset(false, {});
    }
    SqlTrace(const SqlTrace&) = delete;
    SqlTrace& operator=(const SqlTrace&) = delete;

    // Statements finished since the previous call, in completion order.
    std::vector<SqlStatement> Take()
    {
        auto& state = Shared();
        std::lock_guard lock(state.mutex);
        return std::exchange(state.statements, {});
    }

private:
    struct State
    {
        std::mutex mutex;
        bool active = false;
        std::string suffix;
        std::vector<SqlStatement> statements;
        std::map<sqlite3_stmt*, int64_t> rows;
        std::map<sqlite3*, int64_t> pages; // until SQLITE_TRACE_CLOSE

        void Reset(bool enable, std::string connection_suffix)
        {
            active = enable;
            suffix = std::move(connection_suffix);
            statements.clear();
            rows.clear();
        }
    };

    // Never destroyed: traced connections may outlive every instance.
    static State& Shared()
    {
        static auto* state = new State();
        return *state;
    }

    static int64_t PageReads(sqlite3* connection)
    {
        sqlite3_int64 hits = 0;
        sqlite3_int64 misses = 0;
        sqlite3_int64 highwater = 0;
        sqlite3_db_status64(connection, SQLITE_DBSTATUS_CACHE_HIT, &hits, &highwater, 0);
        sqlite3_db_status64(connection, SQLITE_DBSTATUS_CACHE_MISS, &misses, &highwater, 0);
        return hits + misses;
    }

    static int Install(sqlite3* connection, char** /*error*/, const sqlite3_api_routines* /*api*/)
    {
        const std::string_view path =
            sqlite3_db_filename(connection, "main") != nullptr ? sqlite3_db_filename(connection, "main") : "";
        auto& state = Shared();
        {
            std::lock_guard lock(state.mutex);
            if (!state.active || !path.ends_with(state.suffix)) {
                return SQLITE_OK;
            }
            state.pages[connection] = PageReads(connection);
        }
        sqlite3_trace_v2(connection, SQLITE_TRACE_ROW | SQLITE_TRACE_PROFILE | SQLITE_TRACE_CLOSE, &OnTrace, &state);
        return SQLITE_OK;
    }

    // SQLite calls this with the connection mutex held; it is recursive, so the
    // status calls below do not block.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters): sqlite3_trace_v2 callback signature.
    static int OnTrace(unsigned type, void* context, void* subject, void* /*detail*/)
    {
        auto& state = *static_cast<State*>(context);
        std::lock_guard lock(state.mutex);
        if (type == SQLITE_TRACE_CLOSE) {
            state.pages.erase(static_cast<sqlite3*>(subject));
            return 0;
        }
        auto* statement = static_cast<sqlite3_stmt*>(subject);
        if (type == SQLITE_TRACE_ROW) {
            ++state.rows[statement];
            return 0;
        }
        auto* connection = sqlite3_db_handle(statement);
        const auto pages = PageReads(connection);
        auto& previous = state.pages[connection];
        const char* sql = sqlite3_sql(statement);
        SqlStatement record{sql != nullptr ? sql : "",
            0,
            sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_VM_STEP, 0),
            sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0),
            pages - previous};
        previous = pages;
        if (const auto found = state.rows.find(statement); found != state.rows.end()) {
            record.rows = found->second;
            state.rows.erase(found);
        }
        if (state.active) {
            state.statements.push_back(std::move(record));
        }
        return 0;
    }
};

} // namespace glamdb::test
