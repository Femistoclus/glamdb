#include "apps/admin/data.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <fstream>
#include <sstream>

namespace glamdb::NAdmin {
namespace {
NClient::TResponse Response(const std::string& name)
{
    std::ifstream source(GLAMDB_OUTCOME_FIXTURE);
    const auto fixture = nlohmann::json::parse(source);
    for (const auto& item : fixture.at("cases")) {
        if (item.at("name") == name) {
            return {item.at("status").get<int>(), {}, item.at("response").dump()};
        }
    }
    throw std::runtime_error("missing literal outcome fixture");
}

TEST(TAdminResultTest, ShutdownFailurePreservesUnresolvedOutcomeAndRecoveryContext)
{
    const auto response = Response("unknown_publication");
    const Status closed = std::unexpected(ErrorDetail{"IO_ERROR", "recovery fence failed"});
    std::ostringstream output, diagnostics;
    EXPECT_EQ(WriteDataResponse(response, closed, {.Data = output, .Diagnostics = diagnostics}), 3);
    EXPECT_EQ(output.str(), response.Body + '\n');
    EXPECT_NE(diagnostics.str().find("IO_ERROR"), std::string::npos);
}

TEST(TAdminResultTest, ShutdownFailurePreservesCommittedReceipt)
{
    const auto response = Response("committed");
    const Status closed = std::unexpected(ErrorDetail{"IO_ERROR", "close failed"});
    std::ostringstream output, diagnostics;
    EXPECT_EQ(WriteDataResponse(response, closed, {.Data = output, .Diagnostics = diagnostics}), 4);
    EXPECT_EQ(output.str(), response.Body + '\n');
    EXPECT_NE(diagnostics.str().find("IO_ERROR"), std::string::npos);
}

TEST(TAdminResultTest, OutputFailureAfterConfirmedWriteHasDistinctExit)
{
    const auto response = Response("committed");
    std::ostringstream output, diagnostics;
    output.setstate(std::ios::badbit);
    EXPECT_EQ(WriteDataResponse(response, {}, {.Data = output, .Diagnostics = diagnostics}), 4);
    EXPECT_FALSE(diagnostics.str().empty());
}

TEST(TAdminResultTest, OutputFailureDoesNotDowngradeUnresolvedOutcome)
{
    const auto response = Response("unknown_publication");
    std::ostringstream output, diagnostics;
    output.setstate(std::ios::badbit);
    EXPECT_EQ(WriteDataResponse(response, {}, {.Data = output, .Diagnostics = diagnostics}), 3);
}
} // namespace
} // namespace glamdb::NAdmin
