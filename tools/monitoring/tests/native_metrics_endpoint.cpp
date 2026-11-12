// Application-side integration fixture, not a native database HTTP server.
#include "core/telemetry/metrics.h"

#include <prometheus/exposer.h>

#include <iostream>
#include <string>

int main()
{
    const auto metrics = NGlamDB::NTelemetry::GetMetricsCollectable();
    prometheus::Exposer endpoint("127.0.0.1:0", 1);
    endpoint.RegisterCollectable(metrics);
    NGlamDB::NTelemetry::RecordFinished(NGlamDB::NTelemetry::EOperation::Append,
        glamdb::CommitDisposition::Committed,
        glamdb::CommitPhase::Complete,
        {},
        0.025);
    std::cout << endpoint.GetListeningPorts().at(0) << std::endl;
    std::string stop;
    std::getline(std::cin, stop);
}
