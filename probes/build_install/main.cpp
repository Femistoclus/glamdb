#include <arrow/api.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/catalog/sql/catalog_store.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/file_io.h>
#include <iceberg/file_writer.h>
#include <iceberg/metrics_config.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/table_scan.h>
#include <iceberg/update/fast_append.h>
#include <iceberg/version.h>
#include <parquet/arrow/reader.h>
#include <sqlite3.h>

#include <cstdio>
#include <exception>
#include <iostream>
#include <type_traits>

static_assert(std::is_abstract_v<iceberg::sql::CatalogStore>);
static_assert(std::is_abstract_v<iceberg::Writer>);
static_assert(ICEBERG_VERSION_MAJOR == 0 && ICEBERG_VERSION_MINOR == 4 && ICEBERG_VERSION_PATCH == 0);

int main()
try {
    auto volatile make_writer = &iceberg::DataWriter::Make;
    auto volatile make_append = &iceberg::FastAppend::Make;
    auto volatile make_scan = &iceberg::DataTableScan::Make;
    auto volatile make_catalog = &iceberg::sql::SqlCatalog::Make;
    auto io = std::make_shared<iceberg::FileIO>();
    if (!make_writer || !make_append || !make_scan || !make_catalog || io->NewInputFile("unused")) {
        return 1;
    }
    iceberg::parquet::RegisterAll();
    iceberg::avro::RegisterAll();
    for (auto format : {iceberg::FileFormatType::kParquet, iceberg::FileFormatType::kAvro}) {
        if (!iceberg::WriterFactoryRegistry::GetFactory(format)()) {
            return 2;
        }
    }
    auto config = iceberg::MetricsConfig::Make(
        std::unordered_map<std::string, std::string>{{"write.metadata.metrics.column.key", "full"}});
    if (!config || (*config)->ColumnMode("key").kind != iceberg::MetricsMode::Kind::kFull) {
        return 3;
    }
    iceberg::sql::SqlCatalogConfig catalog_config;
    catalog_config.name = "installed-consumer";
    catalog_config.uri = ":memory:";
    catalog_config.max_connections = 1;
    if (!iceberg::sql::SqlCatalog::MakeSqliteCatalog(catalog_config, io)) {
        return 4;
    }
    std::cout << "Arrow " << arrow::GetBuildInfo().version_string << '\n'
              << "Iceberg " << ICEBERG_FULL_VERSION_STRING << '\n'
              << "SQLite " << sqlite3_libversion() << " " << sqlite3_sourceid() << '\n';
    return sqlite3_libversion_number() != 3054001;
} catch (const std::exception& error) {
    std::fprintf(stderr, "%s\n", error.what());
    return 5;
} catch (...) {
    return 6;
}
