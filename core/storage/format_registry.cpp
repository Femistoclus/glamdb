#include "core/storage/format_registry.h"

#include <iceberg/avro/avro_register.h>
#include <iceberg/parquet/parquet_register.h>

#include <mutex>

namespace NGlamDB::NStorage {

void RegisterStorageFormats()
{
    static std::once_flag registration;
    std::call_once(registration, [] {
        iceberg::avro::RegisterAll();
        iceberg::parquet::RegisterAll();
    });
}

}
