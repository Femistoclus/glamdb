#include <iceberg/arrow/arrow_io_util.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/manifest/manifest_writer.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/type.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

int main()
try {
    std::vector<std::string> names;
    for (int index = 0; index < 4; ++index) {
        const std::string name = "field" + std::to_string(index);
        names.push_back(name);
    }
    std::vector<iceberg::SchemaField> fields;
    fields.reserve(names.size());
    for (int index = 0; index < 4; ++index) {
        fields.push_back(iceberg::SchemaField::MakeRequired(index + 1, names[index], iceberg::string()));
    }
    auto schema = std::make_shared<iceberg::Schema>(std::move(fields));
    std::shared_ptr<iceberg::FileIO> fileIO = iceberg::arrow::MakeMockFileIO();
    iceberg::avro::RegisterAll();
    auto writer = iceberg::ManifestWriter::MakeWriter(2,
        1,
        "/manifest.avro",
        fileIO,
        iceberg::PartitionSpec::Unpartitioned(),
        schema);
    if (!writer) {
        return 1;
    }
    return (*writer)->Close() ? 0 : 2;
} catch (...) {
    return 3;
}
