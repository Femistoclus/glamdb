#pragma once

#include <iceberg/catalog.h>

namespace glamdb::detail {

// iceberg::Table requires a catalog, but GlamDB publishes only through the
// coordinator's SQLite transaction. Every operation of this catalog fails, so
// an accidental Iceberg commit cannot bypass that single write authority.
class TInertCatalog final : public iceberg::Catalog
{
public:
    std::string_view name() const override;
    iceberg::Status CreateNamespace(const iceberg::Namespace& ns,
        const std::unordered_map<std::string, std::string>& properties) override;
    iceberg::Result<std::vector<iceberg::Namespace>> ListNamespaces(const iceberg::Namespace& ns) const override;
    iceberg::Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
        const iceberg::Namespace& ns) const override;
    iceberg::Status DropNamespace(const iceberg::Namespace& ns) override;
    iceberg::Result<bool> NamespaceExists(const iceberg::Namespace& ns) const override;
    iceberg::Status UpdateNamespaceProperties(const iceberg::Namespace& ns,
        const std::unordered_map<std::string, std::string>& updates,
        const std::unordered_set<std::string>& removals) override;
    iceberg::Result<std::vector<iceberg::TableIdentifier>> ListTables(const iceberg::Namespace& ns) const override;
    iceberg::Result<std::shared_ptr<iceberg::Table>> CreateTable(const iceberg::TableIdentifier& identifier,
        const std::shared_ptr<iceberg::Schema>& schema,
        const std::shared_ptr<iceberg::PartitionSpec>& spec,
        const std::shared_ptr<iceberg::SortOrder>& order,
        const std::string& location,
        const std::unordered_map<std::string, std::string>& properties) override;
    iceberg::Result<std::shared_ptr<iceberg::Table>> UpdateTable(const iceberg::TableIdentifier& identifier,
        const std::vector<std::unique_ptr<iceberg::TableRequirement>>& requirements,
        const std::vector<std::unique_ptr<iceberg::TableUpdate>>& updates) override;
    iceberg::Result<std::shared_ptr<iceberg::Transaction>> StageCreateTable(const iceberg::TableIdentifier& identifier,
        const std::shared_ptr<iceberg::Schema>& schema,
        const std::shared_ptr<iceberg::PartitionSpec>& spec,
        const std::shared_ptr<iceberg::SortOrder>& order,
        const std::string& location,
        const std::unordered_map<std::string, std::string>& properties) override;
    iceberg::Result<bool> TableExists(const iceberg::TableIdentifier& identifier) const override;
    iceberg::Status DropTable(const iceberg::TableIdentifier& identifier, bool purge) override;
    iceberg::Status RenameTable(const iceberg::TableIdentifier& from, const iceberg::TableIdentifier& to) override;
    iceberg::Result<std::shared_ptr<iceberg::Table>> LoadTable(const iceberg::TableIdentifier& identifier) override;
    iceberg::Result<std::shared_ptr<iceberg::Table>> RegisterTable(const iceberg::TableIdentifier& identifier,
        const std::string& metadata_file_location) override;
};

} // namespace glamdb::detail
