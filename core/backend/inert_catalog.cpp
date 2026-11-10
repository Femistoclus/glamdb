#include "core/backend/inert_catalog.h"

namespace glamdb::detail {
namespace {

auto Denied()
{
    return iceberg::NotSupported("GlamDB publishes tables only through the coordinator catalog");
}

} // namespace

std::string_view TInertCatalog::name() const
{
    return "glamdb-inert";
}

iceberg::Status TInertCatalog::CreateNamespace(const iceberg::Namespace&,
    const std::unordered_map<std::string, std::string>&)
{
    return Denied();
}

iceberg::Result<std::vector<iceberg::Namespace>> TInertCatalog::ListNamespaces(const iceberg::Namespace&) const
{
    return Denied();
}

iceberg::Result<std::unordered_map<std::string, std::string>> TInertCatalog::GetNamespaceProperties(
    const iceberg::Namespace&) const
{
    return Denied();
}

iceberg::Status TInertCatalog::DropNamespace(const iceberg::Namespace&)
{
    return Denied();
}

iceberg::Result<bool> TInertCatalog::NamespaceExists(const iceberg::Namespace&) const
{
    return Denied();
}

iceberg::Status TInertCatalog::UpdateNamespaceProperties(const iceberg::Namespace&,
    const std::unordered_map<std::string, std::string>&,
    const std::unordered_set<std::string>&)
{
    return Denied();
}

iceberg::Result<std::vector<iceberg::TableIdentifier>> TInertCatalog::ListTables(const iceberg::Namespace&) const
{
    return Denied();
}

iceberg::Result<std::shared_ptr<iceberg::Table>> TInertCatalog::CreateTable(const iceberg::TableIdentifier&,
    const std::shared_ptr<iceberg::Schema>&,
    const std::shared_ptr<iceberg::PartitionSpec>&,
    const std::shared_ptr<iceberg::SortOrder>&,
    const std::string&,
    const std::unordered_map<std::string, std::string>&)
{
    return Denied();
}

iceberg::Result<std::shared_ptr<iceberg::Table>> TInertCatalog::UpdateTable(const iceberg::TableIdentifier&,
    const std::vector<std::unique_ptr<iceberg::TableRequirement>>&,
    const std::vector<std::unique_ptr<iceberg::TableUpdate>>&)
{
    return Denied();
}

iceberg::Result<std::shared_ptr<iceberg::Transaction>> TInertCatalog::StageCreateTable(const iceberg::TableIdentifier&,
    const std::shared_ptr<iceberg::Schema>&,
    const std::shared_ptr<iceberg::PartitionSpec>&,
    const std::shared_ptr<iceberg::SortOrder>&,
    const std::string&,
    const std::unordered_map<std::string, std::string>&)
{
    return Denied();
}

iceberg::Result<bool> TInertCatalog::TableExists(const iceberg::TableIdentifier&) const
{
    return Denied();
}

iceberg::Status TInertCatalog::DropTable(const iceberg::TableIdentifier&, bool)
{
    return Denied();
}

iceberg::Status TInertCatalog::RenameTable(const iceberg::TableIdentifier&, const iceberg::TableIdentifier&)
{
    return Denied();
}

iceberg::Result<std::shared_ptr<iceberg::Table>> TInertCatalog::LoadTable(const iceberg::TableIdentifier&)
{
    return Denied();
}

iceberg::Result<std::shared_ptr<iceberg::Table>> TInertCatalog::RegisterTable(const iceberg::TableIdentifier&,
    const std::string&)
{
    return Denied();
}

} // namespace glamdb::detail
