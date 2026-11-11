#pragma once

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

namespace glamdb::test {

class TempDir
{
public:
    TempDir()
    {
        const char* selected = std::getenv("GLAMDB_TEST_TMPDIR");
        const auto parent = std::filesystem::canonical(selected != nullptr ? selected : "/tmp");
        std::string pattern = (parent / "glamdb-test-XXXXXX").string();
        const char* created = mkdtemp(pattern.data());
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path = created;
    }
    ~TempDir()
    {
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
    std::filesystem::path path;
};

inline constexpr char store_id[] = "00000000-0000-0000-0000-000000000001";
inline constexpr char collection_id[] = "00112233-4455-6677-8899-aabbccddeeff";
inline constexpr char table_uuid[] = "00000000-0000-0000-0000-000000000002";
inline constexpr char operation_id[] = "00000000-0000-0000-0000-000000000003";
inline constexpr char attempt_id[] = "00000000-0000-0000-0000-000000000004";

} // namespace glamdb::test
