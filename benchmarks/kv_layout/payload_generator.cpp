#include <cstddef>
#include <cstdint>

// Only deterministic payload generation lives here. PyArrow performs every
// measured Parquet read/write; this helper owns no storage-engine behavior.
// The ctypes ABI fixes this positional signature; callers declare every argument type.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
extern "C" void FillPayload(const std::uint64_t* ranks,
    std::size_t rows,
    std::size_t valueLen,
    std::uint64_t totalRows,
    int randomStyle,
    int changed,
    char* output)
{
    constexpr std::uint64_t Seed = 10120261112ULL;
    for (std::size_t rowIndex = 0; rowIndex < rows; ++rowIndex) {
        auto rowID = ranks[rowIndex];
        if (randomStyle != 0) {
            rowID = (rowID * 6364136223846793005ULL + 1442695040888963407ULL) & (totalRows - 1);
        }
        for (std::size_t wordIndex = 0; wordIndex * 8 < valueLen; ++wordIndex) {
            auto value = rowID * 0x9E3779B97F4A7C15ULL + wordIndex * 0xD1B54A32D192ED03ULL + Seed;
            value ^= value >> 30;
            value *= 0xBF58476D1CE4E5B9ULL;
            value ^= value >> 27;
            value *= 0x94D049BB133111EBULL;
            value ^= value >> 31;
            for (std::size_t byteIndex = 0; byteIndex < 8 && wordIndex * 8 + byteIndex < valueLen; ++byteIndex) {
                output[rowIndex * valueLen + wordIndex * 8 + byteIndex] =
                    static_cast<char>(((value >> (byteIndex * 8)) & 63) + 32);
            }
        }
        if (changed != 0) {
            output[rowIndex * valueLen] = '~';
        }
    }
}
// NOLINTEND(bugprone-easily-swappable-parameters)
