cmake_minimum_required(VERSION 3.28)
project(GlamDBLibcxxAsanProbe LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(Arrow 25.0.0 EXACT CONFIG REQUIRED)
find_package(Iceberg CONFIG REQUIRED)

foreach(target libcxx_asan libcxx_asan_compatible)
    add_executable(${target} libcxx_asan.cpp)
    target_link_libraries(${target} PRIVATE iceberg::iceberg_bundle_static Arrow::arrow_static)
    target_compile_options(${target} PRIVATE
        -O0 -g -Wall -Wextra -Wpedantic -Werror -fsanitize=address,undefined)
    target_link_options(${target} PRIVATE -static-libstdc++ -fsanitize=address,undefined)
endforeach()
target_compile_definitions(libcxx_asan_compatible PRIVATE _LIBCPP_HAS_NO_ASAN)

enable_testing()
add_test(NAME glamdb.toolchain.mixed-annotations COMMAND libcxx_asan)
set_tests_properties(glamdb.toolchain.mixed-annotations PROPERTIES
    PASS_REGULAR_EXPRESSION "ERROR: AddressSanitizer: container-overflow")
add_test(NAME glamdb.toolchain.compatible-annotations COMMAND libcxx_asan_compatible)
set_tests_properties(glamdb.toolchain.mixed-annotations glamdb.toolchain.compatible-annotations
    PROPERTIES LABELS "toolchain-repro" TIMEOUT 30)
