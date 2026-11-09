if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
    message(FATAL_ERROR "GlamDB requires GCC 14 or newer")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 18)
    message(FATAL_ERROR "GlamDB requires Clang 18 or newer")
endif()

add_library(glamdb_build_options INTERFACE)
target_compile_features(glamdb_build_options INTERFACE cxx_std_23)
target_compile_options(glamdb_build_options INTERFACE
    "$<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic;-Werror>"
    "$<$<CXX_COMPILER_ID:MSVC>:/W4;/WX;/permissive->"
)
