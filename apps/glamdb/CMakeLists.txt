if(NOT EXISTS "${PROJECT_SOURCE_DIR}/third_party/cli11/CMakeLists.txt")
    message(FATAL_ERROR
        "The CLI requires the pinned CLI11 submodule. "
        "Run: git submodule update --init third_party/cli11"
    )
endif()
add_subdirectory("${PROJECT_SOURCE_DIR}/third_party/cli11"
                 "${PROJECT_BINARY_DIR}/third_party/cli11" EXCLUDE_FROM_ALL SYSTEM)

add_library(glamdb_json STATIC outcome_json.cpp)
target_link_libraries(glamdb_json PUBLIC glamdb::core PRIVATE glamdb_build_options)
target_include_directories(glamdb_json PRIVATE "${PROJECT_SOURCE_DIR}")
add_executable(glamdb main.cpp)
target_link_libraries(glamdb PRIVATE glamdb_build_options glamdb_json CLI11::CLI11)
target_include_directories(glamdb PRIVATE "${PROJECT_SOURCE_DIR}")
install(TARGETS glamdb RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
