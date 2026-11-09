find_package(Git REQUIRED)
get_filename_component(GLAMDB_SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)
file(READ "${GLAMDB_SOURCE_DIR}/third_party/lock.json" GLAMDB_DEPENDENCY_LOCK)
string(JSON dependency_count LENGTH "${GLAMDB_DEPENDENCY_LOCK}" submodules)
math(EXPR last_dependency "${dependency_count} - 1")

foreach(index RANGE ${last_dependency})
    string(JSON path GET "${GLAMDB_DEPENDENCY_LOCK}" submodules ${index} path)
    string(JSON expected GET "${GLAMDB_DEPENDENCY_LOCK}" submodules ${index} commit)
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${GLAMDB_SOURCE_DIR}/${path}" rev-parse HEAD
        RESULT_VARIABLE status
        OUTPUT_VARIABLE actual
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    if(NOT status EQUAL 0 OR NOT actual STREQUAL expected)
        message(FATAL_ERROR
            "${path} must be at ${expected}. Run git submodule update --init --recursive."
        )
    endif()
    message(STATUS "${path}: ${actual}")
endforeach()
