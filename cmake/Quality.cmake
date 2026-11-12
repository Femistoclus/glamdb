find_package(Python3 3.10 REQUIRED COMPONENTS Interpreter)

foreach(check IN ITEMS format format-check tidy-check quality-negative-check)
    set(quality_args)
    if(GLAMDB_IO_ONLY)
        list(APPEND quality_args --io-only)
    endif()
    if(check STREQUAL "tidy-check" AND NOT GLAMDB_BUILD_STORAGE)
        list(APPEND quality_args --exclude-dir core/storage --exclude-dir tests/unit/storage
            --exclude-dir core/backend --exclude-dir core/scan --exclude-dir tests/vertical)
    endif()
    if(check STREQUAL "tidy-check" AND NOT GLAMDB_BUILD_IO_PROBE)
        list(APPEND quality_args --exclude-dir probes/io)
    endif()
    if(check STREQUAL "tidy-check" AND NOT GLAMDB_BUILD_ADMIN)
        list(APPEND quality_args --exclude-dir apps/admin)
    endif()
    add_custom_target(${check}
        COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/scripts/quality.py"
                "${check}" --build-dir "${PROJECT_BINARY_DIR}" ${quality_args}
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
        USES_TERMINAL
        VERBATIM
    )
endforeach()
