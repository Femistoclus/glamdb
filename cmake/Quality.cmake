find_package(Python3 3.9 REQUIRED COMPONENTS Interpreter)

foreach(check IN ITEMS format format-check tidy-check quality-negative-check)
    set(quality_args)
    if(GLAMDB_IO_ONLY)
        list(APPEND quality_args --io-only)
    endif()
    if(check STREQUAL "tidy-check" AND NOT GLAMDB_BUILD_STORAGE)
        list(APPEND quality_args --exclude-dir core/storage --exclude-dir tests/unit/storage)
    endif()
    add_custom_target(${check}
        COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/scripts/quality.py"
                "${check}" --build-dir "${PROJECT_BINARY_DIR}" ${quality_args}
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
        USES_TERMINAL
        VERBATIM
    )
endforeach()
