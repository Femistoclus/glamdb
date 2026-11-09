find_package(Python3 3.9 REQUIRED COMPONENTS Interpreter)

foreach(check IN ITEMS format format-check tidy-check quality-negative-check)
    add_custom_target(${check}
        COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/scripts/quality.py"
                "${check}" --build-dir "${PROJECT_BINARY_DIR}"
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
        USES_TERMINAL
        VERBATIM
    )
endforeach()
