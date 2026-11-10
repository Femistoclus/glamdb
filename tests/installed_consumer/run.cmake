if(NOT EXPECT_STORAGE MATCHES "^(ON|OFF)$")
    message(FATAL_ERROR "Installed consumer requires an explicit EXPECT_STORAGE=ON or OFF")
endif()
file(REMOVE_RECURSE "${TEST_BINARY_DIR}")
file(MAKE_DIRECTORY "${TEST_BINARY_DIR}/work")
execute_process(COMMAND "${CMAKE_COMMAND}" --install "${ENGINE_BINARY_DIR}"
    --prefix "${TEST_BINARY_DIR}/prefix" RESULT_VARIABLE result OUTPUT_QUIET)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Engine installation failed: ${result}")
endif()
set(prefixes "${TEST_BINARY_DIR}/prefix;${DEPENDENCY_PREFIX}")
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${TEST_SOURCE_DIR}" -B "${TEST_BINARY_DIR}/build"
    -G "${TEST_GENERATOR}" "-DCMAKE_PREFIX_PATH=${prefixes}" "-DCMAKE_TOOLCHAIN_FILE=${TEST_TOOLCHAIN}"
    "-DCMAKE_CXX_COMPILER=${TEST_CXX_COMPILER}"
    "-DCMAKE_CXX_FLAGS=${TEST_CXX_FLAGS}" "-DCMAKE_EXE_LINKER_FLAGS=${TEST_LINKER_FLAGS}"
    RESULT_VARIABLE result)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Installed consumer configure failed: ${result}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" --build "${TEST_BINARY_DIR}/build" RESULT_VARIABLE result)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Installed consumer build failed: ${result}")
endif()
execute_process(COMMAND "${TEST_BINARY_DIR}/build/consumer" "${TEST_BINARY_DIR}/work/store"
    "${EXPECT_STORAGE}"
    RESULT_VARIABLE result)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Installed consumer failed: ${result}")
endif()
