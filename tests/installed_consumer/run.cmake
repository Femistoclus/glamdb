if(NOT DEFINED TEST_STORAGE_ENABLED)
    message(FATAL_ERROR "Installed consumer requires an explicit storage expectation")
endif()
file(REMOVE_RECURSE "${TEST_BINARY_DIR}")
file(MAKE_DIRECTORY "${TEST_BINARY_DIR}/work")
file(COPY "${TEST_SOURCE_DIR}/CMakeLists.txt" "${TEST_SOURCE_DIR}/main.cpp"
    DESTINATION "${TEST_BINARY_DIR}/source")
execute_process(COMMAND "${CMAKE_COMMAND}" --install "${ENGINE_BINARY_DIR}"
    --prefix "${TEST_BINARY_DIR}/prefix" RESULT_VARIABLE result OUTPUT_QUIET)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Engine installation failed: ${result}")
endif()
set(prefixes "${TEST_BINARY_DIR}/prefix;${DEPENDENCY_PREFIX}")
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${TEST_BINARY_DIR}/source" -B "${TEST_BINARY_DIR}/build"
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
if(TEST_STORAGE_ENABLED)
    set(storage_mode storage)
else()
    set(storage_mode nostorage)
endif()
foreach(phase IN ITEMS write reopen)
    execute_process(COMMAND "${TEST_BINARY_DIR}/build/consumer" "${phase}"
        "${TEST_BINARY_DIR}/work/store" "${TEST_BINARY_DIR}/work/receipts.txt" "${storage_mode}"
        WORKING_DIRECTORY "${TEST_BINARY_DIR}/work" RESULT_VARIABLE result)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "Installed consumer ${storage_mode} ${phase} failed: ${result}")
    endif()
endforeach()
