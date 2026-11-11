# Copies a pinned submodule into a build-private source tree and applies the
# GlamDB patches recorded for it. The submodule itself stays unmodified (it may
# be mounted read-only). Usage:
#   cmake -DSOURCE=<submodule> -DDESTINATION=<copy> -DPATCH_DIR=<dir> -P PrepareSource.cmake

foreach(variable IN ITEMS SOURCE DESTINATION PATCH_DIR)
    if(NOT DEFINED ${variable} OR "${${variable}}" STREQUAL "")
        message(FATAL_ERROR "PrepareSource.cmake requires -D${variable}=...")
    endif()
endforeach()
if(NOT EXISTS "${SOURCE}/CMakeLists.txt")
    message(FATAL_ERROR "Missing dependency source: ${SOURCE}")
endif()

find_program(GLAMDB_GIT git REQUIRED)
file(REMOVE_RECURSE "${DESTINATION}")
file(MAKE_DIRECTORY "${DESTINATION}")
# The submodule's .git file points into the superproject; the copy is not a checkout.
file(REAL_PATH "${SOURCE}" source_directory)
file(COPY "${source_directory}/" DESTINATION "${DESTINATION}" PATTERN ".git" EXCLUDE)

file(GLOB patches LIST_DIRECTORIES false "${PATCH_DIR}/*.patch")
list(SORT patches)
get_filename_component(ceiling "${DESTINATION}" DIRECTORY)
foreach(patch IN LISTS patches)
    # The ceiling keeps git from treating an enclosing repository as the target.
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env "GIT_CEILING_DIRECTORIES=${ceiling}"
            "${GLAMDB_GIT}" apply --whitespace=nowarn "${patch}"
        WORKING_DIRECTORY "${DESTINATION}"
        RESULT_VARIABLE result
        ERROR_VARIABLE error)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "Cannot apply ${patch} to ${DESTINATION}: ${error}")
    endif()
    message(STATUS "Applied ${patch}")
endforeach()
