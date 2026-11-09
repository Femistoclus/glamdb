option(GLAMDB_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(GLAMDB_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

if(GLAMDB_ENABLE_ASAN OR GLAMDB_ENABLE_UBSAN)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang|AppleClang)$")
        message(FATAL_ERROR "Sanitizers require GCC or Clang")
    endif()

    set(glamdb_sanitizers)
    if(GLAMDB_ENABLE_ASAN)
        list(APPEND glamdb_sanitizers address)
    endif()
    if(GLAMDB_ENABLE_UBSAN)
        list(APPEND glamdb_sanitizers undefined)
    endif()
    list(JOIN glamdb_sanitizers "," glamdb_sanitizers)

    target_compile_options(glamdb_build_options INTERFACE
        "-fsanitize=${glamdb_sanitizers}" -fno-omit-frame-pointer
        -fno-sanitize-recover=all
    )
    target_link_options(glamdb_build_options INTERFACE
        "-fsanitize=${glamdb_sanitizers}" -fno-sanitize-recover=all
    )
endif()
