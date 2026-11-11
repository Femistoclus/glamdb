find_package(OpenSSL 1.1.1 REQUIRED)
add_library(glamdb_client http.cpp)
target_include_directories(glamdb_client PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../..")
target_link_libraries(glamdb_client PUBLIC glamdb_asio OpenSSL::SSL OpenSSL::Crypto)
target_compile_features(glamdb_client PUBLIC cxx_std_23)

if(TARGET glamdb_build_options)
    target_link_libraries(glamdb_client PRIVATE glamdb_build_options)
endif()
