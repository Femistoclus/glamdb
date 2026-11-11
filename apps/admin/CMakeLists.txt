add_executable(glamdb_admin main.cpp)
set_target_properties(glamdb_admin PROPERTIES OUTPUT_NAME glamdb-admin)
target_link_libraries(glamdb_admin PRIVATE glamdb_build_options glamdb_core glamdb_transport CLI11::CLI11)
install(TARGETS glamdb_admin RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
