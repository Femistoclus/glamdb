add_executable(glamdb main.cpp)
target_link_libraries(glamdb PRIVATE glamdb_build_options)
install(TARGETS glamdb RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
