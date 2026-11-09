add_test(NAME glamdb.smoke COMMAND glamdb)
set_tests_properties(glamdb.smoke PROPERTIES LABELS "smoke" TIMEOUT 10)
