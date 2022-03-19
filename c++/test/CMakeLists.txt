
add_executable(mkr_unit_test
    main.cpp
    signal_table_tests.cpp
)

target_link_libraries(mkr_unit_test
    PUBLIC
        mkr_format
)

add_test(
    NAME mkr_unit_test
    COMMAND mkr_unit_test
)