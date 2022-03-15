
add_executable(mkr_unit_test
    main.cpp
    mkr_test.cpp
)

add_test(
    NAME mkr_unit_test
    COMMAND mkr_unit_test
)