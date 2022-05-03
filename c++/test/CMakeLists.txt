
add_executable(mkr_unit_tests
    main.cpp
    c_api_tests.cpp
    c_api_build_test.c
    file_reader_writer_tests.cpp
    read_table_writer_utils_tests.cpp
    read_table_tests.cpp
    signal_compression_tests.cpp
    signal_table_tests.cpp
    svb16_basic_tests.cpp
    utils.h
)

target_link_libraries(mkr_unit_tests
    PUBLIC
        mkr_format
)

set_property(TARGET mkr_unit_tests PROPERTY CXX_STANDARD 14)
#target_compile_options(mkr_unit_tests PRIVATE -Wall -Werror)

add_test(
    NAME mkr_unit_tests
    COMMAND mkr_unit_tests
)