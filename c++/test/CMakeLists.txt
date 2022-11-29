
add_executable(pod5_unit_tests
    main.cpp
    c_api_tests.cpp
    c_api_build_test.c
    file_reader_writer_tests.cpp
    read_table_writer_utils_tests.cpp
    read_table_tests.cpp
    run_info_table_tests.cpp
    schema_tests.cpp
    signal_compression_tests.cpp
    signal_table_tests.cpp
    svb16_scalar_tests.cpp
    svb16_x64_tests.cpp
    test_utils.h
    utils.h
)

target_link_libraries(pod5_unit_tests
    PUBLIC
        pod5_format
)

set_property(TARGET pod5_unit_tests PROPERTY CXX_STANDARD 14)
#target_compile_options(pod5_unit_tests PRIVATE -Wall -Werror)

add_test(
    NAME pod5_unit_tests
    COMMAND pod5_unit_tests
)
