
add_executable(pod5_unit_tests
    main.cpp
    c_api_tests.cpp
    c_api_build_test.c
    file_reader_writer_tests.cpp
    output_stream_tests.cpp
    read_table_writer_utils_tests.cpp
    read_table_tests.cpp
    run_info_table_tests.cpp
    schema_tests.cpp
    signal_compression_tests.cpp
    signal_table_tests.cpp
    svb16_scalar_tests.cpp
    svb16_x64_tests.cpp
    test_utils.h
    thread_pool_tests.cpp
    utils.h
    uuid_tests.cpp
)

# Link the C++ filesystem library if needed. Before GCC 9, support for the std::filesystem
# library was considered to be experimental. Therefore it exists in a separate library
# which needs to be opted into.
if (CMAKE_COMPILER_IS_GNUCXX AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.0")
    target_link_libraries(pod5_unit_tests PRIVATE stdc++fs)
endif()

if (${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
    set_source_files_properties(c_api_build_test.c PROPERTIES COMPILE_OPTIONS "-Wdocumentation")
endif()

target_link_libraries(pod5_unit_tests
    PUBLIC
        pod5_format
        ${maybe_public_libs}
)

set_property(TARGET pod5_unit_tests PROPERTY CXX_STANDARD 20)
if (NOT MSVC)
    target_compile_options(pod5_unit_tests PRIVATE ${pod5_warning_options})
endif()

add_test(
    NAME pod5_unit_tests
    COMMAND pod5_unit_tests
)
