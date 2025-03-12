function(make_fuzzer TYPE)
    # Setup the target
    set(NAME fuzzer_${TYPE})
    set(SRC fuzz_${TYPE}.cpp)
    add_executable(${NAME} ${SRC})
    target_link_libraries(${NAME} PUBLIC pod5_format)
    target_link_options(${NAME} PUBLIC -fsanitize=fuzzer)

    # Extract the corpus.
    set(CORPUS ${CMAKE_CURRENT_SOURCE_DIR}/corpus_${TYPE})
    file(ARCHIVE_EXTRACT INPUT ${CORPUS}.zip DESTINATION ${CORPUS})

    # Add a quick smoke test
    set(TEST_NAME ${NAME}_quick_check)
    add_test(
        NAME ${TEST_NAME}
        COMMAND ${NAME} -jobs=3 -max_total_time=500 -timeout=100 ${CORPUS}
        # Run in the current working directory so that failing cases are dumped there.
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )

    # Make sure that all issues get picked up
    set(SANITIZER_OPTIONS
        # Note that we have to disable |detect_leaks| since arrow will allocate persistent state.
        "ASAN_OPTIONS=string_append::detect_stack_use_after_return=1:check_initialization_order=1:detect_leaks=0"
        # TODO: halt_on_error=1
        "UBSAN_OPTIONS=string_append::print_stacktrace=1:halt_on_error=0"
    )
    set_tests_properties(${TEST_NAME} PROPERTIES ENVIRONMENT_MODIFICATION "${SANITIZER_OPTIONS}")
endfunction()

make_fuzzer(compress)
make_fuzzer(file)
