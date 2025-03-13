set(FUZZER_RUN_TIME 0 CACHE STRING "How long to run each fuzzer for. 0 indicates no limit")

# The fuzzer only uses nproc/2 workers, so limit the number of jobs to that.
include(ProcessorCount)
ProcessorCount(NUM_FUZZER_JOBS)
math(EXPR NUM_FUZZER_JOBS "${NUM_FUZZER_JOBS} / 2")

# Make sure that all issues get picked up
set(FUZZ_SANITIZER_OPTIONS
    # Note that we have to disable |detect_leaks| since arrow will allocate persistent state.
    "ASAN_OPTIONS=string_append::detect_stack_use_after_return=1:check_initialization_order=1:detect_leaks=0"
    # TODO: halt_on_error=1
    "UBSAN_OPTIONS=string_append::print_stacktrace=1:halt_on_error=0"
)

function(make_fuzzer TYPE)
    # Extract the corpus.
    set(CORPUS ${CMAKE_CURRENT_SOURCE_DIR}/corpus_${TYPE})
    file(ARCHIVE_EXTRACT INPUT ${CORPUS}.zip DESTINATION ${CORPUS})

    if (ENABLE_FUZZERS)
        # Setup the target
        set(NAME fuzzer_${TYPE})
        add_executable(${NAME} fuzz_${TYPE}.cpp)
        target_link_libraries(${NAME} PUBLIC pod5_format)
        target_link_options(${NAME} PUBLIC -fsanitize=fuzzer)

        # Add a test for it
        add_test(
            NAME ${NAME}
            COMMAND ${NAME} -jobs=${NUM_FUZZER_JOBS} -max_total_time=${FUZZER_RUN_TIME} -timeout=100 ${CORPUS}
            # Run in the current working directory so that failing cases are dumped there.
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
        set_tests_properties(${NAME} PROPERTIES ENVIRONMENT_MODIFICATION "${FUZZ_SANITIZER_OPTIONS}")
    endif()

    include(CheckIncludeFileCXX)
    check_include_file_cxx("filesystem" FILESYSTEM_AVAILABLE)
    if (FILESYSTEM_AVAILABLE)
        # Add a runner for the fuzzer in non-fuzzing builds.
        set(NAME fuzz_runner_${TYPE})
        add_executable(${NAME}
            fuzz_${TYPE}.cpp
            runner.cpp
        )
        target_link_libraries(${NAME} PRIVATE pod5_format)
        target_compile_features(${NAME} PRIVATE cxx_std_17)
        target_compile_options(${NAME} PRIVATE -UNDEBUG)
        add_test(
            NAME ${NAME}
            COMMAND ${NAME} ${CORPUS}
        )
        set_tests_properties(${NAME} PROPERTIES ENVIRONMENT_MODIFICATION "${FUZZ_SANITIZER_OPTIONS}")
    else()
        message(WARNING "Not building fuzz runner due to lack of <filesystem>")
    endif()
endfunction()

make_fuzzer(compress)
make_fuzzer(file)
