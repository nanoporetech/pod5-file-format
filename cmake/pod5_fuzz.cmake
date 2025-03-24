if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR
        "Only LLVM based compilers are supported for fuzzing. Assuming that "
        "'clang' is install, it can be picked by setting the environment "
        "variables 'CC=clang' and 'CXX=clang++' before invoking cmake."
    )
endif()

# Build everything with fuzzing instrumentation and sanitizers
set(POD5_SANITIZER_FLAGS -fsanitize=address,undefined,fuzzer-no-link)
add_compile_options(-g ${POD5_SANITIZER_FLAGS} -UNDEBUG -O1)
add_link_options(${POD5_SANITIZER_FLAGS})
