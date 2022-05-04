cmake_minimum_required(VERSION 3.16.0)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" "${CMAKE_SOURCE_DIR}/bass/cmake")

include(MKRVersion)

project(MKR
    VERSION ${MKR_NUMERIC_VERSION}
)

option(USE_CONAN "Use conan for dependency management" OFF)

if (USE_CONAN AND CMAKE_COMPILER_IS_GNUCXX AND
        CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "9.0" AND
        CMAKE_CXX_COMPILER_VERSION VERSION_LESS "10.0")
    # We build MKR on CentOS 7 in CI, where we have GCC 9 but only the pre-C++11 ABI
    # See https://gcc.gnu.org/onlinedocs/libstdc++/manual/using_dual_abi.html
    # This forces GCC 9 on other platforms (eg: Ubuntu Focal) to use the same ABI.
    # The main gain here is being able to use the same conan packages.
    add_compile_definitions(_GLIBCXX_USE_CXX11_ABI=0)
endif()

# Enable finding conan modules:
set(CMAKE_PREFIX_PATH "${CMAKE_BINARY_DIR}")
include_directories("third_party/include")

foreach (config "Release" "Debug")
    string(TOUPPER "${config}" config_upper)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${config}/lib)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${config}/lib)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${config}/bin)
endforeach()

# debug symbols don't depend on the build type, only on this option
option(DISABLE_DEBUG_SYMBOLS "Force debug symbols to be disabled" OFF)
if (NOT DISABLE_DEBUG_SYMBOLS)
    if (MSVC)
        # Z7 embeds deubgging info into .obj files, which is easier to manage for
        # build accelerators (note that a .pdb will still be generated for libs)
        # https://docs.microsoft.com/en-us/cpp/build/reference/z7-zi-zi-debug-information-format
        add_compile_options(/Z7)
        # this will use fastlink in the IDE and full link from the command link
        # https://docs.microsoft.com/en-us/cpp/build/reference/debug-generate-debug-info
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
        set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
        # /DEBUG option is not recognised for STATIC lib linking
    elseif (CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        add_compile_options(-g)
    endif()
endif()

include(GenerateExportHeader)

enable_testing()

add_subdirectory(c++)
add_subdirectory(python)

include(mkr_packaging)