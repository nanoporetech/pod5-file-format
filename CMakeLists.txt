cmake_minimum_required(VERSION 3.16.0)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" "${CMAKE_SOURCE_DIR}/bass/cmake")

include(MKRVersion)

project(MKR
    VERSION ${MKR_NUMERIC_VERSION}
)

option(FORCE_CI_COMPATIBILITY "Change compiler flags to ensure compatibility with CI builds" ON)
if (FORCE_CI_COMPATIBILITY AND CMAKE_COMPILER_IS_GNUCXX AND
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

include(GenerateExportHeader)

enable_testing()

add_subdirectory(c++)
add_subdirectory(python)

include(mkr_packaging)