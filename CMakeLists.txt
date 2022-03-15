cmake_minimum_required(VERSION 3.16.0)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" "${CMAKE_SOURCE_DIR}/bass/cmake")

include(MKRVersion)

project(MKR
    VERSION ${MKR_NUMERIC_VERSION}
)

# Enable finding conan modules:
set(CMAKE_PREFIX_PATH "${CMAKE_BINARY_DIR}")
include_directories("third_party/include")

enable_testing()

add_subdirectory(c++)