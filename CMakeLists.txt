cmake_minimum_required(VERSION 3.16.0)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" "${CMAKE_SOURCE_DIR}/bass/cmake")

include(MKRVersion)

project(MKR
    VERSION ${MKR_NUMERIC_VERSION}
)

add_subdirectory(c++)