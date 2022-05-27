cmake_minimum_required(VERSION 3.8)
project(test_package CXX)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_output_dirs_setup()

# Test components
find_package(pod5_file_format REQUIRED CONFIG)
add_executable(${PROJECT_NAME} test_package.cpp)

target_link_libraries(${PROJECT_NAME} pod5_file_format::pod5_file_format)
