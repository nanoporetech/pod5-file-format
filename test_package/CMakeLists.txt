cmake_minimum_required(VERSION 3.8)
project(test_package CXX)

# Test components
find_package(pod5_file_format REQUIRED CONFIG)
add_executable(${PROJECT_NAME} test_package.cpp test_cpp_api.cpp)

target_include_directories(${PROJECT_NAME}
    PUBLIC
        ${CMAKE_SOURCE_DIR}/../third_party/include
)

set_target_properties(${PROJECT_NAME}
    PROPERTIES
        CXX_STANDARD 14
)

target_link_libraries(${PROJECT_NAME} pod5_file_format::pod5_file_format)
