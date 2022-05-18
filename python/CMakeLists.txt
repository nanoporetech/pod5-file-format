
configure_file(
    pod5_format/pod5_format/_version.py.in
    ${CMAKE_CURRENT_SOURCE_DIR}/pod5_format/pod5_format/_version.py
)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/pod5_format/pod5_format/_version.py
    DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/pod5_format_tools/pod5_format_tools/
)
message("${CMAKE_CURRENT_SOURCE_DIR}/pod5_format_tools/pod5_format_tools/_version.py")

set(wheel_output_stub "${CMAKE_BINARY_DIR}/wheel.touch")
set(tools_wheel_output_stub "${CMAKE_BINARY_DIR}/tools_wheel.touch")

if (WIN32)
    set(os_str "win-x64")
elseif (APPLE AND CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "arm64")
    set(os_str "osx-arm64")
elseif (APPLE)
    set(os_str "osx64")
elseif (CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "aarch64")
    set(os_str "linux-arm64")
else()
    set(os_str "linux-x64")
endif()

set(wheel_output_dir "${CMAKE_CURRENT_BINARY_DIR}/wheel")
file(MAKE_DIRECTORY ${wheel_output_dir})

add_custom_command(
    OUTPUT "${wheel_output_stub}"
    COMMAND ${CMAKE_COMMAND}
    ARGS
        -D "PYTHON_EXECUTABLE=${Python_EXECUTABLE}"
        -D "PYTHON_PROJECT_DIR=${CMAKE_CURRENT_SOURCE_DIR}/pod5_format/"
        -D "PYBIND_INPUT_LIB=$<TARGET_FILE:pod5_format_pybind>"
        -D "WHEEL_OUTPUT_DIR=${wheel_output_dir}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/build_wheel.cmake"
    DEPENDS
        pod5_format_pybind
    VERBATIM
)

add_custom_target(pod5_format_python_wheel ALL
    SOURCES
        build_wheel.cmake
    DEPENDS
        "${wheel_output_stub}"
)

add_custom_command(
    OUTPUT "${tools_wheel_output_stub}"
    COMMAND ${CMAKE_COMMAND}
    ARGS
        -D "PYTHON_EXECUTABLE=${Python_EXECUTABLE}"
        -D "PYTHON_PROJECT_DIR=${CMAKE_CURRENT_SOURCE_DIR}/pod5_format_tools"
        -D "PYBIND_INPUT_LIB=$<TARGET_FILE:pod5_format_pybind>"
        -D "WHEEL_OUTPUT_DIR=${wheel_output_dir}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/build_wheel.cmake"
    DEPENDS
        pod5_format_pybind
    VERBATIM
)

add_custom_target(pod5_format_tools_python_wheel
    SOURCES
        build_wheel.cmake
    DEPENDS
        "${tools_wheel_output_stub}"
)

install(
    DIRECTORY "${wheel_output_dir}/"
    DESTINATION "."
    COMPONENT wheel
    FILES_MATCHING PATTERN "pod5_format*.whl"
)