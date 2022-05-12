
configure_file(
    mkr_format/mkr_format/_version.py.in
    ${CMAKE_CURRENT_SOURCE_DIR}/mkr_format/mkr_format/_version.py
)

set(wheel_output_stub "${CMAKE_BINARY_DIR}/wheel.touch")

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
        -D "PYTHON_PROJECT_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
        -D "PYBIND_INPUT_LIB=$<TARGET_FILE:mkr_format_pybind>"
        -D "WHEEL_OUTPUT_DIR=${wheel_output_dir}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/build_wheels.cmake"
    DEPENDS
        mkr_format_pybind
    VERBATIM
)

add_custom_target(mkr_format_python_wheel ALL
    SOURCES
        build_wheels.cmake
    DEPENDS
        "${wheel_output_stub}"
)

install(
    DIRECTORY "${wheel_output_dir}/"
    DESTINATION "."
    COMPONENT wheel
    FILES_MATCHING PATTERN "mkr_format*.whl"
)