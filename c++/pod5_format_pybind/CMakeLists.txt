
pybind11_add_module(pod5_format_pybind
    api.h
    bindings.cpp
    utils.h

    repack/repack_functions.h
    repack/repack_states.h
    repack/repack_utils.h
    repack/repack_output.cpp
    repack/repack_output.h
    repack/repacker.cpp
    repack/repacker.h
)

target_link_libraries(pod5_format_pybind
    PRIVATE
        pod5_format
)


set_target_properties(pod5_format_pybind
    PROPERTIES
        POSITION_INDEPENDENT_CODE 1
        CXX_STANDARD 14
)

set(python_project_root "${CMAKE_SOURCE_DIR}/python/lib_pod5/")

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/_version.py.in
    ${python_project_root}/src/lib_pod5/_version.py
)

set(wheel_output_stub "${CMAKE_CURRENT_BINARY_DIR}/wheel.touch")

set(wheel_output_dir "${CMAKE_CURRENT_BINARY_DIR}/wheel_${POD5_FULL_VERSION}")
file(MAKE_DIRECTORY ${wheel_output_dir})

add_custom_command(
    OUTPUT "${wheel_output_stub}"
    COMMAND ${CMAKE_COMMAND}
    ARGS
        -D "PYTHON_EXECUTABLE=${Python_EXECUTABLE}"
        -D "PYTHON_PROJECT_DIR=${python_project_root}"
        -D "PYBIND_INPUT_LIB=$<TARGET_FILE:pod5_format_pybind>"
        -D "WHEEL_OUTPUT_DIR=${wheel_output_dir}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/build_wheel.cmake"
    DEPENDS
        pod5_format_pybind
    VERBATIM
)

add_custom_target(lib_pod5_python_wheel ALL
    SOURCES
        build_wheel.cmake
    DEPENDS
        "${wheel_output_stub}"
)

install(
    DIRECTORY "${wheel_output_dir}/"
    DESTINATION "."
    COMPONENT wheel
    FILES_MATCHING PATTERN "lib_pod5*.whl"
)
