
pybind11_add_module(pod5_format_pybind
    api.h
    bindings.cpp
    utils.h
    subset.cpp
    subset.h

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

if (NOT MSVC)
    target_compile_options(pod5_format_pybind PRIVATE ${pod5_warning_options})
endif()

set_target_properties(pod5_format_pybind
    PROPERTIES
        POSITION_INDEPENDENT_CODE 1
        CXX_STANDARD 17
)

# Non-conan license files to copy.
set(pod5_cxx_licenses_src
    "${CMAKE_SOURCE_DIR}/LICENSE.md"
    "${CMAKE_SOURCE_DIR}/third_party/licenses/gsl-lite.txt"
    "${CMAKE_SOURCE_DIR}/third_party/pybind11/LICENSE"
)
# Destination name for the above files.
set(pod5_cxx_licenses_dst
    "LICENSE.md"
    "gsl-lite.txt"
    "pybind11.txt"
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
        -D "POD5_CONAN_LICENSES=${CMAKE_BINARY_DIR}/pod5_conan_licenses"
        -D "POD5_CXX_LICENSES_SRC=${pod5_cxx_licenses_src}"
        -D "POD5_CXX_LICENSES_DST=${pod5_cxx_licenses_dst}"
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
