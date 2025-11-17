
message("Building python lib-pod5 wheel using ${PYTHON_EXECUTABLE}")
message("  project dir ${PYTHON_PROJECT_DIR}")
message("  with lib ${PYBIND_INPUT_LIB}")
message("  with conan licenses ${POD5_CONAN_LICENSES}")
message("  with c++ licenses ${POD5_CXX_LICENSES_SRC}")
message("  into ${WHEEL_OUTPUT_DIR}")
message("  using: ${PYTHON_EXECUTABLE} -m pip wheel . --wheel-dir ${WHEEL_OUTPUT_DIR}")

# Copy the prebuilt lib into the wheel src.
file(COPY "${PYBIND_INPUT_LIB}" DESTINATION "${PYTHON_PROJECT_DIR}/src/lib_pod5")

# Copy the licenses into the wheel src.
# Note: the trailing / on src is important since it tells cmake to copy only the contents.
file(INSTALL "${POD5_CONAN_LICENSES}/" DESTINATION "${PYTHON_PROJECT_DIR}/licenses")
foreach(license_src license_dst IN ZIP_LISTS POD5_CXX_LICENSES_SRC POD5_CXX_LICENSES_DST)
    file(COPY_FILE "${license_src}" "${PYTHON_PROJECT_DIR}/licenses/${license_dst}")
endforeach()

execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -m pip wheel . --wheel-dir ${WHEEL_OUTPUT_DIR}
    WORKING_DIRECTORY "${PYTHON_PROJECT_DIR}/"
    RESULT_VARIABLE exit_code
    OUTPUT_VARIABLE output
    ERROR_VARIABLE output
)

if (NOT exit_code EQUAL 0)
    message(FATAL_ERROR "Could not generate wheel: ${output}")
endif()

file(GLOB pod5_wheel_names "${WHEEL_OUTPUT_DIR}/*.whl")
foreach(wheel ${pod5_wheel_names})
    message("Built wheel ${wheel}")
endforeach()
