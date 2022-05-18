
message("Building python wheel using ${PYTHON_EXECUTABLE}")
message("  project dir ${PYTHON_PROJECT_DIR}")
message("  into ${WHEEL_OUTPUT_DIR}")

set(output_dir "./dist")

execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -m pip wheel . --wheel-dir ${WHEEL_OUTPUT_DIR} --no-deps
    WORKING_DIRECTORY "${PYTHON_PROJECT_DIR}/"
    RESULT_VARIABLE exit_code
    OUTPUT_VARIABLE output
    ERROR_VARIABLE output
)

if (NOT exit_code EQUAL 0)
    message(FATAL_ERROR "Could not generate wheel: ${output}")
endif()

file(GLOB pod5_wheel_names "${WHEEL_OUTPUT_DIR}/pod5_format*.whl")
foreach(wheel ${pod5_wheel_names})
    message("Built wheel ${wheel}")
endforeach()