
message("Building python wheel using ${PYTHON_EXECUTABLE}")
message("  project dir ${PYTHON_PROJECT_DIR}")
message("  with lib ${PYBIND_INPUT_LIB}")
message("  into ${WHEEL_OUTPUT_DIR}")

set(output_dir "./dist")

set(ENV{POD5_PYBIND_LIB} "${PYBIND_INPUT_LIB}")

set(EXTRA_ARGS)
if (NOT $ENV{POD5_OVERRIDE_WHEEL_PLAT_NAME} STREQUAL "")
    set(EXTRA_ARGS ${EXTRA_ARGS} --plat-name $ENV{POD5_OVERRIDE_WHEEL_PLAT_NAME})
endif()

file(COPY "${PYBIND_INPUT_LIB}" DESTINATION "${PYTHON_PROJECT_DIR}/pod5_format")

execute_process(
    COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wheel --dist-dir ${WHEEL_OUTPUT_DIR} ${EXTRA_ARGS}
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