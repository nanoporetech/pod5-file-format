#!/bin/bash -e

# Parse args.
if [ $# -ne 1 ]; then
    echo "Usage: $0 build_dir"
    exit 1
fi
build_dir=$(realpath "$1")

# Set up the venv.
echo "Setting up venv"
if [ ! -e .coverage_venv ]; then
    python3 -m venv .coverage_venv
fi
# shellcheck disable=SC1091 # "Not following: .coverage_venv/bin/activate was not specified as input"
source .coverage_venv/bin/activate
# --cobertura support added in 5.1.
pip install -U 'gcovr>=5.1'

# Determine the root of the project.
# Note: shellcheck wants these split up into separate lines.
project_root=$(realpath "$0")
project_root=$(dirname "${project_root}")
project_root=$(dirname "${project_root}")
cd "${project_root}"

function generate_coverage {
    test_name=$1
    regex=$2

    echo "Generating coverage report for ${test_name}"

    # Clear out old coverage info.
    find "${project_root}" -name "*.gcda" -delete

    # Run the test.
    # shellcheck disable=SC2086 # the regex is intentionally split
    ctest --test-dir "${build_dir}" ${regex}

    # Generate the coverage report for this test.
    gcovr --filter "${project_root}/c\+\+" --cobertura "${project_root}/coverage-report-${test_name}.xml"
    gcovr --filter "${project_root}/c\+\+" --html-single-page --html-details "${project_root}/coverage-report-${test_name}.html"
}

# Generate a report for each test.
for test_name in $(ctest --test-dir "${build_dir}" -N | sed -rn 's/^ +Test +#[0-9]+: +(.*)$/\1/p'); do
    generate_coverage "${test_name}" "-R ^${test_name}\$"
done

# Generate a full coverage report too.
generate_coverage "all" ""
