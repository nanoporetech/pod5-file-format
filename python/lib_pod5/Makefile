SHELL = /bin/bash
PYTHON ?= python3.7

envDir = venv
envPrompt ?= "lib-pod5"

.PHONY: clean venv update install

# Clean the python virtual environment
clean:
	rm -rf ${envDir}

# Create a python virtual environment
venv:
	${PYTHON} -m venv ${envDir} --prompt=${envPrompt}

# install the python project in the current venv
update:
	source ${envDir}/bin/activate \
	&& pip install --upgrade pip \
	&& pip install -e .[dev]

# Completely re-install the python environment for development
install: clean venv update
	@echo "To activate your new environment:  source ${envDir}/bin/activate"

# Build the wheel
wheel: update
	source ${envDir}/bin/activate \
	&& python -m build
