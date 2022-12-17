SHELL = /bin/bash
PYTHON ?= python3.7

envDir = venv
envPrompt ?= "pod5"

.PHONY: clean install update docs

# Clean the python virtual environment
clean:
	rm -rf ${envDir}

# Completely install the python environment for development
install: clean
	${PYTHON} -m venv --prompt=${envPrompt} ${envDir}
	source ${envDir}/bin/activate \
	&& pip install --upgrade pip \
	&& pip install -e .[dev] \
	&& pre-commit install

	@echo "To activate your new environment:  source ${envDir}/bin/activate"

# Re-install the pod5 environments to refresh / update the environment with changes
update:
	source ${envDir}/bin/activate \
	&& pip install -e .[dev]

	@echo "Updated python environment"
