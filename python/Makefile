SHELL = /bin/bash
PYTHON ?= python3.9

envDir = venv
envPrompt ?= "pod5"

.PHONY: clean install update

# Clean the python virtual environment
clean: 
	rm -rf ${envDir}

# Completely install the python environment for development
install: clean
	${PYTHON} -m venv --prompt=${envPrompt} ${envDir}
	source ${envDir}/bin/activate \
	&& pip install --upgrade pip \
	&& pip install -e  pod5_format/[dev] pod5_format_tools/ \
	&& pre-commit install

	@echo "To activate your new environment:  source ${envDir}/bin/activate"

# Re-install the pod5 environments to refresh / update the environment with changes
update: 
	source ${envDir}/bin/activate \
	&& pip install -e  pod5_format/[dev] pod5_format_tools/

	@echo "Updated python environment"


