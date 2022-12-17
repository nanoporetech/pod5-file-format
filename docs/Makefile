# Run this Makefile from project root with: make -f docs/Makefile
SHELL = /bin/bash
PYTHON ?= python3.7

envDir = venv
envPrompt ?= "docs"

.PHONY: clean install update docs

# Clean the python virtual environment
clean:
	rm -rf ${envDir}

venv:
	${PYTHON} -m venv --prompt=${envPrompt} ${envDir}

update:
	source ${envDir}/bin/activate \
	&& pip install -U pip \
	&& pip install -r docs/requirements.txt

# Clean existing docs and re-build using the same command as used by RTD
docs:
	rm -rf _build/ public/
	source ${envDir}/bin/activate \
	&& python -m sphinx -T -E -b html -d _build/doctrees -D language=en docs/ _build/html

# Completely install the python environment for development
install: clean venv update

# Re-install and create docs
all: install docs
