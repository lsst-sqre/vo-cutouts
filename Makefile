.PHONY: update-deps
update-deps:
	pip install --upgrade pip-tools pip setuptools
	# Hashes are disabled until lsst.daf.butler is on PyPI.
	# pip-compile --upgrade --build-isolation --generate-hashes --output-file requirements/main.txt requirements/main.in
	# pip-compile --upgrade --build-isolation --generate-hashes --output-file requirements/dev.txt requirements/dev.in
	pip-compile --upgrade --build-isolation --output-file requirements/main.txt requirements/main.in
	pip-compile --upgrade --build-isolation --output-file requirements/dev.txt requirements/dev.in

.PHONY: init
init:
	pip install --editable .
	pip install --no-deps --upgrade -r requirements/main.txt -r requirements/dev.txt
	rm -rf .tox
	pip install --upgrade tox tox-docker
	pre-commit install

.PHONY: update
update: update-deps init

.PHONY: run
run:
	tox -e run
