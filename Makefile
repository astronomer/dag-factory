PYTHON=venv/bin/python3

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: setup-dev
setup-dev: ## Setup development environment
	@pip3 install virtualenv
	@make venv

.PHONY: venv
venv: venv/bin/activate
venv/bin/activate: pyproject.toml
	@test -d venv || virtualenv -p python3 venv
	@${PYTHON} -m pip install -U pip
	@${PYTHON} -m pip install -e .[dev]
	@${PYTHON} -m pip install cattrs==1.0.0
	@touch venv/bin/activate

.PHONY: clean
clean: ## Removes build and test artifacts
	@echo "==> Removing build and test artifacts"
	@rm -rf *.egg *egg-info .cache .coverage .tox build bin include dist htmlcov lib .pytest_cache .venv
	@find . -name '*.pyc' -exec rm -f {} +
	@find . -name '*.pyo' -exec rm -f {} +
	@find . -name '*~' -exec rm -f {} +
	@find . -name '__pycache__' -exec rm -rf {} +


.PHONY: build-whl
build-whl: ## Build installable whl file
	python3 -m build --outdir dev/include/
	cd examples && ln -sf ../dev/dags/* .

.PHONY: docker-run
docker-run: build-whl ## Runs local Airflow for testing
	@if ! lsof -i :8080 | grep LISTEN > /dev/null; then \
		cd dev && astro dev start; \
	else \
		cd dev && astro dev restart; \
	fi

.PHONY: docker-stop
docker-stop: ## Stop Docker container
	cd dev && astro dev stop
