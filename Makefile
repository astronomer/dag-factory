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
venv/bin/activate: setup.py
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

.PHONY: fmt
fmt: venv ## Formats all files with black
	@echo "==> Formatting with Black"
	@${PYTHON} -m black dagfactory

.PHONY: fmt-check
fmt-check: venv ## Checks files were formatted with black
	@echo "==> Formatting with Black"
	@${PYTHON} -m black --check dagfactory

.PHONY: lint
lint: venv ## Lint code with pylint
	@${PYTHON} -m pylint dagfactory

.PHONY: test
test: venv ## Runs unit tests
	@${PYTHON} -m tox

.PHONY: docker-build
docker-build:
	@echo "==> Building docker image for local testing"
	@docker build -t dag_factory:latest .

.PHONY: docker-run
docker-run: docker-build ## Runs local Airflow for testing
	@docker run -d -e AIRFLOW__CORE__DAGS_FOLDER=/usr/local/airflow/dags -v $(PWD)/examples:/usr/local/airflow/dags -p 127.0.0.1:8080:8080 --name=dag_factory dag_factory:latest
	@echo "==> Airflow is running at http://localhost:8080"

.PHONY: docker-stop
docker-stop: ## Stop Docker container
	@docker stop dag_factory; docker rm dag_factory