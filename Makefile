.EXPORT_ALL_VARIABLES:
PIPENV_VENV_IN_PROJECT = 1
PIPENV_IGNORE_VIRTUALENVS = 1

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.installed: Pipfile Pipfile.lock
	@echo "==> Pipfile(.lock) is newer than .installed, (re)installing"
	@pipenv install --dev
	@echo "This file is used by 'make' for keeping track of last install time. If Pipfile or Pipfile.lock are newer then this file (.installed) then all 'make *' commands that depend on '.installed' know they need to run pipenv install first." \
		> .installed

.PHONY: clean
clean: ## Removes build and test artifacts
	@echo "==> Removing build and test artifacts"
	@rm -rf *.egg *egg-info .cache .coverage .tox build bin include dist htmlcov lib .pytest_cache .venv .installed
	@find . -name '*.pyc' -exec rm -f {} +
	@find . -name '*.pyo' -exec rm -f {} +
	@find . -name '*~' -exec rm -f {} +
	@find . -name '__pycache__' -exec rm -rf {} +

.PHONY: fmt
fmt: .installed ## Formats all files with black
	@echo "==> Formatting with Black"
	@pipenv run black dagfactory

.PHONY: test
test: .installed ## Runs unit tests
	@pipenv run pytest tests -p no:warnings --verbose --color=yes

.PHONY: docker-build
docker-build:
	@echo "==> Building docker image for local testing"
	@docker build -t dag_factory:latest .

.PHONY: docker-run
docker-run: docker-build ## Runs local Airflow for testing
	@docker run -d -e AIRFLOW__CORE__DAGS_FOLDER=/usr/local/airflow/dags -v $(PWD)/examples:/usr/local/airflow/dags -p 8080:8080 --name=dag_factory dag_factory:latest
	@echo "==> Airflow is running at http://localhost:8080"

.PHONY: docker-stop
docker-stop: ## Stop Docker container
	@docker stop dag_factory; docker rm dag_factory