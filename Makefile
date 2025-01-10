.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: ## Setup development environment
	python -m venv venv
	. venv/bin/activate && pip --no-cache-dir install ".[tests]"
	@echo "To activate the virtual environment, run:"
	@echo "source venv/bin/activate"

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
	rm -rf dev/include/*
	rm -rf dist/*
	hatch build
	cp dist/* dev/include/

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
