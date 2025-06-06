#!/bin/bash

# Exit on error
set -e

# Create a uv virtual environment
echo "Creating virtual environment at $(pwd)/scripts/airflow3/venv-af3"
uv venv "$(pwd)/scripts/airflow3/venv-af3"

# Install dependencies in the virtual environment
echo "Installing dependencies..."
uv pip install --python "$(pwd)/scripts/airflow3/venv-af3/bin/python" --pre -r "$(pwd)/scripts/airflow3/requirements.txt"

uv pip install --python "$(pwd)/scripts/airflow3/venv-af3/bin/python" ".[test]"

echo "uv virtual environment setup and dependencies installed successfully!"
