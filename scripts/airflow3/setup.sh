#!/bin/bash

# Exit on error
set -e

# Create a UV virtual environment named 'env' (you can change this as needed)
echo "Creating virtual environment at $(pwd)/tools"
python3 -m venv "$(pwd)/scripts/airflow3/venv-af3"

# Activate the virtual environment
echo "Activating virtual environment..."
source "$(pwd)/scripts/airflow3/venv-af3/bin/activate"

# Install dependencies in the virtual environment
echo "Installing dependencies..."
pip3 install --pre -r "$(pwd)/scripts/airflow3/requirements.txt"

pip3 install ".[test]"

echo "UV virtual environment setup and dependencies installed successfully!"
