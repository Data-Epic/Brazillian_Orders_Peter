#!/bin/bash
set -e
echo "Running tests..."
poetry run python run_tests.py
echo "Starting the application..."
poetry run python src/main.py
tail -f /dev/null
