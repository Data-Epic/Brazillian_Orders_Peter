#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status
echo "Running tests..."
python run_tests.py
echo "Starting the application..."
python src/main.py &
echo "Keeping the container alive..."
tail -f /dev/null