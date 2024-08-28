#!/bin/bash
set -e  
echo "Running tests..."
python run_tests.py
echo "Starting the application..."
python src/main.py 
tail -f /dev/null
