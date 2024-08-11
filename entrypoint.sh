#!/bin/bash

# Start the application
python src/main.py

# Keep the container running
tail -f /dev/null

