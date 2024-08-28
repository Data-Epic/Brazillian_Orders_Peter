# Brazilian Orders ETL and API

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline application for the Brazilian E-Commerce Public Dataset by Olist. It uses Polars for data processing and DuckDB as the target database. The entire pipeline, including data ingestion scripts, unit tests, and the DuckDB database, is containerized using Docker for easy deployment and scalability. A Flask application provides API endpoints for data processing and retrieval, and a CI/CD pipeline automates deployment to an EC2 instance.

## Features

Data extraction from multiple CSV files in the Olist dataset
Data transformation and modeling to create dimensional and fact tables
Data analysis to generate insightful aggregate tables
Data loading into a DuckDB database
RESTful API endpoints for data uploading, transformation, and retrieval
Comprehensive unit tests for data preprocessing, database operations, and API endpoints
Full dockerization of the ETL pipeline for reproducibility and portability
CI/CD pipeline using GitHub Actions for automated deployment to EC2

## Prerequisites

Docker and Docker Compose
Git
AWS account (for EC2 deployment)

## Quick Start

1. Clone the repository:
git clone https://github.com/Data-Epic/Brazillian_Orders_Peter.git
cd Brazillian_Orders_Peter

2. Download the dataset:
Get the dataset from Kaggle
Unzip the downloaded file
Create a data/ directory in the project root
Place all CSV files in the data/ directory

3. Create a GitHub repository and a self-hosted runner:
Create a new GitHub repository and connect it to your local project
Set up a self-hosted GitHub Actions runner on your EC2 instance

4. Configure GitHub Secrets:
Add the following AWS EC2 and Docker Hub configurations to your GitHub secrets:
EC2_SSH_PRIVATE_KEY=your_instance_private_key_content
EC2_HOST=your_instance_host
EC2_USER=your_instance_user_name
DOCKER_USERNAME=your_docker_username
DOCKER_PASSWORD=your_docker_password

5. Use the API:
Load tables by uploading CSV files to the data loading endpoints
View loaded and analytical tables using the data retrieval endpoints
Process and analyze tables by executing the data processing endpoints

## Deployed Application URL

The application is deployed to an EC2 instance and can be accessed at: http://18.223.169.57:8080/api/docs/#/


## Project Structure

Brazillian_Orders_Peter/
│
├── .github/
│   └── workflows/
│       └── ci-cd.yml           # CI/CD Workflow configuration file
│
├── data/                       # Input CSV files
│
├── src/
│   ├── static/                 # Static files for the web application
│   ├── uploads/                # Directory for uploaded files
│   ├── init.py
│   ├── api.py                  # API endpoints
│   ├── database.py             # Database operations
│   ├── main.py                 # Main application file
│   ├── processing.py           # Data processing logic
│   └── utils.py                # Utility functions
│
├── tests/                      # Unit tests
│
├── .gitignore                  # Git ignore file
├── Dockerfile                  # Docker configuration
├── docker-compose.yml          # Docker Compose configuration
├── entrypoint.sh               # Bash scripts for container entry point
├── requirements.txt            # Python dependencies
├── README.md                   # Project documentation
└── run_tests.py                # Script to run all tests

## API Endpoints

Detailed documentation of API endpoints will be available at /api/docs when the application is running.

## Customization

Updating Dependencies
If you need to update or add dependencies:
Modify the requirements.txt file

## GitHub Secrets

Ensure that you have added the required GitHub secrets as mentioned in the "Quick Start" section.
Deployment
The application is automatically deployed to an EC2 instance via GitHub Actions when changes are pushed to the orders_api_deployment branch. The deployment process includes the following steps:

- Logging in to Docker Hub
- Building and tagging the Docker image
- Pushing the image to Docker Hub
- Connecting to the EC2 instance via SSH
- Pulling the latest Docker image from Docker Hub
- Removing the existing container (if any)
- Running the new container with the updated image

## Troubleshooting

Ensure all required CSV files are in the data/ directory
Check Docker logs for any error messages
Ensure that Docker and GitHub Actions runner are installed and set up correctly on the EC2 instance
Verify the GitHub secrets are configured correctly