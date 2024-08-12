# Brazilian Customers Orders ETL Pipeline Application

This project implements an ETL (Extract, Transform, Load) pipeline Application for the Brazilian E-Commerce Public Dataset by Olist. It uses Polars for data processing and DuckDB as the target database. The entire pipeline, including data ingestion scripts, unit tests, and the DuckDB database, is containerized using Docker for easy deployment and scalability.The Docker environment is designed to extract the  data and provide a command-line interface (CLI) for querying the data using DuckDB.
A Flask Application is created that encapsulates all processing and retrieval functionalities and a CI-CD pipeline is setup which autodeploys the flask API to an EC2 instance.

## Features

- Data uploading from multiple CSV files in the Olist dataset
- Data transformation and modeling to create dimensional and fact tables
- Aata analysis to generate insightful aggregate tables
- Data loading into a DuckDB database
- Restful API endpoints that encapsulates all data uploading, transformation and retrieval
- Comprehensive unit tests for data preprocessing database operations, and API endpoints
- Full dockerization of the ETL pipeline for reproducibility and portability
- CI/CD Pipeline using github actions for automated deployment to AWS EC2

## Prerequisites

- Docker and Docker Compose
- Git
- AWS account (for EC2 deployment)

## Quick Start

1. Clone the repository:
git clone https://github.com/Data-Epic/Brazillian_Orders_Peter.git

cd Brazillian_Orders_Peter

2. Download the dataset:
- Get the list dataset and information from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), unzip the downloaded zip file
- Create a `data/` directory 
- Place all CSV files in the `data/` directory

3. Build and run the Docker container:
docker-compose up --build

This will:
Build the Docker image based on the provided Dockerfile.
Install necessary dependencies.
Start the flask application.

4. In a new terminal, you can start a new container:
docker start -ai api-deployment-container

Certainly. I'll provide an improved version of your README file with better structure, more details, and clearer instructions. Here's an enhanced version:
markdownCopy# Brazilian Customers Orders ETL Pipeline Application

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline application for the Brazilian E-Commerce Public Dataset by Olist. It uses Polars for data processing and DuckDB as the target database. The entire pipeline, including data ingestion scripts, unit tests, and the DuckDB database, is containerized using Docker for easy deployment and scalability. A Flask application provides API endpoints for data processing and retrieval, and a CI/CD pipeline automates deployment to an EC2 instance.

## Features

- Data extraction from multiple CSV files in the Olist dataset
- Data transformation and modeling to create dimensional and fact tables
- Data analysis to generate insightful aggregate tables
- Data loading into a DuckDB database
- RESTful API endpoints for data uploading, transformation, and retrieval
- Comprehensive unit tests for data preprocessing, database operations, and API endpoints
- Full dockerization of the ETL pipeline for reproducibility and portability
- CI/CD pipeline using GitHub Actions for automated deployment to EC2

## Prerequisites

- Docker and Docker Compose
- Git
- AWS account (for EC2 deployment)

## Quick Start

1. Clone the repository:
git clone https://github.com/Data-Epic/Brazillian_Orders_Peter.git
cd Brazillian_Orders_Peter
Copy
2. Download the dataset:
- Get the dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Unzip the downloaded file
- Create a `data/` directory in the project root
- Place all CSV files in the `data/` directory

3. Build and run the Docker container:
docker-compose up --build
Copy
This will:
- Build the Docker image based on the provided Dockerfile
- Install necessary dependencies
- Start the Flask application

4. In a new terminal, you can start a new container:
docker start -ai api-deployment-container
Copy
5. Use the API:
- Load tables by uploading CSV files to the data loading endpoints
- View loaded and analytical tables using the data retrieval endpoints
- Process and analyze tables by executing the data processing endpoints

Input Data

- olist_customers_dataset.csv
- olist_order_items_dataset.csv
- olist_orders_dataset.csv
- olist_products_dataset.csv
- olist_sellers_dataset.csv
- product_category_name_translation.csv
- olist_order_payments_dataset.csv

Output Tables
The application generates the following analytical and aggregate tables in a DuckDB database:

fact_table
top_sellers
top_selling_product_category
order_status_count
top_average_delivery_time
loyal_customers

Project Structure
Certainly. I'll provide an improved version of your README file with better structure, more details, and clearer instructions. Here's an enhanced version:
markdownCopy# Brazilian Customers Orders ETL Pipeline Application

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline application for the Brazilian E-Commerce Public Dataset by Olist. It uses Polars for data processing and DuckDB as the target database. The entire pipeline, including data ingestion scripts, unit tests, and the DuckDB database, is containerized using Docker for easy deployment and scalability. A Flask application provides API endpoints for data processing and retrieval, and a CI/CD pipeline automates deployment to an EC2 instance.

## Features

- Data extraction from multiple CSV files in the Olist dataset
- Data transformation and modeling to create dimensional and fact tables
- Data analysis to generate insightful aggregate tables
- Data loading into a DuckDB database
- RESTful API endpoints for data uploading, transformation, and retrieval
- Comprehensive unit tests for data preprocessing, database operations, and API endpoints
- Full dockerization of the ETL pipeline for reproducibility and portability
- CI/CD pipeline using GitHub Actions for automated deployment to EC2

## Prerequisites

- Docker and Docker Compose
- Git
- AWS account (for EC2 deployment)

## Quick Start

1. Clone the repository:
git clone https://github.com/Data-Epic/Brazillian_Orders_Peter.git
cd Brazillian_Orders_Peter
Copy
2. Download the dataset:
- Get the dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Unzip the downloaded file
- Create a `data/` directory in the project root
- Place all CSV files in the `data/` directory

3. Build and run the Docker container:
docker-compose up --build
Copy
This will:
- Build the Docker image based on the provided Dockerfile
- Install necessary dependencies
- Start the Flask application

4. In a new terminal, you can start a new container:
docker start -ai api-deployment-container
Copy
5. Use the API:
- Load tables by uploading CSV files to the data loading endpoints
- View loaded and analytical tables using the data retrieval endpoints
- Process and analyze tables by executing the data processing endpoints

## Input Data

The following CSV files are required:

- olist_customers_dataset.csv
- olist_order_items_dataset.csv
- olist_orders_dataset.csv
- olist_products_dataset.csv
- olist_sellers_dataset.csv
- product_category_name_translation.csv
- olist_order_payments_dataset.csv

## Output Tables

The application generates the following analytical and aggregate tables in the DuckDB database:

- fact_table
- top_sellers
- top_selling_product_category
- order_status_count
- top_average_delivery_time
- loyal_customers

## Project Structure
Brazillian_Orders_Peter/
├── data/                  # Input CSV files (create this directory)
├── src/                   # Source code files
│   ├── data_extraction.py
│   ├── data_transformation.py
│   ├── data_loading.py
│   └── main.py
├── tests/                 # Unit tests
├── .gitignore             # Git ignore file
├── Dockerfile             # Docker configuration
├── docker-compose.yml     # Docker Compose configuration
├── entrypoint.sh          # Bash command line scripts
├── requirements.txt       # Application dependencies
└── README.md              # This file

## API Endpoints

Detailed documentation of API endpoints will be available at `/docs` when the application is running.

## Customization

### Updating Dependencies

If you need to update or add dependencies:

1. Modify the `requirements.txt` file
2. Rebuild the Docker image:

docker compose up --build

### Environment Variables

Configure environment-specific settings in a `.env` file:
DATABASE_URL=your_database_url
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key

## Deployment

The application is automatically deployed to an EC2 instance via GitHub Actions when changes are pushed to the main branch.

## Troubleshooting

- Ensure all required CSV files are in the `data/` directory
- Check Docker logs for any error messages: