# Brazilian Customers Orders ETL Pipeline Application

This project implements an ETL (Extract, Transform, Load) pipeline Application for the Brazilian E-Commerce Public Dataset by Olist. It uses Polars for data processing and DuckDB as the target database. The entire pipeline, including data ingestion scripts, unit tests, and the DuckDB database, is containerized using Docker for easy deployment and scalability.The Docker environment is designed to extract the  data and provide a command-line interface (CLI) for querying the data using DuckDB.
A Flask Application is created that encapsulates all processing and retrieval functionalities and a CI-CD pipeline is setup which autodeploys the flask API to an EC2 instance.

## Features

- Data uploading from multiple CSV files in the Olist dataset
- Data transformation and modeling to create dimensional and fact tables
- Aata analysis to generate insightful aggregate tables
- Data loading into a DuckDB database
- API endpoints that encapsulates all data uploading, transformation and retrieval
- Comprehensive unit tests for data preprocessing database operations, and API endpoints
- Full dockerization of the ETL pipeline for reproducibility and portability
- CI/CD Pipeline using github actions that auto deploy the API endpoints to EC2 Instance

## Prerequisites

- Docker and Docker Compose
- Git

## Quick Start

1. Clone the repository:
git clone https://github.com/Data-Epic/Brazillian_Orders_Peter.git

2. Download the dataset:
- Get the list dataset and information from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), unzip the downloaded zip file
- Create a `data/` directory Place all CSV files in the `data/` directory

3. Build and run the Docker container:
docker-compose up --build

This will:
Build the Docker image based on the provided Dockerfile.
Install necessary dependencies.
Start the flask application.

4. Also, In a new terminal, you can start a new container:
docker start -ai api-deployment-container

5. Load all the tables by uploading the required csv file in the data loading endpoints
6. View all the loaded and analytical tables that has been loaded into the database in the data retrieval endpoints
7. Process and analyze the tables by executing the data processing endpoints

Input Data

olist_customers_dataset.csv
olist_order_items_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
olist_order_payments_dataset.csv

Output Tables
The application generates the following analytical and aggregate tables in a DuckDB database:

fact_table
top_sellers
top_selling_product_category
order_status_count
top_average_delivery_time
loyal_customers

Project Structure
customer_orders_analysis/
├── data/                  # Input CSV files you have to create this when you clone to local
├── src/                   # Source code files
|── tests/                 # Unit tests
|-- gitignore              #gitignore file
├── Dockerfile             # Docker configuration
├── docker-compose.yml     # Docker Compose configuration
|-- entrypoint.sh          # bash command line scrupts
├── requirements.txt       # Application dependencies
└── README.md              # This file

Customization
Updating Dependencies:

If you need to update or add dependencies and packages, modify the requirements.txt file and rebuild the Docker image using:

docker compose up --build

Dependency Issues:
Ensure all required dependencies are listed in the requirements.txt file.