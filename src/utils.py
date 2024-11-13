import os
import logging
import polars as pl
from src.database import get_db
from src.processing import load_data, transform_df, process_dim_table_df
from flask import request, jsonify, Flask
from flask_swagger_ui import get_swaggerui_blueprint
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app.config["UPLOAD_FOLDER"] = "uploads"
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

SWAGGER_URL = "/api/docs"
API_URL = "/static/swagger.json"
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL, API_URL, config={"app_name": "Brazillian Orders ETL API"}
)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


def upload_csv() -> dict:
    """
    Function to upload a CSV file to the server

    Args:
        None

    Returns:
        dict: A dictionary containing the status of the file upload
    """
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file part"}), 400
        file = request.files["file"]
        if file.filename == "":
            return jsonify({"error": "No selected file"}), 400
        if file and file.filename.endswith(".csv"):
            filename = file.filename
            file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            file.save(file_path)

            return {
                "status": "success",
                "message": "File uploaded successfully",
                "file_path": file_path,
            }

    except Exception as e:
        logger.error(f"Error occurred during file upload: {str(e)}")
        return {
            "status": "error",
            "message": "An error occurred during file upload",
            "error": str(e),
        }


def df_to_list_of_dicts(df: pl.DataFrame) -> list:
    """
    Converts a polars DataFrame to a list of dictionaries

    Args:
        df (pl.DataFrame): A polars DataFrame

    Returns:
        list: A list of dictionaries
    """
    try:
        new_df_dict = {}
        all_records = []
        df_columns = df.columns
        for row in df.iter_rows():
            num = 0
            for col in df.columns:
                new_df_dict[col] = row[df_columns.index(col)]
                num += 1
                if num % len(df_columns) == 0:
                    all_records.append(new_df_dict.copy())

        return all_records

    except Exception as e:
        logger.error(
            f"Error occurred during conversion of DataFrame to list of dictionaries: {str(e)}"
        )
        return {
            "status": "error",
            "message": "An error occurred during conversion of DataFrame to list of dictionaries",
            "error": str(e),
        }


def load_and_transform_data(file_path: str) -> dict:
    """
    Load and transform the data in the CSV file

    Args:
        file_path (str): The path to the CSV file

    Returns:
        dict: A dictionary containing the status of the data load and transformation
    """
    try:
        df = load_data(file_path)
        df = transform_df(df)
        list_dicts_df = df_to_list_of_dicts(df)

        return {
            "status": "success",
            "message": "Data loaded and transformed successfully",
            "data": list_dicts_df,
        }

    except Exception as e:
        logger.error(f"Error occurred during data load and transformation: {str(e)}")
        return {
            "status": "error",
            "message": "An error occurred during data load and transformation",
            "error": str(e),
        }


def jsonify_loaded_data(
    new_data: list, data_list: list, model: declarative_base
) -> list:
    """
    Converts the loaded data to JSON format

    Args:
        new_data (list): The new data to be converted to JSON
        data_list (list): The list to store the JSON data
        model (declarative_base): The SQLAlchemy model

    Returns:
        list: A list of JSON data
    """
    try:
        table_columns = [column.key for column in inspect(model).c]
        for record in new_data:
            record_dict = {}
            for column in table_columns:
                record_dict[column] = getattr(record, column)
            data_list.append(record_dict)

        return data_list

    except Exception as e:
        logger.error(
            f"Error occurred during conversion of loaded data to JSON: {str(e)}"
        )
        return {
            "status": "error",
            "message": "An error occurred during conversion of loaded data to JSON",
            "error": str(e),
        }


def query_existing_data(
    model: declarative_base, list_dicts_df: list, db: get_db
) -> tuple:
    """
    Function to Query existing model in the database

    """
    try:
        table_ids = tuple(record["id"] for record in list_dicts_df)
        # Use a single query to check for existing data
        existing_data = db.query(model).filter(model.id.in_(table_ids)).all()
        existing_data_ids = {data.id for data in existing_data}
        return existing_data, existing_data_ids

    except Exception as e:
        logger.error(f"Error occurred during querying of existing data: {str(e)}")
        return {
            "status": "error",
            "message": "An error occurred during querying of existing data",
            "error": str(e),
        }


def query_table_data(model: declarative_base, db: get_db) -> tuple:
    """
    Function to retrieve just the first 5 records from the table

    Args:
        model (declarative_base): The SQLAlchemy model
        db (get_db): The database session

    Returns:
        tuple: A tuple containing the records and the records list

    """
    try:
        records = db.query(model).all()
        records_filtered = records[:5]
        records_list = []
        if records_filtered:
            for record in records_filtered:
                record_dict = {}
                for column in record.__table__.columns:
                    record_dict[column.name] = getattr(record, column.name)
                records_list.append(record_dict)

        return records, records_list

    except Exception as e:
        logger.error(f"Error occurred during querying of table data: {str(e)}")
        return {
            "status": "error",
            "message": "An error occurred during querying of table data",
            "error": str(e),
        }


def process_dim_tables(
    db: get_db,
    Sellers: list,
    Customers: list,
    Orders: list,
    Order_Items: list,
    Order_Payments: list,
    Products: list,
    Product_Category: list,
) -> list:
    """
    It Processes the dimension tables from the database e.g. Sellers, Customers
    and convert them to a dataframe in order to get a fact table from modelling them

    Args:
        db (get_db): The database session
        Sellers (list): The Sellers table
        Customers (list): The Customers table
        Orders (list): The Orders table
        Order_Items (list): The Order_Items table
        Order_Payments (list): The Order_Payments table
        Products (list): The Products table
        Product_Category (list): The Product_Category table

    Returns:
        list: A list of the processed dimension tables
    """
    try:
        sellers = db.query(Sellers).all()
        customers = db.query(Customers).all()
        orders = db.query(Orders).all()
        order_items = db.query(Order_Items).all()
        order_payments = db.query(Order_Payments).all()
        products = db.query(Products).all()
        product_categories = db.query(Product_Category).all()

        if (
            not sellers
            or not customers
            or not orders
            or not order_items
            or not order_payments
            or not products
            or not product_categories
        ):
            return jsonify(
                {"error": "One or more of the dimension tables are empty"}
            ), 404

        orders_df = process_dim_table_df(orders)
        order_items_df = process_dim_table_df(order_items)
        customers_df = process_dim_table_df(customers)
        order_payments_df = process_dim_table_df(order_payments)
        products_df = process_dim_table_df(products)
        sellers_df = process_dim_table_df(sellers)
        product_categories_df = process_dim_table_df(product_categories)

        list_of_tables = [
            orders_df,
            order_items_df,
            customers_df,
            order_payments_df,
            products_df,
            sellers_df,
            product_categories_df,
        ]

        return list_of_tables

    except Exception as e:
        logger.error(f"Error occurred during processing of dimension tables: {str(e)}")
        return {
            "status": "error",
            "message": "An error occurred during processing of dimension tables",
            "error": str(e),
        }
