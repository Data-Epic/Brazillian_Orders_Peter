import sys
import os
import logging
import polars as pl
from src.database import get_db
from src.processing import (load_data,
                        transform__df,
                        process_fact_table,
                        process_dim_table_df,
                        get_top_sellers,
                        get_top_selling_product_category,
                        get_orders_status_count,
                        get_average_delivery_duration,
                        get_loyal_customers
                        )
from flask import Flask, request, jsonify
from flask_swagger_ui import get_swaggerui_blueprint
from src.api import app
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#configuring upload folder
app.config['UPLOAD_FOLDER'] = 'uploads'

#ensuring upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

#configuring swagger UI
SWAGGER_URL = '/api/docs'
API_URL = '/static/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={'app_name': "Brazillian Orders ETL API"}
)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


def upload_csv():
    """
    Uploads a CSV file to the server
    """
    # check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    # if user does not select file, browser also
    # submit an empty part without filename
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file and file.filename.endswith('.csv'):
        filename = file.filename
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        return {"status": "success",
                "message": "File uploaded successfully",
                  "file_path": file_path}
    

def df_to_list_of_dicts(df:pl.DataFrame):
    """
    Converts a polars DataFrame to a list of dictionaries
    """
    new_df_dict = {}
    all_records = []
    df_columns = df.columns
    for row in df.iter_rows():
        num = 0
        for col in df.columns:
            new_df_dict[col] = row[df_columns.index(col)]
            num += 1
            if num % len(df_columns) == 0:
                all_records.append(new_df_dict.copy()) #get the dataframe records into a list of dictionaries,
    
    return all_records

def load_and_transform_data(file_path:str):
    """
    Load and transform the data in the CSV file
    """
    df = load_data(file_path)
    df = df.head(400)
    # df = df.head(15000)
    df = transform__df(df)
    # print("df", df.head())
    list_dicts_df = df_to_list_of_dicts(df)
    
    return {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": list_dicts_df}

def jsonify_loaded_data(new_data:list, data_list:list, model: declarative_base):
    """
    Converts the loaded data to JSON format
    """
    table_columns = [column.key for column in inspect(model).c]
    print("table_columns", table_columns)
    for record in new_data:
        record_dict = {}
        for column in table_columns:
            record_dict[column] = getattr(record, column)
        data_list.append(record_dict)

    return data_list

def query_existing_data(model: declarative_base, list_dicts_df:list, db:get_db):
    """
    Function to Query existing model in the database

    """
      # Convert list of dictionaries to a list of tuples for better performance
    table_ids = tuple(record['id'] for record in list_dicts_df)
    # Use a single query to check for existing data
    existing_data = db.query(model).filter(model.id.in_(table_ids)).all()
    print("existing_data", existing_data)
    logger.info(f"Found {len(existing_data)} existing records in the database")
    # Create a set of existing seller IDs for faster lookup
    existing_data_ids = {data.id for data in existing_data}

    return existing_data, existing_data_ids

def query_table_data(model:declarative_base, db:get_db):
    """
    Function to retrieve just the first 5 records from the table
    """
    records = db.query(model).all()
    records_filtered = records[:5]
    # print("all sellers, ", sellers_filtered)
    records_list = []
    if records_filtered:
        for record in records_filtered:
            record_dict = {}
            for column in record.__table__.columns:
                record_dict[column.name] = getattr(record, column.name)
            records_list.append(record_dict)
    
    return records, records_list

def process_dim_tables(db: get_db, 
    Sellers: list,
    Customers: list,
    Orders: list,
    Order_Items: list,
    Order_Payments: list,
    Products: list,
    Product_Category: list
):
    """
    It Processes the dimension tables from the database e.g. Sellers, Customers
    and convert them to a dataframe in order to get a fact table from modelling them
    
    """

     # Get all the tables
    sellers = db.query(Sellers).all()
    customers = db.query(Customers).all()
    orders = db.query(Orders).all()
    order_items = db.query(Order_Items).all()
    order_payments = db.query(Order_Payments).all()
    products = db.query(Products).all()
    product_categories = db.query(Product_Category).all()
    
    # Check if the tables are not empty
    if not sellers or not customers or not orders \
        or not order_items or not order_payments or not products \
            or not product_categories:
        return jsonify({'error': 'One or more of the dimension tables are empty'}), 404
    
    orders_df = process_dim_table_df(orders)
    order_items_df = process_dim_table_df(order_items)
    customers_df = process_dim_table_df(customers)
    order_payments_df = process_dim_table_df(order_payments)
    products_df = process_dim_table_df(products)
    sellers_df = process_dim_table_df(sellers)
    product_categories_df = process_dim_table_df(product_categories)

    # print("orders", orders)

    list_of_tables = [orders_df, order_items_df, customers_df, 
                            order_payments_df,products_df, 
                        sellers_df, product_categories_df]  
    
    return list_of_tables
    
