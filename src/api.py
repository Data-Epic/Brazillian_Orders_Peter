from flask import Flask, request, jsonify
import polars
import pandas as pd
import numpy as np
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from werkzeug.utils import secure_filename
from flask_swagger_ui import get_swaggerui_blueprint
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
from sqlalchemy.orm import Session
from src.database import (get_db, Sellers,
                       Customers,
                       Orders,
                       Order_Items,
                       Products,
                       Product_Category,
                       FactTable,
                       Order_Payments,
                       Top_Sellers,
                       Top_Selling_Product_Category,
                       Order_Status_Count,
                       Top_Average_Delivery_Duration,
                          Loyal_Customers
                       )
app = Flask(__name__)
from src.utils import (upload_csv,
                df_to_list_of_dicts,
                load_and_transform_data,
                jsonify_loaded_data,
                query_existing_data,
                process_dim_tables,
                query_table_data
                )


import polars as pl
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# list_dicts_df = df_to_list_of_dicts(df)
# json_df = jsonify(list_dicts_df)
# print(json_df)

@app.route('/api/load_sellers_data', methods=['POST'])
def api_load_sellers_data(
):
    try:
        file_upload = upload_csv()
        if file_upload['status'] == 'success':
            file_path = file_upload['file_path']

            try:
                loaded_data = load_and_transform_data(file_path)
                if loaded_data['status'] == 'success':
                    list_dicts_df = loaded_data['data']
                    #if list_dicts_df do not have specific columns, return error
                    sellers_df_columns = ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']
                    if not all(col in list_dicts_df[0] for col in sellers_df_columns):
                        return jsonify({'error': 'Invalid file, Kindly provide the Sellers csv file'
                                        }), 400

                with get_db() as db:
                    try:
                        existing_sellers, existing_seller_ids = query_existing_data(Sellers, list_dicts_df, db)
                        new_sellers = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_seller_ids:
                                seller = Sellers(
                                    id = record['id'],
                                    seller_id=record['seller_id'],
                                    seller_zip_code_prefix=record['seller_zip_code_prefix'],
                                    seller_city=record['seller_city'],
                                    seller_state=record['seller_state']
                                )
                                new_sellers.append(seller)
                                db.add(seller)
                                db.commit()
                                db.refresh(seller)

                        sellers_list = []
                        sellers_list = jsonify_loaded_data(new_sellers, sellers_list, Sellers)
                       
                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': sellers_list[:5],
                            'new_sellers_count': len(new_sellers),
                            'existing_sellers_count': len(existing_sellers)
                        }), 200
                    
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
                    
            except Exception as e:
                logger.error(f"Error loading and transforming data to a list of dataframes: {str(e)}")
                return jsonify({'status':'error',
                                'error': str(e)}), 400

    except Exception as e:
        logger.error(f"An error occurred while uploading the data: {str(e)}")
        return jsonify({'status':'error',
                        "message": "An error occured while uploading, Kindly upload the valid Sellers csv file",
                        'error': str(e)}), 500
    

@app.route('/api/get_sellers', methods=['GET'])
def api_get_sellers():
    with get_db() as db:
        try:
            sellers, sellers_list = query_table_data(Sellers, db)
            if sellers_list:

                return jsonify({"status": "success",
                                "message": "Sellers data retrieved successfully",
                                "body":  sellers_list,
                                "count_records": len(sellers) }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Sellers data available",
                                "body":  sellers_list
                            }), 200
        
        except Exception as e:
            logger.error(f"An error occurred retrieving all sellers data: {str(e)}")
            
            return jsonify({'status':  'error',
                            'error': f'retrieving sellers data: {str(e)}'}), 500


@app.route('/api/load_customers_data', methods=['POST'])
def api_load_customers_data(
):
    try:
        file_upload = upload_csv()
        if file_upload['status'] == 'success':
            file_path = file_upload['file_path']
            
            try:
                loaded_data = load_and_transform_data(file_path)
                if loaded_data['status'] == 'success':
                    list_dicts_df = loaded_data['data']
                    #if list_dicts_df do not have specific columns, return error
                    customers_df_columns = ['customer_id', 'customer_unique_id', 
                                      'customer_zip_code_prefix', 'customer_city',
                                      'customer_state']
                    if not all(col in list_dicts_df[0] for col in customers_df_columns):
                        return jsonify({'error': 'Invalid file, Kindly provide the Customers csv file'
                                        }), 400

                with get_db() as db:
                    try:
                        existing_customers, existing_customer_ids = query_existing_data(Customers, list_dicts_df, db)
                        # Insert only new customers
                        new_customers = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_customer_ids:
                                customer = Customers(
                                    id = record['id'],
                                    customer_id=record['customer_id'],
                                    customer_unique_id=record['customer_unique_id'],
                                    customer_zip_code_prefix=record['customer_zip_code_prefix'],
                                    customer_city=record['customer_city'],
                                    customer_state = record['customer_state']
                                )
                                new_customers.append(customer)
                                db.add(customer)
                                db.commit()
                                db.refresh(customer)
                        
                        customers_list = []
                        customers_list = jsonify_loaded_data(new_customers, customers_list, Customers)
                        
                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': customers_list[:5],
                            'new_customers_count': len(new_customers),
                            'existing_customers_count': len(existing_customers)
                        }), 200
                    
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
                    
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid customers dataset: {str(e)}")
                return jsonify({'status':'error','error': str(e)}), 400
                    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'status': 'error',
                         "message": "An error occured while uploading, Kindly upload the valid Customers csv file",
            'error': str(e)}), 500


@app.route('/api/get_customers', methods=['GET'])
def api_get_customers():
    with get_db() as db:
        try:
            customers, customers_list = query_table_data(Customers, db)
            
            if customers_list:
                return jsonify({"status": "success",
                                "message": "Customers data retrieved successfully",
                                "body":  customers_list,
                                "count": len(customers)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Customers data available",
                                "body":  customers_list
                            }), 200
        
        except Exception as e:
            logger.error(f"An error occurred retrieving all customers: {str(e)}")
            return jsonify({'status': 'error',
                            'error':f' retrieving Customers data: {str(e)}'}), 500



@app.route('/api/load_orders_data', methods=['POST'])
def api_load_orders_data(
):
    try:
        file_upload = upload_csv()
        if file_upload['status'] == 'success':
            file_path = file_upload['file_path']
            
            try:
                loaded_data = load_and_transform_data(file_path)
                if loaded_data['status'] == 'success':
                    list_dicts_df = loaded_data['data']
                    #if list_dicts_df do not have specific columns, return error
                    orders_df_columns = ['order_id', 'order_status', 
                                      'order_purchase_timestamp',
                                      'order_approved_at', 'order_delivered_carrier_date',
                                      'order_delivered_customer_date', 'order_estimated_delivery_date']
                    
                    if not all(col in list_dicts_df[0] for col in orders_df_columns):
                        return jsonify({'error': 'Invalid file, Kindly provide the Orders csv file'
                                        }), 400

                with get_db() as db:
                    try:
                        existing_orders, existing_order_ids = query_existing_data(Orders, list_dicts_df, db)
                        # Insert only new sellers
                        new_orders = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_order_ids:
                                order = Orders(
                                    id = record['id'],
                                    order_id=record['order_id'],
                                    customer_id=record['customer_id'],
                                    order_status=record['order_status'],
                                    order_purchase_timestamp=record['order_purchase_timestamp'],
                                    order_approved_at = record['order_approved_at'],
                                    order_delivered_carrier_date = record['order_delivered_carrier_date'],
                                    order_delivered_customer_date = record['order_delivered_customer_date'],
                                    order_estimated_delivery_date = record['order_estimated_delivery_date']
                                )
                                new_orders.append(order)
                                db.add(order)
                                db.commit()
                                db.refresh(order)

                        order_list = []
                        order_list = jsonify_loaded_data(new_orders, order_list, Orders)

                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': order_list[:5],
                            'new_orders_count': len(new_orders),
                            'existing_orders_count': len(existing_orders),
                            
                        }), 200
                    
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'status': 'error',
                                        'error': f'Database operation failed details {str(db_error)}'}), 500
                    
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid orders dataset: {str(e)}")
                return jsonify({'status': 'error', 'error': str(e)}), 400
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'status':'error',
                         "message": "An error occured while uploading, Kindly upload the valid Orders csv file",
            'error': str(e)}), 500


@app.route('/api/get_orders', methods=['GET'])
def api_get_orders():
    with get_db() as db:
        try:
            orders, orders_list = query_table_data(Orders, db)

            if orders_list:
                return jsonify({"status": "success",
                                "message": "Orders data retrieved successfully",
                                "body":  orders_list,
                                "count_records": len(orders)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Orders data available",
                                "body":  orders_list
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all orders: {str(e)}")
            return jsonify({f'status': 'error', 'error': f'error retrieving Orders data: {str(e)}'}), 500
        

@app.route('/api/load_order_items_data', methods=['POST'])
def api_load_order_items_data(
):
    try:
        file_upload = upload_csv()
        if file_upload['status'] == 'success':
            file_path = file_upload['file_path']
            
            try:
                loaded = load_and_transform_data(file_path)
                if loaded['status'] == 'success':
                    list_dicts_df = loaded['data']
                    #if list_dicts_df do not have specific columns, return error
                    order_items_df_columns = ['order_id', 'order_item_id', 'product_id', 
                                      'seller_id', 'shipping_limit_date', 'price', 'freight_value']
                    
                    if not all(col in list_dicts_df[0] for col in order_items_df_columns):
                        return jsonify({'error': 'Invalid file, Kindly provide the Order Items csv file'
                                        }), 400

                with get_db() as db:
                    try:
                        existing_order_items, existing_order_ids = query_existing_data(Order_Items, list_dicts_df, db)
                        # Insert only new order items
                        new_order_items = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_order_ids:
                                order_item = Order_Items(
                                    id = record['id'],
                                    order_id=record['order_id'],
                                    order_item_id=record['order_item_id'],
                                    product_id=record['product_id'],
                                    seller_id = record['seller_id'],
                                    shipping_limit_date = record['shipping_limit_date'],
                                    price = record['price'],
                                    freight_value = record['freight_value']
                                )
                                new_order_items.append(order_item)
                                db.add(order_item)
                                db.commit()
                                db.refresh(order_item)

                        order_items_list = []
                        order_items_list = jsonify_loaded_data(new_order_items, order_items_list, Order_Items)

                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': order_items_list[:5],
                            'new_order_items_count': len(new_order_items),
                            'existing_order_items_count': len(existing_order_items)
                        }), 200
                    
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'status':'error', 'error': 'Database operation failed', 'details': str(db_error)}), 500
            
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid order items dataset: {str(e)}")
                return jsonify({'status':'error', 
                
                            'error': str(e)}), 400
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'status': 'error', 
            "message": "An error occured while uploading, Kindly upload the valid OrderItems csv file",
            'error': str(e)}), 500


@app.route('/api/get_order_items', methods=['GET'])
def api_get_order_items():
    with get_db() as db:
        try:
            order_items, order_items_list = query_table_data(Order_Items, db)
            
            if order_items_list:
                return jsonify({"status": "success",
                                "message": "Order Items data retrieved successfully",
                                "body":  order_items_list,
                                "count_records": len(order_items)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Order Items data available",
                                "body":  order_items_list
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all order items: {str(e)}")
        return jsonify({'status':'error', 
                        'error':f'error retrieving Order Items data: {str(e)}'}), 500


@app.route('/api/load_order_payments_data', methods=['POST'])
def api_load_order_payments_data(
):
    try:
        file_upload = upload_csv()
        if file_upload['status'] == 'success':
            file_path = file_upload['file_path']
            
            try:
                loaded_data = load_and_transform_data(file_path)
                if loaded_data['status'] == 'success':
                    list_dicts_df = loaded_data['data']
                    #if list_dicts_df do not have specific columns, return error
                    order_payments_df_columns = ['order_id', 'payment_sequential', 'payment_type', 
                                      'payment_installments', 'payment_value']
                    
                    if not all(col in list_dicts_df[0] for col in order_payments_df_columns):
                        return jsonify({'error': 'Invalid file, Kindly provide the Order Payments csv file'
                                        }), 400
                
                with get_db() as db:
                    try:
                        existing_order_payments, existing_order_ids = query_existing_data(Order_Payments, list_dicts_df, db)
                        new_order_payments = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_order_ids:
                                order_payment = Order_Payments(
                                    id = record['id'],
                                    order_id=record['order_id'],
                                    payment_sequential=record['payment_sequential'],
                                    payment_type=record['payment_type'],
                                    payment_installments = record['payment_installments'],
                                    payment_value = record['payment_value']
                                )
                                new_order_payments.append(order_payment)
                                db.add(order_payment)
                                db.commit()
                                db.refresh(order_payment)

                        order_payments_list = []
                        order_payments_list = jsonify_loaded_data(new_order_payments, order_payments_list, Order_Payments)

                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': order_payments_list[:5],
                            'new_order_payments_count': len(new_order_payments),
                            'existing_order_payments_count': len(existing_order_payments)
                        }), 200
                    
                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'status':'error', 'error': f'Database operation failed details: {str(db_error)}'}), 500
                    
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid order payments dataset: {str(e)}")
                return jsonify({'status':'error', 'error': str(e)}), 400
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'status':'error', 
                         "message": "An error occured while uploading, Kindly upload the valid Order Payments csv file",
                        'error': str(e)}), 500


@app.route('/api/get_order_payments', methods=['GET'])
def api_get_order_payments():
    with get_db() as db:
        try:
            order_payments, order_payments_list = query_table_data(Order_Payments, db)
            
            if order_payments_list:
                return jsonify({"status": "success",
                                "message": "Order Payments data retrieved successfully",
                                "body":  order_payments_list,
                                "count_records": len(order_payments)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Order Payments data available",
                                "body":  order_payments_list
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all order payments: {str(e)}")
            return jsonify({'status':'error', 'error':f'error retrieving Order Payments data {str(e)}'}), 500
    

@app.route('/api/load_products_data', methods=['POST'])
def api_load_products_data(
):
    try:
        file_upload = upload_csv()
        if file_upload['status'] == 'success':
            file_path = file_upload['file_path']
            
            try:
                loaded_data = load_and_transform_data(file_path)
                if loaded_data['status'] == 'success':
                    list_dicts_df = loaded_data['data']
                    #if list_dicts_df do not have specific columns, return error
                    products_df_columns = ['product_id', 'product_category_name', 'product_name_lenght', 
                                      'product_description_lenght', 'product_photos_qty', 'product_weight_g',
                                      'product_length_cm', 'product_height_cm', 'product_width_cm']
                    
                    if not all(col in list_dicts_df[0] for col in products_df_columns):
                        return jsonify({'error': 'Invalid file, Kindly provide the Products csv file'
                                        }), 400

                with get_db() as db:
                    try:
                        existing_products, existing_product_ids = query_existing_data(Products, list_dicts_df, db)
                        # Insert only new products
                        new_products = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_product_ids:
                                product = Products(
                                    id = record['id'],
                                    product_id=record['product_id'],
                                    product_category_name=record['product_category_name'],
                                    product_name_lenght=record['product_name_lenght'],
                                    product_description_lenght = record['product_description_lenght'],
                                    product_photos_qty = record['product_photos_qty'],
                                    product_weight_g = record['product_weight_g'],
                                    product_length_cm = record['product_length_cm'],
                                    product_height_cm = record['product_height_cm'],
                                    product_width_cm = record['product_width_cm']
                                )

                                new_products.append(product)
                                db.add(product)
                                db.commit()
                                db.refresh(product)

                        products_list = []
                        products_list = jsonify_loaded_data(new_products, products_list, Products)
         
                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': products_list[:5],
                            'new_products_count': len(new_products),
                            'existing_products_count': len(existing_products)
                        }), 200

                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid products dataset: {str(e)}")
                return jsonify({'error': str(e)}), 500
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'status':'error', 
            "message": "An error occured while uploading, Kindly upload the valid Products csv file",
            'error': str(e)}), 500


@app.route('/api/get_products', methods=['GET'])
def api_get_products():
    with get_db() as db:
        try:
            products, products_list = query_table_data(Products, db)
            if products_list:
                return jsonify({"status": "success",
                                "message": "Products data retrieved successfully",
                                "body":  products_list,
                                "count_records": len(products)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Products data available",
                                "body":  products_list,
                                "count_records": len(products)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all products: {str(e)}")
            return jsonify({'status':'error',
                            'error':f'error retrieving Products data: {str(e)}'}), 500


@app.route('/api/load_products_category', methods=['POST'])
def api_load_products_category():
    try:
        file_upload = upload_csv()
        if file_upload['status'] == 'success':
            file_path = file_upload['file_path']
            
            try:
                loaded_data = load_and_transform_data(file_path)
                if loaded_data['status'] == 'success':
                    list_dicts_df = loaded_data['data']
                    #if list_dicts_df do not have specific columns, return error
                    product_category_df_columns = ['product_category_name', 'product_category_name_english']
                    if not all(col in list_dicts_df[0] for col in product_category_df_columns):
                        return jsonify({'error': 'Invalid file, Kindly provide the Product Category csv file'
                                        }), 400
                    
                with get_db() as db:
                    try:
                        existing_product_categories, existing_product_category_ids = query_existing_data(Product_Category, list_dicts_df, db)
                        new_product_categories = []
                        for record in list_dicts_df:
                            if record['id'] not in existing_product_category_ids:
                                product_category = Product_Category(
                                    id = record['id'],
                                    product_category_name=record['product_category_name'],
                                    product_category_name_english=record['product_category_name_english']
                                )
                                new_product_categories.append(product_category)
                                db.add(product_category)
                                db.commit()
                                db.refresh(product_category)

                        product_category_list = []
                        product_category_list = jsonify_loaded_data(new_product_categories, product_category_list, Product_Category)

                        return jsonify({
                            'status': 'success',
                            'message': 'Data processed successfully',
                            'body': product_category_list[:5],
                            'new_product_categories_count': len(new_product_categories),
                            'existing_product_categories_count': len(existing_product_categories)
                        }), 200

                    except Exception as db_error:
                        db.rollback()
                        logger.error(f"Database operation failed: {str(db_error)}")
                        return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500   
            except Exception as e:
                logger.error(f"Error loading the dataset, kindly provide the valid product category dataset: {str(e)}")
                return jsonify({'error': str(e)}), 400
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'status':'error',
                         "message": "An error occured while uploading, Kindly upload the valid Products Category csv file",
                         'error': str(e)}), 500


@app.route('/api/get_products_category', methods=['GET'])
def api_get_products_category():
    with get_db() as db:
        try:
            product_categories, product_categories_list = query_table_data(Product_Category, db)
            if product_categories_list:
                return jsonify({"status": "success",
                                "message": "Product Categories data retrieved successfully",
                                "body":  product_categories_list,
                                "count_records": len(product_categories)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Product Categories data available",
                                "body":  product_categories_list,
                                "count_records": len(product_categories)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all product categories: {str(e)}")
            return jsonify({'status':'error',
                            'error':f'error retrieving Product Categories data: {str(e)}'}), 500


@app.route('/api/process_fact_table', methods=['POST'])
def api_process_fact_table():
    try:
        with get_db() as db:
            try:
                list_of_tables = process_dim_tables(db,Sellers,Customers,
                                                    Orders,Order_Items,
                                                    Order_Payments,
                                                    Products,Product_Category)

                for df in list_of_tables:
                    if not isinstance(df, pl.DataFrame):
                        return jsonify({'status': 'error',
                                        'body': {df.head(1).to_dicts()} ,
                                        'message': f'transformed_data didnt yield a valid polars dataframe'}), 400

                fact_table = process_fact_table(list_of_tables)
                #remove duplicates 
                fact_table = fact_table.filter(fact_table.is_duplicated() == False)

                if not isinstance(fact_table, pl.DataFrame):
                    return jsonify({'status': 'error',
                                    'message': 'fact_table didnt yield a valid polars dataframe'}), 400

                list_dicts_df = df_to_list_of_dicts(fact_table)
                existing_fact_table, existing_fact_table_ids = query_existing_data(FactTable, list_dicts_df, db)
                # Insert only new fact_table
                new_fact_table = []
                for record in list_dicts_df:
                    if record['id'] not in existing_fact_table_ids:
                        fact = FactTable(
                            id = record['id'],
                            order_id=record['order_id'],
                            customer_id=record['customer_id'],
                            order_status=record['order_status'],
                            order_purchase_timestamp=record['order_purchase_timestamp'],
                            order_approved_at = record['order_approved_at'],
                            order_delivered_carrier_date = record['order_delivered_carrier_date'],
                            order_delivered_customer_date = record['order_delivered_customer_date'],
                            order_estimated_delivery_date = record['order_estimated_delivery_date'],
                            customer_unique_id = record['customer_unique_id'],
                            customer_zip_code_prefix = record['customer_zip_code_prefix'],
                            customer_city = record['customer_city'],
                            customer_state = record['customer_state'],
                            order_item_id = record['order_item_id'],
                            product_id = record['product_id'],
                            seller_id = record['seller_id'],
                            shipping_limit_date = record['shipping_limit_date'],
                            price = record['price'],
                            freight_value = record['freight_value'],
                            product_category_name = record['product_category_name'],
                            seller_zip_code_prefix = record['seller_zip_code_prefix'],
                            seller_city = record['seller_city'],
                            seller_state = record['seller_state'],
                            product_category_name_english = record['product_category_name_english']
                        )
                        new_fact_table.append(fact)
                        db.add(fact)
                        db.commit()
                        db.refresh(fact)
                    
                fact_table_list = []
                fact_table_list = jsonify_loaded_data(new_fact_table, fact_table_list, FactTable)

                return jsonify({
                    'status': 'success',
                    'message': 'Data processed successfully',
                    'body': fact_table_list[:5],
                    'new_fact_table_count': len(new_fact_table),
                    'existing_fact_table_count': len(existing_fact_table)
                }), 200
            
            except Exception as db_error:
                db.rollback()
                logger.error(f"Database operation failed: {str(db_error)}")
                return jsonify({'error': 'Database operation failed',
                                "message": "Kindly Confirm all the dimension tables are loaded",                                
                                 'details': str(db_error)}), 500
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/get_fact_table', methods=['GET'])
def api_get_fact_table():
    with get_db() as db:
        try:
            fact_table, fact_table_list = query_table_data(FactTable, db)
            if fact_table_list:
                return jsonify({"status": "success",
                                "message": "Fact Table data retrieved successfully",
                                "body":  fact_table_list,
                                "count_records": len(fact_table)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Fact Table data available",
                                "body":  fact_table_list,
                                "count_records": len(fact_table)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all fact table: {str(e)}")
            return jsonify({'error retrieving Fact Table data': str(e)}), 500

@app.route('/api/load_top_sellers', methods=['POST'])
def api_load_top_sellers():
    try:
        with get_db() as db:
            try:
                # Get all the tables
                fact_table = db.query(FactTable).all()
                if not fact_table:
                    return jsonify({'error': 'Fact table to be analyzed not found in the database'}), 404                
                fact_table = process_dim_table_df(fact_table)
                top_sellers_df = get_top_sellers(fact_table) 

                if not isinstance(top_sellers_df, pl.DataFrame):
                    return jsonify({'status': 'error',
                                    'message': 'transformed_data didnt yield a valid polars dataframe'}), 400
                
                list_dicts_df = df_to_list_of_dicts(top_sellers_df)
                existing_top_sellers, existing_top_sellers_ids = query_existing_data(Top_Sellers, list_dicts_df, db)  
                # Insert only new top_sellers
                new_top_sellers = []
                for record in list_dicts_df:
                    if record['id'] not in existing_top_sellers_ids:
                        top_seller = Top_Sellers(
                            id = record['id'],
                            seller_id=record['seller_id'],
                            total_sales = record['Total_sales'],
                        )
                        new_top_sellers.append(top_seller)
                        db.add(top_seller)
                        db.commit()
                        db.refresh(top_seller)
                
                top_sellers_list = []
                top_sellers_list = jsonify_loaded_data(new_top_sellers, top_sellers_list, Top_Sellers)  
                
                return jsonify({
                    'status': 'success',
                    'message': 'Data processed successfully',
                    'body': top_sellers_list[:5],
                    'new_top_sellers_count': len(new_top_sellers),
                    'existing_top_sellers_count': len(existing_top_sellers)
                }), 200
            
            except Exception as db_error:
                db.rollback()
                logger.error(f"Database operation failed: {str(db_error)}")
                return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_top_sellers', methods=['GET'])
def api_get_top_sellers():
    with get_db() as db:
        try:
            top_sellers, top_sellers_list = query_table_data(Top_Sellers, db)
            if top_sellers_list:
                return jsonify({"status": "success",
                                "message": "Top Sellers data retrieved successfully",
                                "body":  top_sellers_list,
                                "count_records": len(top_sellers)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Top Sellers data available",
                                "body":  top_sellers_list,
                                "count_records": len(top_sellers)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all top sellers: {str(e)}")
            return jsonify({'error retrieving Top Sellers data': str(e)}), 500


@app.route('/api/load_top_selling_product_category', methods=['POST'])
def api_load_top_selling_product_category():
    try:
        with get_db() as db:
            try:
                # Get all the tables
                fact_table = db.query(FactTable).all()
                if not fact_table:
                    return jsonify({'error': 'Fact table to be analyzed not found in the database'}), 404
                
                fact_table = process_dim_table_df(fact_table)
                top_selling_product_category_df = get_top_selling_product_category(fact_table) 

                if not isinstance(top_selling_product_category_df, pl.DataFrame):
                    return jsonify({'status': 'error',
                                    'message': 'transformed_data didnt yield a valid polars dataframe'}), 400
                
                list_dicts_df = df_to_list_of_dicts(top_selling_product_category_df)

                existing_top_selling_product_category, existing_top_selling_product_category_ids = query_existing_data(Top_Selling_Product_Category, list_dicts_df, db)
                new_top_selling_product_category = []
                for record in list_dicts_df:
                    if record['id'] not in existing_top_selling_product_category_ids:
                        top_selling_product_category = Top_Selling_Product_Category(
                            id = record['id'],
                            product_category_name_english=record['product_category_name_english'],
                            total_sales = record['Total_sales'],
                        )
                        new_top_selling_product_category.append(top_selling_product_category)
                        db.add(top_selling_product_category)
                        db.commit()
                        db.refresh(top_selling_product_category)

                top_selling_product_category_list = []
                top_selling_product_category_list = jsonify_loaded_data(new_top_selling_product_category, top_selling_product_category_list, Top_Selling_Product_Category)
                return jsonify({
                    'status': 'success',
                    'message': 'Data processed successfully',
                    'body': top_selling_product_category_list[:5],
                    'new_top_selling_product_category_count': len(new_top_selling_product_category),
                    'existing_top_selling_product_category_count': len(existing_top_selling_product_category)
                }), 200

            except Exception as db_error:
                db.rollback()
                logger.error(f"Database operation failed: {str(db_error)}")
                return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_top_selling_product_category', methods=['GET'])
def api_get_top_selling_product_category():
    with get_db() as db:
        try:
            top_selling_product_category, top_selling_product_category_list = query_table_data(Top_Selling_Product_Category, db) 
            if top_selling_product_category_list:

                return jsonify({"status": "success",
                                "message": "Top Selling Product Category data retrieved successfully",
                                "body":  top_selling_product_category_list,
                                "count_records": len(top_selling_product_category_list)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Top Selling Product Category data available",
                                "body":  top_selling_product_category_list,
                                "count_records": len(top_selling_product_category)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all top selling product category: {str(e)}")
            return jsonify({'error retrieving Top Selling Product Category data': str(e)}), 500


@app.route('/api/load_orders_status_analysis', methods=['POST'])
def api_load_orders_status_analysis():
    try:
        with get_db() as db:
            try:
                # Get all the tables
                fact_table = db.query(FactTable).all()

                if not fact_table:
                    return jsonify({'error': 'Fact table to be analyzed not found in the database'}), 404
                
                fact_table = process_dim_table_df(fact_table)
                orders_status_analysis_df = get_orders_status_count(fact_table) 

                if not isinstance(orders_status_analysis_df, pl.DataFrame):
                    return jsonify({'status': 'error',
                                    'message': 'transformed_data didnt yield a valid polars dataframe'}), 400
                
                list_dicts_df = df_to_list_of_dicts(orders_status_analysis_df)
                existing_orders_status_analysis, existing_orders_status_analysis_ids = query_existing_data(Order_Status_Count, list_dicts_df, db)
                new_orders_status_analysis = []
                for record in list_dicts_df:
                    if record['id'] not in existing_orders_status_analysis_ids:
                        orders_status_analysis = Order_Status_Count(
                            id = record['id'],
                            order_status=record['order_status'],
                            count_records = record['Count'],
                        )
                        new_orders_status_analysis.append(orders_status_analysis)
                        db.add(orders_status_analysis)
                        db.commit()
                        db.refresh(orders_status_analysis)

                orders_status_analysis_list = []
                orders_status_analysis_list = jsonify_loaded_data(new_orders_status_analysis, orders_status_analysis_list, Order_Status_Count)

                return jsonify({
                    'status': 'success',
                    'message': 'Data processed successfully',
                    'body': orders_status_analysis_list[:5],
                    'new_orders_status_analysis_count': len(new_orders_status_analysis),
                    'existing_orders_status_analysis_count': len(existing_orders_status_analysis)
                }), 200

            except Exception as db_error:
                db.rollback()
                logger.error(f"Database operation failed: {str(db_error)}")
                return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/get_orders_status_analysis', methods=['GET'])
def api_get_orders_status_analysis():
    with get_db() as db:
        try:
            orders_status_analysis, orders_status_analysis_list = query_table_data(Order_Status_Count, db)
            
            if orders_status_analysis_list:
                return jsonify({"status": "success",
                                "message": "Orders Status Analysis data retrieved successfully",
                                "body":  orders_status_analysis_list,
                                "count_records": len(orders_status_analysis_list)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Orders Status Analysis data available",
                                "body":  orders_status_analysis_list,
                                "count_records": len(orders_status_analysis)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all orders status analysis: {str(e)}")
            return jsonify({'error retrieving Orders Status Analysis data': str(e)}), 500


@app.route('/api/load_average_delivery_duration', methods=['POST'])
def api_load_average_delivery_duration():
    try:
        with get_db() as db:
            try:
                # Get all the tables
                fact_table = db.query(FactTable).all()

                if not fact_table:
                    return jsonify({'error': 'Fact table to be analyzed not found in the database'}), 404
                
                fact_table = process_dim_table_df(fact_table)

                orders_delivery_analysis_df = get_average_delivery_duration(fact_table) 

                if not isinstance(orders_delivery_analysis_df, pl.DataFrame):
                    return jsonify({'status': 'error',
                                    'message': 'transformed_data didnt yield a valid polars dataframe'}), 400
                
                list_dicts_df = df_to_list_of_dicts(orders_delivery_analysis_df)
                existing_orders_delivery_analysis, existing_orders_delivery_analysis_ids = query_existing_data(Top_Average_Delivery_Duration, list_dicts_df, db)
                # Insert only new orders_delivery_analysis
                new_orders_delivery_analysis = []
                for record in list_dicts_df:
                    if record['id'] not in existing_orders_delivery_analysis_ids:
                        orders_delivery_analysis = Top_Average_Delivery_Duration(
                            id = record['id'],
                            product_category_name_english=record['product_category_name_english'],
                            average_delivery_duration_days = record['Average_delivery_duration_days'],
                        )
                        new_orders_delivery_analysis.append(orders_delivery_analysis)
                        db.add(orders_delivery_analysis)
                        db.commit()
                        db.refresh(orders_delivery_analysis)

                orders_delivery_analysis_list = []
                orders_delivery_analysis_list = jsonify_loaded_data(new_orders_delivery_analysis, orders_delivery_analysis_list, Top_Average_Delivery_Duration)

                return jsonify({
                    'status': 'success',
                    'message': 'Data processed successfully',
                    'body': orders_delivery_analysis_list[:5],
                    'new_orders_delivery_analysis_count': len(new_orders_delivery_analysis),
                    'existing_orders_delivery_analysis_count': len(existing_orders_delivery_analysis)
                }), 200
            
            except Exception as db_error:
                db.rollback()
                logger.error(f"Database operation failed: {str(db_error)}")
                return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/get_average_delivery_duration', methods=['GET'])
def api_get_average_delivery_duration():
    with get_db() as db:
        try:
            orders_delivery_analysis, orders_delivery_analysis_list = query_table_data(Top_Average_Delivery_Duration, db)
            if orders_delivery_analysis_list:
                return jsonify({"status": "success",
                                "message": "Orders Delivery Analysis data retrieved successfully",
                                "body":  orders_delivery_analysis_list,
                                "count_records": len(orders_delivery_analysis_list)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Orders Delivery Analysis data available",
                                "body":  orders_delivery_analysis_list,
                                "count_records": len(orders_delivery_analysis)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all orders delivery analysis: {str(e)}")
            return jsonify({'error retrieving Orders Delivery Analysis data': str(e)}), 500

@app.route('/api/analyze_loyal_customers', methods=['POST'])
def api_load_loyal_customers():
    try:
        with get_db() as db:
            try:
                # Get all the tables
                fact_table = db.query(FactTable).all()

                if not fact_table:
                    return jsonify({'error': 'Fact table to be analyzed not found in the database'}), 404
                
                fact_table = process_dim_table_df(fact_table)
                loyal_customers_df = get_loyal_customers(fact_table) 

                if not isinstance(loyal_customers_df, pl.DataFrame):
                    return jsonify({'status': 'error',
                                    'message': 'transformed_data didnt yield a valid polars dataframe'}), 400
                
                list_dicts_df = df_to_list_of_dicts(loyal_customers_df)
                existing_loyal_customers, existing_loyal_customers_ids = query_existing_data(Loyal_Customers, list_dicts_df, db)
                # Insert only new loyal_customers
                new_loyal_customers = []
                for record in list_dicts_df:
                    if record['id'] not in existing_loyal_customers_ids:
                        loyal_customers = Loyal_Customers(
                            id = record['id'],
                            customer_unique_id=record['customer_unique_id'],
                            no_of_orders = record['no_of_orders'],
                        )
                        new_loyal_customers.append(loyal_customers)
                        db.add(loyal_customers)
                        db.commit()
                        db.refresh(loyal_customers)

                loyal_customers_list = []
                loyal_customers_list = jsonify_loaded_data(new_loyal_customers, loyal_customers_list, Loyal_Customers)
                
                return jsonify({
                    'status': 'success',
                    'message': 'Data processed successfully',
                    'body': loyal_customers_list[:5],
                    'new_loyal_customers_count': len(new_loyal_customers),
                    'existing_loyal_customers_count': len(existing_loyal_customers)
                }), 200
            
            except Exception as db_error:
                db.rollback()
                logger.error(f"Database operation failed: {str(db_error)}")
                return jsonify({'error': 'Database operation failed', 'details': str(db_error)}), 500
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_loyal_customers', methods=['GET'])
def api_get_loyal_customers():
    with get_db() as db:
        try:
            loyal_customers, loyal_customers_list = query_table_data(Loyal_Customers, db)
            if loyal_customers_list:
                return jsonify({"status": "success",
                                "message": "Loyal Customers data retrieved successfully",
                                "body":  loyal_customers_list,
                                "count_records": len(loyal_customers_list)
                            }), 200
            else:
                return jsonify({"status": "success",
                                "message": "No Loyal Customers data available",
                                "body":  loyal_customers_list,
                                "count_records": len(loyal_customers)
                            }), 200
            
        except Exception as e:
            logger.error(f"An error occurred retrieving all loyal customers: {str(e)}")
            return jsonify({'error retrieving Loyal Customers data': str(e)}), 500

