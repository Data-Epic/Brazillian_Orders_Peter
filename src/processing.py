import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import polars as pl
import os
import numpy as np
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import DeclarativeMeta

def load_data(file_path) -> pl.DataFrame:
    try:
        if isinstance(file_path, str):
            csv_format = file_path.endswith(".csv")
            excel_format = file_path.endswith(".xlsx")
            
            #if the file exists in the working directory
            if csv_format:
                data_read_csv = pl.read_csv(file_path)
                if data_read_csv.is_empty() == False:
                    return data_read_csv
                else:
                    return "Error reading csv file"
            elif excel_format:
                data_read_excel = pl.read_excel(file_path)
                if data_read_excel.is_empty() == False:
                    return data_read_excel
                else:
                    return "Error reading excel file"
            else:
                return "File path does not exist or is not in the right format"
        else:
            return "Invalid file path or format provided"

    except ValueError as e:
        print(f"An error occurred: {e}")
        return e
    


def check_missing_duplcates(df: pl.DataFrame, df_name: str):
    null_df = df.filter(df.is_empty() == True)
    null_count = null_df.shape[0]
    print(f"{df_name} Null count: {null_count}")
    unique_count = df.n_unique()
    filtered_df = df.filter(df.is_duplicated() == False)
    if unique_count == filtered_df.shape[0]:
        print(f"No duplicates found in {df_name}")


#i Want to link these tables together and establish a fact table
#I will join the dataframes on the common columns
#I wnant to create a unique for the product category dataframe,
#  so that we can join it with the other dataframes

def transform_product_category_df(df) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        if df.is_empty() == False:
            col_name = "product_category_id"
            df = df.with_columns(pl.Series(col_name, np.arange(1, len(df) + 1)))
            df = df.with_columns(pl.col(col_name).cast(pl.Int64))
            return df
        elif df.is_empty() == True:
            return "Dataframe is empty"
    else:
        return "Please provide a valid dataframe"

def transform__df(df) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        if df.is_empty() == False:
            col_name = "id"
            df = df.with_columns(pl.Series(col_name, np.arange(1, len(df) + 1)))
            df = df.with_columns(pl.col(col_name).cast(pl.Int64))
            return df
        elif df.is_empty() == True:
            return "Dataframe is empty"
    else:
        return "Please provide a valid dataframe"

def process_dim_table_df(db_table: list) -> pl.DataFrame:
    """
    This adds an id column to the dataframe
    """
    if isinstance(db_table, list) == True and db_table:
        df_columns = db_table[0].__table__.columns.keys()
        
        df_dict = {col: [getattr(row, f'{col}') for row in db_table] for col in df_columns}
        df = pl.DataFrame(
                df_dict    
            )
        df = transform__df(df)
        # print(df)
        return df
    else:
        return "Please provide a valid sqlalchemy table"

        
def process_fact_table(list_of_dfs: list,
                       no_tables = 7) -> pl.DataFrame:
    """
    Function to join the dataframes on the common columns and establish a fact table
    """

    if isinstance(list_of_dfs, list) == True:
        all_dfs = []
        if len(list_of_dfs) != 0:
            for df in list_of_dfs:
                if isinstance(df, pl.DataFrame):
                    all_dfs.append(True)
 
        if len(all_dfs) == no_tables: 
            orders_df , \
            order_items_df, \
            customers_df,order_payments_df,products_df,sellers_df, product_category_df = list_of_dfs
            if 'id' in (orders_df.columns
                        and order_items_df.columns
                        and customers_df.columns
                        and order_payments_df.columns
                        and products_df.columns
                        and sellers_df.columns
                        and product_category_df.columns):
                
                orders_df = orders_df.drop('id')
                order_items_df = order_items_df.drop('id')
                customers_df = customers_df.drop('id')
                order_payments_df = order_payments_df.drop('id')
                products_df = products_df.drop('id')
                sellers_df = sellers_df.drop('id')
                product_category_df = product_category_df.drop('id')

            fact_table = orders_df.join(
                order_items_df, on="order_id", how="inner"  
                        ). \
                join(customers_df, on="customer_id", how="inner"). \
                    join(products_df.select(["product_id", "product_category_name"])
                ,on="product_id", how="inner").\
                    join(sellers_df, on="seller_id", how="inner"). \
                join(product_category_df, on=["product_category_name"], how="inner")

            print("fact_table_columns: ", fact_table.columns)

            fact_table = fact_table.with_columns(pl.Series("id", np.arange(1, len(fact_table) + 1)))
            fact_table = fact_table.with_columns(pl.col("id").cast(pl.Int64))

            return fact_table

        else:
            return "Ensure you have a list of just polars dataframes"
    else:
        return "Please provide a list of polars dataframes"

        
        

#analysis
#i want to get the top selling sellers
#i want to get the most used payment type
#I want to get the top loyal customers
#I want to get the top selling product category
#I want to get  the order status count 
#I want to get the average delivery time for each product category
#I want to get the average payment value by payment type

def get_top_sellers(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function returns the top selling sellers
    """
    if isinstance(df, pl.DataFrame) == True:
        if df.is_empty() == False:
            top_selling_sellers = df.group_by("seller_id").agg([
                pl.sum("price").alias("Total_sales")
            ]).sort("Total_sales", descending = True).select(["seller_id", "Total_sales"]).head(10)

            top_selling_sellers = top_selling_sellers.with_columns(pl.Series("id",
                                                                            np.arange(1, len(top_selling_sellers) + 1)))
            top_selling_sellers = top_selling_sellers.with_columns(pl.col("id").cast(pl.Int64))

            #rearrange the columns
            top_selling_sellers = top_selling_sellers.select(["id", "seller_id", "Total_sales"])

            print("top sellers: ", top_selling_sellers)
            return top_selling_sellers
        else:
            return "Dataframe is empty, Provide the valid Fact table"
    else:
        return "Please provide the valid Fact table"

def get_top_selling_product_category(df: pl.DataFrame) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame) == True:
        if df.is_empty() == False:
            top_selling_product_category = df.group_by("product_category_name_english").agg([
                pl.sum("price").alias("Total_sales")
            ]).sort("Total_sales", descending = True) \
                .select(["product_category_name_english", "Total_sales"]).head(10)
            top_selling_product_category = top_selling_product_category.with_columns(pl.Series("id", np.arange(1, len(top_selling_product_category) + 1)))
            top_selling_product_category = top_selling_product_category.with_columns(pl.col("id").cast(pl.Int64))

            top_selling_product_category = top_selling_product_category.select(["id", "product_category_name_english", "Total_sales"])

            # print("top selling product category: ", top_selling_product_category)
            return top_selling_product_category
        else:
            return "Dataframe is empty, Provide the valid Fact table"
    else:
        return "Please provide the valid Fact table"

def get_orders_status_count(df: pl.DataFrame) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame) == True:
        if df.is_empty() == False:
            order_status_count = df.group_by("order_status").agg([
                pl.count("order_status").alias("Count")
            ]).sort("Count", descending = True).select(["order_status", "Count"]).head(10)

            order_status_count = order_status_count.with_columns(pl.Series("id", np.arange(1, len(order_status_count) + 1)))
            order_status_count = order_status_count.with_columns(pl.col("id").cast(pl.Int64))

            order_status_count = order_status_count.select(["id", "order_status", "Count"])

            # print("order status count: ", order_status_count)
            return order_status_count
        else:
            return "Dataframe is empty, Provide the valid Fact table"
    else:
        return "Please provide the valid Fact table"

def get_average_delivery_duration(df: pl.DataFrame) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame) == True:
        if df.is_empty() == False:
            delivery_time = df. \
                with_columns((pl.col("order_delivered_customer_date")\
                            - pl.col("order_purchase_timestamp")) \
                                .alias("delivery_duration"))
            
            
            #convert the delivery duration to days
            delivery_time = delivery_time.with_columns((pl.col("delivery_duration") / (24 * 60 * 60*1000000)) \
                                                    .cast(pl.Float64).alias("delivery_duration_days")) 
            
            print("delivery time_ss: ", delivery_time)

            delivery_time = delivery_time.group_by("product_category_name_english") \
                    .agg([pl.col("delivery_duration_days").mean().alias("Average_delivery_duration_days")]) \
                            .select(["product_category_name_english", "Average_delivery_duration_days"]) \
                                .sort("Average_delivery_duration_days", descending = True).head(10)

            delivery_time = delivery_time.with_columns(pl.Series("id", np.arange(1, len(delivery_time) + 1)))
            delivery_time = delivery_time.with_columns(pl.col("id").cast(pl.Int64))

            delivery_time = delivery_time.select(["id", "product_category_name_english", "Average_delivery_duration_days"])

            print("average delivery time: ", delivery_time)
            return delivery_time
        else:
            return "Dataframe is empty, Provide the valid Fact table"
    else:
        return "Please provide the valid Fact table"

def get_loyal_customers(df: pl.DataFrame) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame) == True:
        if df.is_empty() == False:
            loyal_customers = df.group_by("customer_unique_id").agg([
                pl.count("customer_id").alias("no_of_orders"),
            ]).sort("no_of_orders", descending = True).head(10)

            loyal_customers = loyal_customers.with_columns(pl.Series("id", np.arange(1, len(loyal_customers) + 1)))
            loyal_customers = loyal_customers.with_columns(pl.col("id").cast(pl.Int64))

            loyal_customers = loyal_customers.select(["id", "customer_unique_id", "no_of_orders"])

            print("loyal customers: ", loyal_customers)
            return loyal_customers
        else:
            return "Dataframe is empty, Provide the valid Fact table"
    else:
        return "Please provide the valid Fact table"
