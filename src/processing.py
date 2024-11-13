import polars as pl
import numpy as np


def load_data(file_path: str) -> pl.DataFrame:
    """
    This function reads the data from the file path and returns a polars dataframe

    Args:
    file_path: str: The path to the file to be read

    Returns:
    pl.DataFrame: A polars dataframe

    """
    try:
        if isinstance(file_path, str):
            csv_format = file_path.endswith(".csv")
            excel_format = file_path.endswith(".xlsx")

            if csv_format:
                data_read_csv = pl.read_csv(file_path)
                if not data_read_csv.is_empty():
                    return data_read_csv
                else:
                    return "Error reading csv file"
            elif excel_format:
                data_read_excel = pl.read_excel(file_path)
                if not data_read_excel.is_empty():
                    return data_read_excel
                else:
                    return "Error reading excel file"
            else:
                return "File path does not exist or is not in the right format"
        else:
            return "Invalid file path or format provided"

    except Exception as e:
        return {"status": "error", "message": "Error reading the file", "error": str(e)}


def transform_product_category_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function adds an id column to the product category dataframe

    Args:
    df: pl.DataFrame: The dataframe to be transformed

    Returns:
    pl.DataFrame: A polars dataframe with an id column

    """
    try:
        if isinstance(df, pl.DataFrame):
            if not df.is_empty():
                col_name = "product_category_id"
                df = df.with_columns(pl.Series(col_name, np.arange(1, len(df) + 1)))
                df = df.with_columns(pl.col(col_name).cast(pl.Int64))
                return df
            elif df.is_empty():
                return "Dataframe is empty"
        else:
            return "Please provide a valid dataframe"
    except Exception as e:
        return {
            "status": "error",
            "message": "Error transforming the dataframe",
            "error": str(e),
        }


def transform_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function adds an id column to the polars dataframe

    Args:
    df: pl.DataFrame: The dataframe to be transformed

    Returns:
    pl.DataFrame: A polars dataframe with an id column
    """
    try:
        if isinstance(df, pl.DataFrame):
            if not df.is_empty():
                col_name = "id"
                df = df.with_columns(pl.Series(col_name, np.arange(1, len(df) + 1)))
                df = df.with_columns(pl.col(col_name).cast(pl.Int64))
                return df
            elif df.is_empty():
                return "Dataframe is empty"
        else:
            return "Please provide a valid dataframe"
    except Exception as e:
        return {
            "status": "error",
            "message": "Error transforming the dataframe",
            "error": str(e),
        }


def process_dim_table_df(db_table: list) -> pl.DataFrame:
    """
    This function processes the dim tables from the database
    and returns a polars dataframe

    Args:
    db_table: list: A list of sqlalchemy tables

    Returns:
    pl.DataFrame: A polars dataframe

    """
    try:
        if isinstance(db_table, list) and db_table:
            df_columns = db_table[0].__table__.columns.keys()
            # convert the sqlalchemy table to a dictionary
            df_dict = {
                col: [getattr(row, f"{col}") for row in db_table] for col in df_columns
            }
            df = pl.DataFrame(df_dict)
            df = transform_df(df)
            return df
        else:
            return "Please provide a valid sqlalchemy table"
    except Exception as e:
        return {
            "status": "error",
            "message": "Error processing the dim table",
            "error": str(e),
        }


def process_fact_table(list_of_dfs: list, no_tables=7) -> pl.DataFrame:
    """
    Function to join the dimension tables on the common keys and establish a fact table

    Args:
    list_of_dfs: list: A list of polars dataframes
    no_tables: int: The number of tables to be joined

    Returns:
    pl.DataFrame: A polars dataframe
    """
    try:
        if isinstance(list_of_dfs, list):
            all_dfs = []
            if len(list_of_dfs) != 0:
                for df in list_of_dfs:
                    if isinstance(df, pl.DataFrame):
                        all_dfs.append(True)

            if len(all_dfs) == no_tables:
                (
                    orders_df,
                    order_items_df,
                    customers_df,
                    order_payments_df,
                    products_df,
                    sellers_df,
                    product_category_df,
                ) = list_of_dfs
                if "id" in (
                    orders_df.columns
                    and order_items_df.columns
                    and customers_df.columns
                    and order_payments_df.columns
                    and products_df.columns
                    and sellers_df.columns
                    and product_category_df.columns
                ):
                    orders_df = orders_df.drop("id")
                    order_items_df = order_items_df.drop("id")
                    customers_df = customers_df.drop("id")
                    order_payments_df = order_payments_df.drop("id")
                    products_df = products_df.drop("id")
                    sellers_df = sellers_df.drop("id")
                    product_category_df = product_category_df.drop("id")

                fact_table = (
                    orders_df.join(order_items_df, on="order_id", how="inner")
                    .join(customers_df, on="customer_id", how="inner")
                    .join(
                        products_df.select(["product_id", "product_category_name"]),
                        on="product_id",
                        how="inner",
                    )
                    .join(sellers_df, on="seller_id", how="inner")
                    .join(
                        product_category_df, on=["product_category_name"], how="inner"
                    )
                )

                fact_table = fact_table.with_columns(
                    pl.Series("id", np.arange(1, len(fact_table) + 1))
                )
                fact_table = fact_table.with_columns(pl.col("id").cast(pl.Int64))

                return fact_table

            else:
                return "Ensure you have a list of just polars dataframes"
        else:
            return "Please provide a list of polars dataframes"

    except Exception as e:
        return {
            "status": "error",
            "message": "Error processing the fact table",
            "error": str(e),
        }


def get_top_sellers(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function returns the top selling sellers

    Args:
    df: pl.DataFrame: The fact table

    Returns:
    pl.DataFrame: A polars dataframe
    """
    try:
        if isinstance(df, pl.DataFrame):
            if not df.is_empty():
                top_selling_sellers = (
                    df.group_by("seller_id")
                    .agg([pl.sum("price").alias("Total_sales")])
                    .sort("Total_sales", descending=True)
                    .select(["seller_id", "Total_sales"])
                    .head(10)
                )

                top_selling_sellers = top_selling_sellers.with_columns(
                    pl.Series("id", np.arange(1, len(top_selling_sellers) + 1))
                )

                top_selling_sellers = top_selling_sellers.with_columns(
                    pl.col("id").cast(pl.Int64)
                )

                top_selling_sellers = top_selling_sellers.select(
                    ["id", "seller_id", "Total_sales"]
                )

                return top_selling_sellers
            else:
                return "Dataframe is empty, Provide the valid Fact table"
        else:
            return "Please provide the valid Fact table"

    except Exception as e:
        return {
            "status": "error",
            "message": "Error getting the top sellers",
            "error": str(e),
        }


def get_top_selling_product_category(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function gets the top selling product category

    Args:
    df: pl.DataFrame: The fact table

    Returns:
    pl.DataFrame: A polars dataframe
    """
    try:
        if isinstance(df, pl.DataFrame):
            if not df.is_empty():
                top_selling_product_category = (
                    df.group_by("product_category_name_english")
                    .agg([pl.sum("price").alias("Total_sales")])
                    .sort("Total_sales", descending=True)
                    .select(["product_category_name_english", "Total_sales"])
                    .head(10)
                )
                top_selling_product_category = (
                    top_selling_product_category.with_columns(
                        pl.Series(
                            "id", np.arange(1, len(top_selling_product_category) + 1)
                        )
                    )
                )
                top_selling_product_category = (
                    top_selling_product_category.with_columns(
                        pl.col("id").cast(pl.Int64)
                    )
                )

                top_selling_product_category = top_selling_product_category.select(
                    ["id", "product_category_name_english", "Total_sales"]
                )

                return top_selling_product_category
            else:
                return "Dataframe is empty, Provide the valid Fact table"
        else:
            return "Please provide the valid Fact table"

    except Exception as e:
        return {
            "status": "error",
            "message": "Error getting the top selling product category",
            "error": str(e),
        }


def get_orders_status_count(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function gets the order status count

    Args:
    df: pl.DataFrame: The fact table

    Returns:
    pl.DataFrame: A polars dataframe

    """
    try:
        if isinstance(df, pl.DataFrame):
            if not df.is_empty():
                order_status_count = (
                    df.group_by("order_status")
                    .agg([pl.count("order_status").alias("Count")])
                    .sort("Count", descending=True)
                    .select(["order_status", "Count"])
                    .head(10)
                )

                order_status_count = order_status_count.with_columns(
                    pl.Series("id", np.arange(1, len(order_status_count) + 1))
                )
                order_status_count = order_status_count.with_columns(
                    pl.col("id").cast(pl.Int64)
                )

                order_status_count = order_status_count.select(
                    ["id", "order_status", "Count"]
                )

                return order_status_count
            else:
                return "Dataframe is empty, Provide the valid Fact table"
        else:
            return "Please provide the valid Fact table"

    except Exception as e:
        return {
            "status": "error",
            "message": "Error getting the order status count",
            "error": str(e),
        }


def get_average_delivery_duration(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function gets the average delivery duration

    Args:
    df: pl.DataFrame: The fact table

    Returns:
    pl.DataFrame: A polars dataframe
    """
    try:
        if isinstance(df, pl.DataFrame):
            if not df.is_empty():
                delivery_time = df.with_columns(
                    (
                        pl.col("order_delivered_customer_date")
                        - pl.col("order_purchase_timestamp")
                    ).alias("delivery_duration")
                )

                # convert the delivery duration to days
                delivery_time = delivery_time.with_columns(
                    (pl.col("delivery_duration") / (24 * 60 * 60 * 1000000))
                    .cast(pl.Float64)
                    .alias("delivery_duration_days")
                )

                delivery_time = (
                    delivery_time.group_by("product_category_name_english")
                    .agg(
                        [
                            pl.col("delivery_duration_days")
                            .mean()
                            .alias("Average_delivery_duration_days")
                        ]
                    )
                    .select(
                        [
                            "product_category_name_english",
                            "Average_delivery_duration_days",
                        ]
                    )
                    .sort("Average_delivery_duration_days", descending=True)
                    .head(10)
                )

                delivery_time = delivery_time.with_columns(
                    pl.Series("id", np.arange(1, len(delivery_time) + 1))
                )
                delivery_time = delivery_time.with_columns(pl.col("id").cast(pl.Int64))

                delivery_time = delivery_time.select(
                    [
                        "id",
                        "product_category_name_english",
                        "Average_delivery_duration_days",
                    ]
                )

                return delivery_time
            else:
                return "Dataframe is empty, Provide the valid Fact table"
        else:
            return "Please provide the valid Fact table"

    except Exception as e:
        return {
            "status": "error",
            "message": "Error getting the average delivery duration",
            "error": str(e),
        }


def get_loyal_customers(df: pl.DataFrame) -> pl.DataFrame:
    """
    This function gets the loyal customers

    Args:
    df: pl.DataFrame: The fact table

    Returns:
    pl.DataFrame: A polars dataframe

    """
    try:
        if isinstance(df, pl.DataFrame):
            if not df.is_empty():
                loyal_customers = (
                    df.group_by("customer_unique_id")
                    .agg(
                        [
                            pl.count("customer_id").alias("no_of_orders"),
                        ]
                    )
                    .sort("no_of_orders", descending=True)
                    .head(10)
                )

                loyal_customers = loyal_customers.with_columns(
                    pl.Series("id", np.arange(1, len(loyal_customers) + 1))
                )
                loyal_customers = loyal_customers.with_columns(
                    pl.col("id").cast(pl.Int64)
                )

                loyal_customers = loyal_customers.select(
                    ["id", "customer_unique_id", "no_of_orders"]
                )

                return loyal_customers
            else:
                return "Dataframe is empty, Provide the valid Fact table"
        else:
            return "Please provide the valid Fact table"

    except Exception as e:
        return {
            "status": "error",
            "message": "Error getting the loyal customers",
            "error": str(e),
        }
