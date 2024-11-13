import os
import polars as pl
import numpy as np
from datetime import datetime
import unittest
from unittest.mock import patch
from src.processing import (
    load_data,
    transform_product_category_df,
    transform_df,
    process_fact_table,
    process_dim_table_df,
    get_top_sellers,
    get_top_selling_product_category,
    get_orders_status_count,
    get_average_delivery_duration,
    get_loyal_customers,
)

curr_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
data_dir = os.path.join(curr_dir, "data")
WORKING_DIR = "/Brazillian_Orders_Peter/data"

orders_df_path = f"{WORKING_DIR}/olist_order_items_dataset.csv"
order_items_df_path = f"{WORKING_DIR}/olist_orders_dataset.csv"
customers_df_path = f"{WORKING_DIR}/olist_customers_dataset.csv"
order_payments_df_path = f"{WORKING_DIR}/olist_order_payments_dataset.csv"
products_df_path = f"{WORKING_DIR}/olist_products_dataset.csv"
sellers_df_path = f"{WORKING_DIR}/olist_sellers_dataset.csv"
product_category_df_path = f"{WORKING_DIR}/product_category_name_translation.csv"

orders_df = load_data(orders_df_path)
order_items_df = load_data(order_items_df_path)
customers_df = load_data(customers_df_path)
order_payments_df = load_data(order_payments_df_path)
products_df = load_data(products_df_path)
sellers_df = load_data(sellers_df_path)
product_category_df = load_data(product_category_df_path)


class TestLoadData(unittest.TestCase):
    def test_load_data_none(self) -> None:
        """
        Tests the load_data function with a None file path

        Args:
            None

        Returns:
            None

        """
        file_path = None
        result = load_data(file_path)
        self.assertEqual(result, "Invalid file path or format provided")

    def test_load_data_invalid(self) -> None:
        """
        Tests the load_data function with an invalid file path
        Args:
            None
        Returns:
            None
        """
        file_path = "invalid_path"
        result = load_data(file_path)
        self.assertEqual(
            result, "File path does not exist or is not in the right format"
        )

    def test_load_data_valid(self) -> None:
        """
        Tests the load_data function with a valid file path
        Args:
            None
        Returns:
            None
        """

        file_path = os.path.join(data_dir, "olist_customers_dataset.csv")
        result = load_data(file_path).is_empty()
        self.assertEqual(result, False)


class TestTransformProductCategoryDF(unittest.TestCase):
    def setUp(self):
        self.sample_data = pl.DataFrame(
            {
                "product_category_name": ["category1", "category2", "category3"],
                "product_category_name_english": ["cat1", "cat2", "cat3"],
            }
        )

    def test_with_correct_df(self) -> None:
        """
        Tests the transform_product_category_df function with a valid product category dataframe

        Args:
            None

        Returns:
            None
        """
        file_path = os.path.join(data_dir, "product_category_name_translation.csv")
        df = load_data(file_path)
        result_df = transform_product_category_df(df)

        df_rows = len(df)
        result_rows = len(result_df)
        self.assertEqual(df_rows, result_rows)

        check_result = result_df.is_empty()
        self.assertEqual(check_result, False)

        self.assertIn("product_category_id", result_df.columns)
        self.assertEqual(result_df["product_category_id"].dtype, pl.Int64)
        expected_ids = pl.Series("product_category_id", np.arange(1, len(df) + 1))
        equal_val = expected_ids == result_df["product_category_id"]

        self.assertEqual(equal_val.all(), True)

        original_columns = set(self.sample_data.columns)
        result_columns = set(result_df.columns) - {"product_category_id"}
        self.assertEqual(original_columns, result_columns)

    def test_empty_dataframe(self) -> None:
        """
        Test the transform category function parameter with an empty DataFrame

        Args:
            None

        Returns:
            None
        """
        empty_df = pl.DataFrame()
        result_df = transform_product_category_df(empty_df)
        self.assertEqual(result_df, "Dataframe is empty")

    def test_with_string(self) -> None:
        """
        Tests the transform category function parameter with a string type

        Args:
            None

        Returns:
            None
        """

        result = transform_product_category_df("string")
        self.assertEqual(result, "Please provide a valid dataframe")

    def test_with_none(self) -> None:
        """
        Tests the transform category function parameter with a None type

        Args:
            None

        Returns:
            None
        """

        result = transform_product_category_df(None)
        self.assertEqual(result, "Please provide a valid dataframe")


class TestTransformDF(unittest.TestCase):
    def setUp(self):
        self.sample_data = pl.DataFrame(
            {
                "product_id": ["prod1", "prod2", "prod3"],
                "product_category_name": ["category1", "category2", "category3"],
                "product_name_lenght": ["name1", "name2", "name3"],
                "product_description_lenght": ["desc1", "desc2", "desc3"],
                "product_photos_qty": ["1", "2", "3"],
                "product_weight_g": [1, 2, 3],
                "product_length_cm": [1, 2, 3],
                "product_height_cm": [1, 2, 3],
                "product_width_cm": [1, 2, 3],
            }
        )

    def test_with_correct_df(self) -> None:
        """
        Tests the transform_df function with a valid product dataframe

        Args:
            None

        Returns:
            None
        """

        file_path = os.path.join(data_dir, "olist_products_dataset.csv")
        df = load_data(file_path)
        result_df = transform_df(df)

        df_rows = len(df)
        result_rows = len(result_df)
        self.assertEqual(df_rows, result_rows)
        check_result = result_df.is_empty()
        self.assertEqual(check_result, False)
        self.assertIn("id", result_df.columns)
        self.assertEqual(result_df["id"].dtype, pl.Int64)
        expected_ids = pl.Series("id", np.arange(1, len(df) + 1))
        equal_val = expected_ids == result_df["id"]

        self.assertEqual(equal_val.all(), True)
        original_columns = set(self.sample_data.columns)
        result_columns = set(result_df.columns) - {"id"}
        self.assertEqual(original_columns, result_columns)

    def test_empty_dataframe(self) -> None:
        """
        Test the transform category function parameter with an empty DataFrame

        Args:
            None

        Returns:
            None
        """
        empty_df = pl.DataFrame()
        result_df = transform_df(empty_df)
        self.assertEqual(result_df, "Dataframe is empty")

    def test_with_string(self) -> None:
        """
        Tests the transform category function parameter with a string type

        Args:
            None

        Returns:
            None
        """

        result = transform_df("string")
        self.assertEqual(result, "Please provide a valid dataframe")

    def test_with_none(self) -> None:
        """
        Tests the transform category function parameter with a None type

        Args:
            None

        Returns:
            None
        """

        result = transform_df(None)
        self.assertEqual(result, "Please provide a valid dataframe")


class TestProcessDimTableDF(unittest.TestCase):
    def setUp(self):
        self.mock_db_table = [
            MockDBRow(id=1, name="Peter", job="Data Engineer"),
            MockDBRow(id=2, name="John", job="Data Scientist"),
            MockDBRow(id=3, name="Jane", job="Software Engineer"),
        ]

    def test_with_valid_input(self) -> None:
        """
        Tests the process_dim_table_df function with a valid list of dataframes

        Args:
            None

        Returns:
            None
        """
        with patch(
            "src.processing.process_dim_table_df",
            return_value=pl.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Peter", "John", "Jane"],
                    "job": ["Data Engineer", "Data Scientist", "Software Engineer"],
                }
            ),
        ):
            result = process_dim_table_df(self.mock_db_table)
            self.assertIsInstance(result, pl.DataFrame)
            self.assertEqual(result.shape, (3, 3))
            self.assertListEqual(result.columns, ["id", "name", "job"])

            expected_df = pl.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Peter", "John", "Jane"],
                    "job": ["Data Engineer", "Data Scientist", "Software Engineer"],
                }
            )
            for col in result.columns:
                self.assertListEqual(result[col].to_list(), expected_df[col].to_list())

    def test_empty_list_tables(self) -> None:
        """
        Test the process_dim_table_df function with an empty list

        Args:
            None

        Returns:
            None
        """
        empty_list = []
        result_df = process_dim_table_df(empty_list)
        self.assertEqual(result_df, "Please provide a valid sqlalchemy table")

    def test_with_string(self) -> None:
        """
        Test the process_dim_table_df function parameter with a string type

        Args:
            None

        Returns:
            None
        """

        result = process_dim_table_df("string")
        self.assertEqual(result, "Please provide a valid sqlalchemy table")

    def test_with_none(self):
        "Test the process_dim_table_df function parameter with a None type"
        result = process_dim_table_df(None)
        self.assertEqual(result, "Please provide a valid sqlalchemy table")


class MockDBRow:
    def __init__(self, id, name, job):
        self.id = id
        self.name = name
        self.job = job

    @property
    def __table__(self):
        return self

    @property
    def columns(self):
        return MockColumns(["id", "name", "job"])


class MockColumns:
    def __init__(self, column_names):
        self.column_names = column_names

    def keys(self):
        return self.column_names


class TestProcessFactTable(unittest.TestCase):
    def test_with_correct_data(self) -> None:
        """
        Tests the process_fact_table function with a valid list of dataframes

        Args:
            None

        Returns:
            None
        """

        list_of_dfs = [
            orders_df,
            order_items_df,
            customers_df,
            order_payments_df,
            products_df,
            sellers_df,
            product_category_df,
        ]
        result = process_fact_table(list_of_dfs)
        self.assertIsInstance(result, pl.DataFrame)

    def test_empty_list(self) -> None:
        """
        Test the process_fact_table function with an empty list

        Args:
            None

        Returns:
            None
        """
        empty_list = []
        result = process_fact_table(empty_list)
        self.assertEqual(result, "Ensure you have a list of just polars dataframes")

    def test_invalid_list(self) -> None:
        """
        Test the process_fact_table function with an invalid list

        Args:
            None

        Returns:
            None
        """
        invalid_list = [
            "string",
            "string",
            "string",
            "string",
            "string",
            "string",
            "string",
        ]
        result = process_fact_table(invalid_list)
        self.assertEqual(result, "Ensure you have a list of just polars dataframes")

    def test_none_type(self) -> None:
        """
        Test the process_fact_table function with a None type

        Args:
            None

        Returns:
            None
        """
        result = process_fact_table(None)
        self.assertEqual(result, "Please provide a list of polars dataframes")

    def test_with_string(self):
        """
        Tests the process_fact_table function with a string type

        Args:
            None

        Returns:
            None
        """
        result = process_fact_table("string")
        self.assertEqual(result, "Please provide a list of polars dataframes")

    def test_with_int(self):
        """
        Tests the process_fact_table function with an integer type

        Args:
            None

        Returns:
            None
        """
        result = process_fact_table(123)
        self.assertEqual(result, "Please provide a list of polars dataframes")


class TestGetTopSellers(unittest.TestCase):
    def setUp(self):
        self.sample_data = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "order_id": ["order1", "order2", "order3", "order4"],
                "customer_id": ["customer1", "customer2", "customer3", "customer4"],
                "order_status": ["status1", "status2", "status3", "status4"],
                "order_purchase_timestamp": [
                    datetime(2021, 1, 1),
                    datetime(2021, 1, 2),
                    datetime(2021, 1, 3),
                    datetime(2021, 1, 4),
                ],
                "order_approved_at": [
                    datetime(2021, 1, 1),
                    datetime(2021, 1, 2),
                    datetime(2021, 1, 3),
                    datetime(2021, 1, 4),
                ],
                "order_delivered_carrier_date": [
                    datetime(2021, 1, 1),
                    datetime(2021, 1, 2),
                    datetime(2021, 1, 3),
                    datetime(2021, 1, 4),
                ],
                "order_delivered_customer_date": [
                    datetime(2021, 1, 2),
                    datetime(2021, 1, 3),
                    datetime(2021, 1, 4),
                    datetime(2021, 1, 5),
                ],
                "order_estimated_delivery_date": [
                    datetime(2021, 1, 3),
                    datetime(2021, 1, 4),
                    datetime(2021, 1, 5),
                    datetime(2021, 1, 6),
                ],
                "customer_unique_id": ["unique1", "unique2", "unique3", "unique4"],
                "customer_zip_code_prefix": [1, 2, 3, 4],
                "customer_city": ["city1", "city2", "city3", "city4"],
                "customer_state": ["state1", "state2", "state3", "state4"],
                "order_item_id": [1, 2, 3, 4],
                "product_id": ["prod1", "prod2", "prod3", "prod4"],
                "seller_id": ["seller1", "seller2", "seller3", "seller4"],
                "shipping_limit_date": [
                    datetime(2021, 1, 1),
                    datetime(2021, 1, 2),
                    datetime(2021, 1, 3),
                    datetime(2021, 1, 4),
                ],
                "price": [1.0, 2.0, 3.0, 4.0],
                "freight_value": [1, 2, 3, 4],
                "product_category_name": [
                    "category1",
                    "category2",
                    "category3",
                    "category4",
                ],
                "seller_zip_code_prefix": [1, 2, 3, 4],
                "seller_city": ["city1", "city2", "city3", "city4"],
                "seller_state": ["state1", "state2", "state3", "state4"],
                "product_category_name_english": ["cat1", "cat2", "cat3", "cat4"],
            }
        )

    def test_with_correct_data(self):
        """
        Tests the get_top_sellers function with a valid Fact table

        Args:
            None

        Returns:
            None
        """
        result = get_top_sellers(self.sample_data)
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.shape, (4, 3))
        self.assertListEqual(result.columns, ["id", "seller_id", "Total_sales"])
        expected_df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "seller_id": ["seller1", "seller2", "seller3", "seller4"],
                "Total_sales": [1.0, 2.0, 3.0, 4.0],
            }
        )

        for col in result.columns:
            self.assertEqual(
                set(result[col].to_list()), set(expected_df[col].to_list())
            )

    def test_empty_dataframe(self):
        """
        Tests the get_top_sellers function parameter with an empty DataFrame

        Args:
            None

        Returns:
            None
        """
        empty_df = pl.DataFrame()
        result_df = get_top_sellers(empty_df)
        self.assertEqual(result_df, "Dataframe is empty, Provide the valid Fact table")

    def test_with_string(self):
        """
        Tests the get_top_sellers function parameter with a string type

        Args:
            None

        Returns:
            None
        """

        result = get_top_sellers("string")
        self.assertEqual(result, "Please provide the valid Fact table")

    def test_with_none(self) -> None:
        """
        Tests the get_top_sellers function parameter with a None type

        Args:
            None

        Returns:
            None
        """

        result = get_top_sellers(None)
        self.assertEqual(result, "Please provide the valid Fact table")


class TestGetTopSellingProductCategory(unittest.TestCase):
    def setUp(self):
        fact_table_instance = TestGetTopSellers()
        fact_table_instance.setUp()
        self.sample_data = fact_table_instance.sample_data

    def test_with_correct_data(self):
        """
        Tests the get_top_selling_product_category function with a valid Fact table

        Args:
            None

        Returns:
            None
        """
        result = get_top_selling_product_category(self.sample_data)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.shape, (4, 3))
        self.assertListEqual(
            result.columns, ["id", "product_category_name_english", "Total_sales"]
        )

        expected_df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "product_category_name_english": ["cat1", "cat2", "cat3", "cat4"],
                "Total_sales": [1.0, 2.0, 3.0, 4.0],
            }
        )

        for col in result.columns:
            self.assertEqual(
                set(result[col].to_list()), set(expected_df[col].to_list())
            )

    def test_empty_dataframe(self):
        """
        Tests the get_top_selling_product_category function parameter with an empty DataFrame

        Args:
            None

        Returns:
            None
        """
        empty_df = pl.DataFrame()
        result_df = get_top_selling_product_category(empty_df)
        self.assertEqual(result_df, "Dataframe is empty, Provide the valid Fact table")

    def test_with_string(self):
        """
        Tests the get_top_selling_product_category function parameter with a string type

        Args:
            None

        Returns:
            None
        """
        result = get_top_selling_product_category("string")
        self.assertEqual(result, "Please provide the valid Fact table")

    def test_with_none(self):
        """
        Tests the get_top_selling_product_category function parameter with a None type

        Args:
            None

        Returns:
            None
        """
        result = get_top_selling_product_category(None)
        self.assertEqual(result, "Please provide the valid Fact table")


class TestGetOrdersStatusCount(unittest.TestCase):
    def setUp(self):
        fact_table_instance = TestGetTopSellingProductCategory()
        fact_table_instance.setUp()
        self.sample_data = fact_table_instance.sample_data

    def test_with_correct_data(self):
        """
        Tests the get_orders_status_count function with a valid Fact table

        Args:
            None

        Returns:
            None
        """
        result = get_orders_status_count(self.sample_data)
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.shape, (4, 3))
        self.assertListEqual(result.columns, ["id", "order_status", "Count"])
        expected_df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "order_status": ["status1", "status2", "status3", "status4"],
                "Count": [1, 1, 1, 1],
            }
        )

        for col in result.columns:
            self.assertEqual(
                set(result[col].to_list()), set(expected_df[col].to_list())
            )

    def test_empty_dataframe(self):
        """
        Tests the get_orders_status_count function parameter with an empty DataFrame

        Args:
            None

        Returns:
            None
        """
        empty_df = pl.DataFrame()
        result_df = get_orders_status_count(empty_df)
        self.assertEqual(result_df, "Dataframe is empty, Provide the valid Fact table")

    def test_with_string(self):
        """
        Tests the get_orders_status_count function parameter with a string type

        Args:
            None

        Returns:
            None
        """

        result = get_orders_status_count("string")
        self.assertEqual(result, "Please provide the valid Fact table")

    def test_with_none(self):
        """
        Test the get_orders_status_count function parameter with a None type

        Args:
            None

        Returns:
            None
        """
        result = get_orders_status_count(None)
        self.assertEqual(result, "Please provide the valid Fact table")


class TestGetAverageDeliveryDuration(unittest.TestCase):
    def setUp(self):
        fact_table_instance = TestGetOrdersStatusCount()
        fact_table_instance.setUp()
        self.sample_data = fact_table_instance.sample_data

    def test_with_correct_data(self):
        """
        Tests the get_average_delivery_duration function with a valid Fact table

        Args:
            None

        Returns:
            None
        """
        result = get_average_delivery_duration(self.sample_data)
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.shape, (4, 3))
        self.assertListEqual(
            result.columns,
            ["id", "product_category_name_english", "Average_delivery_duration_days"],
        )

        expected_df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "product_category_name_english": ["cat1", "cat2", "cat3", "cat4"],
                "Average_delivery_duration_days": [1.0, 1.0, 1.0, 1.0],
            }
        )

        for col in result.columns:
            self.assertEqual(
                set(result[col].to_list()), set(expected_df[col].to_list())
            )

    def test_empty_dataframe(self) -> None:
        """
        Test the get_average_delivery_duration function parameter with an empty DataFrame

        Args:
            None

        Returns:
            None
        """

        empty_df = pl.DataFrame()
        result_df = get_average_delivery_duration(empty_df)
        self.assertEqual(result_df, "Dataframe is empty, Provide the valid Fact table")

    def test_with_string(self) -> None:
        """
        Test the get_average_delivery_duration function parameter with a string type

        Args:
            None

        Returns:
            None
        """

        result = get_average_delivery_duration("string")
        self.assertEqual(result, "Please provide the valid Fact table")

    def test_with_none(self) -> None:
        """
        Test the get_average_delivery_duration function parameter with a None type

        Args:
            None

        Returns:
            None
        """

        result = get_average_delivery_duration(None)
        self.assertEqual(result, "Please provide the valid Fact table")


class TestGetLoyalCustomers(unittest.TestCase):
    def setUp(self):
        fact_table_instance = TestGetAverageDeliveryDuration()
        fact_table_instance.setUp()
        self.sample_data = fact_table_instance.sample_data

    def test_with_correct_data(self):
        """
        Tests the get_loyal_customers function with a valid Fact table

        Args:
            None

        Returns:
            None
        """
        result = get_loyal_customers(self.sample_data)
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.shape, (4, 3))
        self.assertListEqual(
            result.columns, ["id", "customer_unique_id", "no_of_orders"]
        )

        expected_df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "customer_unique_id": ["unique1", "unique2", "unique3", "unique4"],
                "no_of_orders": [1, 1, 1, 1],
            }
        )

        for col in result.columns:
            self.assertEqual(
                set(result[col].to_list()), set(expected_df[col].to_list())
            )

    def test_empty_dataframe(self) -> None:
        """
        Test the get_loyal_customers function parameter with an empty DataFrame

        Args:
            None

        Returns:
            None
        """
        empty_df = pl.DataFrame()
        result_df = get_loyal_customers(empty_df)
        self.assertEqual(result_df, "Dataframe is empty, Provide the valid Fact table")

    def test_with_string(self) -> None:
        """
        Test the get_loyal_customers function parameter with a string type

        Args:
            None

        Returns:
            None
        """

        result = get_loyal_customers("string")
        self.assertEqual(result, "Please provide the valid Fact table")

    def test_with_none(self) -> None:
        """
        Test the get_loyal_customers function parameter with a None type

        Args:
            None
        Returns:
            None
        """
        result = get_loyal_customers(None)
        self.assertEqual(result, "Please provide the valid Fact table")


if __name__ == "__main__":
    unittest.main()
