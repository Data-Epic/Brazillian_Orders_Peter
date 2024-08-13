#adding the project root to the python path
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest 
from unittest.mock import patch, MagicMock
from io import BytesIO
import json
from src.api import (app, get_db, Sellers,
                     Customers,
                     Orders,
                        Order_Items,
                        Order_Payments,
                        Products,
                        Product_Category
                     )

class TestLoadSellers(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_sample_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking the database session
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                  "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{'id': '1', 'seller_id': 'seller1', 'seller_zip_code_prefix': '12345', 'seller_city': 'City1', 'seller_state': 'State1'},
                {'id': '2', 'seller_id': 'seller2', 'seller_zip_code_prefix': '67890', 'seller_city': 'City2', 'seller_state': 'State2'}
            ]}

        # Mocking database query results
        mock_session.query.return_value.filter.return_value.all.return_value = []
        
        mock_file = (BytesIO(b'mock,file,content'), 'sellers.csv')

        response = self.app.post('/api/load_sellers_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['new_sellers_count'], 2)
        self.assertEqual(data['existing_sellers_count'], 0)

    def test_api_load_sellers_data_no_file(self):
        response = self.app.post('/api/load_sellers_data')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_invalid_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):

        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                  "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data with invalid data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{"invalid_column": "data"}]}
        
        mock_file = (BytesIO(b'mock,file,content'), 'invalid_sellers.csv')
        response = self.app.post('/api/load_sellers_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        # print("response", response)
        # print("response", response.status_code)
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['error'], 'Invalid file, Kindly provide the Sellers csv file')


class TestGetSellers(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    def test_get_sellers(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_sellers = [
            Sellers(id=1, seller_id='seller1', seller_zip_code_prefix='12345', seller_city='City1', seller_state='State1'),
            Sellers(id=2, seller_id='seller2', seller_zip_code_prefix='67890', seller_city='City2', seller_state='State2')
        ]
        mock_session.query.return_value.all.return_value = mock_sellers

        response = self.app.get('/api/get_sellers')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "Sellers data retrieved successfully")
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['body'][0]['seller_id'], 'seller1')
        self.assertEqual(data['body'][1]['seller_id'], 'seller2')

    @patch('src.api.get_db')
    def test_get_sellers_empty(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.return_value.all.return_value = []

        response = self.app.get('/api/get_sellers')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "No Sellers data available")
        self.assertEqual(len(data['body']), 0)

    @patch('src.api.get_db')
    def test_get_sellers_error(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.side_effect = Exception('An error occurred')

        response = self.app.get('/api/get_sellers')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

class TestLoadCustomers(unittest.TestCase):
    
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_sample_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking the database session
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{'id': '1', 'customer_id': 'customer1', 'customer_unique_id': 'unique1', 'customer_zip_code_prefix':'12345', 'customer_city': 'City1', 'customer_state': 'State1'},
                {'id': '2', 'customer_id': 'customer2', 'customer_unique_id': 'unique2', 'customer_zip_code_prefix': '67890', 'customer_city': 'City2', 'customer_state': 'State2'}
            ]}

        # Mocking database query results
        mock_session.query.return_value.filter.return_value.all.return_value = []
        
        mock_file = (BytesIO(b'mock,file,content'), 'customers.csv')

        response = self.app.post('/api/load_customers_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['new_customers_count'], 2)
        self.assertEqual(data['existing_customers_count'], 0)

    def test_api_load_customers_data_no_file(self):
        response = self.app.post('/api/load_customers_data')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        # print("data", data)
        self.assertEqual(data['status'], 'error')

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_invalid_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):

        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data with invalid data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{"invalid_column": "data"}]}
        
        mock_file = (BytesIO(b'mock,file,content'), 'invalid_customers.csv')
        response = self.app.post('/api/load_customers_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['error'], 'Invalid file, Kindly provide the Customers csv file')

class TestGetCustomers(unittest.TestCase):
        
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    def test_with_valid_data(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_customers = [
            Customers(id=1, customer_id='customer1', customer_unique_id='unique1', customer_zip_code_prefix='12345', customer_city='City1', customer_state='State1'),
            Customers(id=2, customer_id='customer2', customer_unique_id='unique2', customer_zip_code_prefix='67890', customer_city='City2', customer_state='State2')
        ]
        mock_session.query.return_value.all.return_value = mock_customers

        response = self.app.get('/api/get_customers')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "Customers data retrieved successfully")
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['body'][0]['customer_id'], 'customer1')
        self.assertEqual(data['body'][1]['customer_id'], 'customer2')

    @patch('src.api.get_db')
    def test_get_customers_empty(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.return_value.all.return_value = []

        response = self.app.get('/api/get_customers')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "No Customers data available")
        self.assertEqual(len(data['body']), 0)

    @patch('src.api.get_db')
    def test_get_customers_error(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.side_effect = Exception('An error occurred')

        response = self.app.get('/api/get_customers')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

class TestLoadOrders(unittest.TestCase):
                
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_sample_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking the database functions
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{'id': '1', 'order_id': 'order1', 'customer_id': 'customer1', 'order_status': 'delivered', 'order_purchase_timestamp': '2021-01-01 00:00:00', 'order_approved_at': '2021-01-01 00:00:00', 'order_delivered_carrier_date': '2021-01-01 00:00:00', 'order_delivered_customer_date': '2021-01-01 00:00:00', 'order_estimated_delivery_date': '2021-01-01 00:00:00'},
                {'id': '2', 'order_id': 'order2', 'customer_id': 'customer2', 'order_status': 'shipped', 'order_purchase_timestamp': '2021-01-01 00:00:00', 'order_approved_at': '2021-01-01 00:00:00', 'order_delivered_carrier_date': '2021-01-01 00:00:00', 'order_delivered_customer_date': '2021-01-01 00:00:00', 'order_estimated_delivery_date': '2021-01-01 00:00:00'}
            ]}

        mock_file = (BytesIO(b'mock,file,content'), 'orders.csv')

        response = self.app.post('/api/load_orders_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['new_orders_count'], 2)
        self.assertEqual(data['existing_orders_count'], 0)

    def test_api_load_orders_data_no_file(self):
        response = self.app.post('/api/load_orders_data')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_invalid_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data with invalid data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{"invalid_column": "data"}]}
        
        mock_file = (BytesIO(b'mock,file,content'), 'invalid_orders.csv')
        response = self.app.post('/api/load_orders_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['error'], 'Invalid file, Kindly provide the Orders csv file')


class TestGetOrders(unittest.TestCase):
            
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    def test_with_valid_data(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_orders = [
            Orders(id=1, order_id='order1', customer_id='customer1', order_status='delivered', order_purchase_timestamp='2021-01-01 00:00:00', order_approved_at='2021-01-01 00:00:00', order_delivered_carrier_date='2021-01-01 00:00:00', order_delivered_customer_date='2021-01-01 00:00:00', order_estimated_delivery_date='2021-01-01 00:00:00'),
            Orders(id=2, order_id='order2', customer_id='customer2', order_status='shipped', order_purchase_timestamp='2021-01-01 00:00:00', order_approved_at='2021-01-01 00:00:00', order_delivered_carrier_date='2021-01-01 00:00:00', order_delivered_customer_date='2021-01-01 00:00:00', order_estimated_delivery_date='2021-01-01 00:00:00')
        ]
        mock_session.query.return_value.all.return_value = mock_orders

        response = self.app.get('/api/get_orders')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "Orders data retrieved successfully")
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['body'][0]['order_id'], 'order1')
        self.assertEqual(data['body'][1]['order_id'], 'order2')

    @patch('src.api.get_db')
    def test_get_orders_empty(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.return_value.all.return_value = []

        response = self.app.get('/api/get_orders')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
    
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "No Orders data available")
        self.assertEqual(len(data['body']), 0)
    
    @patch('src.api.get_db')
    def test_get_orders_error(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.side_effect = Exception('An error occurred')

        response = self.app.get('/api/get_orders')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

class TestLoadOrderItems(unittest.TestCase):
                
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_sample_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking the database functions
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{'id': '1', 'order_id': 'order1', 'order_item_id': 'item1', 'product_id': 'product1', 'seller_id': 'seller1', 'shipping_limit_date': '2021-01-01 00:00:00', 'price': 100.0, 'freight_value': 10.0},
                {'id': '2', 'order_id': 'order2', 'order_item_id': 'item2', 'product_id': 'product2', 'seller_id': 'seller2', 'shipping_limit_date': '2021-01-01 00:00:00', 'price': 200.0, 'freight_value': 20.0}
            ]}

        mock_file = (BytesIO(b'mock,file,content'), 'order_items.csv')

        response = self.app.post('/api/load_order_items_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['new_order_items_count'], 2)
        self.assertEqual(data['existing_order_items_count'], 0)

    def test_api_load_order_items_data_no_file(self):
        response = self.app.post('/api/load_order_items_data')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_invalid_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data with invalid data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{"invalid_column": "data"}]}
        
        mock_file = (BytesIO(b'mock,file,content'), 'invalid_order_items.csv')
        response = self.app.post('/api/load_order_items_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['error'], 'Invalid file, Kindly provide the Order Items csv file')

class TestLoadOrderPayments(unittest.TestCase):
            
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_sample_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking the database functions
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{'id': '1', 'order_id': 'order1', 'payment_sequential': 1, 'payment_type': 'credit_card', 'payment_installments': 1, 'payment_value': 100.0},
                {'id': '2', 'order_id': 'order2', 'payment_sequential': 2, 'payment_type': 'debit_card', 'payment_installments': 2, 'payment_value': 200.0}
            ]}

        mock_file = (BytesIO(b'mock,file,content'), 'order_payments.csv')

        response = self.app.post('/api/load_order_payments_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['new_order_payments_count'], 2)
        self.assertEqual(data['existing_order_payments_count'], 0)

    def test_api_load_order_payments_data_no_file(self):
        response = self.app.post('/api/load_order_payments_data')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_invalid_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data with invalid data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{"invalid_column": "data"}]}
        
        mock_file = (BytesIO(b'mock,file,content'), 'invalid_order_payments.csv')
        response = self.app.post('/api/load_order_payments_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['error'], 'Invalid file, Kindly provide the Order Payments csv file')

class TestGetOrderPayments(unittest.TestCase):
                
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    def test_with_valid_data(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_order_payments = [
            Order_Payments(id=1, order_id='order1', payment_sequential=1, payment_type='credit_card', payment_installments=1, payment_value=100.0),
            Order_Payments(id=2, order_id='order2', payment_sequential=2, payment_type='debit_card', payment_installments=2, payment_value=200.0)
        ]
        mock_session.query.return_value.all.return_value = mock_order_payments

        response = self.app.get('/api/get_order_payments')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "Order Payments data retrieved successfully")
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['body'][0]['order_id'], 'order1')
        self.assertEqual(data['body'][1]['order_id'], 'order2')

    @patch('src.api.get_db')
    def test_get_order_payments_empty(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.return_value.all.return_value = []

        response = self.app.get('/api/get_order_payments')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "No Order Payments data available")
        self.assertEqual(len(data['body']), 0)

    @patch('src.api.get_db')
    def test_get_order_payments_error(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.side_effect = Exception('An error occurred')

        response = self.app.get('/api/get_order_payments')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

class TestLoadProducts(unittest.TestCase):
                    
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_sample_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking the database functions
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}

        #mocking load_and_transform_data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{'id': '1', 'product_id': 'product1', 'product_category_name': 'category1', 'product_name_lenght': 10, 'product_description_lenght': 20, 'product_photos_qty': 1, 'product_weight_g': 100, 'product_length_cm': 10, 'product_height_cm': 10, 'product_width_cm': 10},
                {'id': '2', 'product_id': 'product2', 'product_category_name': 'category2', 'product_name_lenght': 20, 'product_description_lenght': 30, 'product_photos_qty': 2, 'product_weight_g': 200, 'product_length_cm': 20, 'product_height_cm': 20, 'product_width_cm': 20}
            ]}

        mock_file = (BytesIO(b'mock,file,content'), 'products.csv')

        response = self.app.post('/api/load_products_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['new_products_count'], 2)
        self.assertEqual(data['existing_products_count'], 0)

    def test_api_load_products_data_no_file(self):
        response = self.app.post('/api/load_products_data')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_invalid_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
    #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}

        #mocking load_and_transform_data with invalid data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{"invalid_column": "data"}]}

        mock_file = (BytesIO(b'mock,file,content'), 'invalid_products.csv')
        response = self.app.post('/api/load_products_data',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})

        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['error'], 'Invalid file, Kindly provide the Products csv file')

class TestGetProducts(unittest.TestCase):
                        
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    def test_with_valid_data(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_products = [
            Products(id=1, product_id='product1', product_category_name='category1', product_name_lenght=10, product_description_lenght=20, product_photos_qty=1, product_weight_g=100, product_length_cm=10, product_height_cm=10, product_width_cm=10),
            Products(id=2, product_id='product2', product_category_name='category2', product_name_lenght=20, product_description_lenght=30, product_photos_qty=2, product_weight_g=200, product_length_cm=20, product_height_cm=20, product_width_cm=20)
        ]
        mock_session.query.return_value.all.return_value = mock_products

        response = self.app.get('/api/get_products')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "Products data retrieved successfully")
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['body'][0]['product_id'], 'product1')
        self.assertEqual(data['body'][1]['product_id'], 'product2')

    @patch('src.api.get_db')
    def test_get_products_empty(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.return_value.all.return_value = []

        response = self.app.get('/api/get_products')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "No Products data available")
        self.assertEqual(len(data['body']), 0)

    @patch('src.api.get_db')
    def test_get_products_error(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.side_effect = Exception('An error occurred')

        response = self.app.get('/api/get_products')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')


class TestLoadProductCategory(unittest.TestCase):
                                
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_sample_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking the database functions
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{'id': '1', 'product_category_name': 'category1', 'product_category_name_english': 'category1_english'},
                {'id': '2', 'product_category_name': 'category2', 'product_category_name_english': 'category2_english'}
            ]}

        mock_file = (BytesIO(b'mock,file,content'), 'product_category.csv')

        response = self.app.post('/api/load_products_category',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Data processed successfully')
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['new_product_categories_count'], 2)
        self.assertEqual(data['existing_product_categories_count'], 0)

    def test_api_load_product_category_data_no_file(self):
        response = self.app.post('/api/load_products_category')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')

    @patch('src.api.get_db')
    @patch('src.api.upload_csv')
    @patch('src.api.load_and_transform_data')
    def test_with_invalid_data(self, mock_load_and_transform, mock_upload_csv, mock_get_db):
        #mocking upload_csv
        mock_upload_csv.return_value = {"status": "success",        
                "message": "File uploaded successfully",
                "file_path": "path/to/file.csv"}
        
        #mocking load_and_transform_data with invalid data
        mock_load_and_transform.return_value = {"status": "success",
            "message": "Data loaded and transformed successfully",
            "data": [{"invalid_column": "data"}]}
        
        mock_file = (BytesIO(b'mock,file,content'), 'invalid_product_category.csv')
        response = self.app.post('/api/load_products_category',
                                    content_type='multipart/form-data',
                                    data={'file': mock_file})
        
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['error'], 'Invalid file, Kindly provide the Product Category csv file')


class TestGetProductCategory(unittest.TestCase):
    
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch('src.api.get_db')
    def test_with_valid_data(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_product_categories = [
            Product_Category(id=1, product_category_name='category1', product_category_name_english='category1_english'),
            Product_Category(id=2, product_category_name='category2', product_category_name_english='category2_english')
        ]
        mock_session.query.return_value.all.return_value = mock_product_categories

        response = self.app.get('/api/get_products_category')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "Product Categories data retrieved successfully")
        self.assertEqual(len(data['body']), 2)
        self.assertEqual(data['body'][0]['product_category_name'], 'category1')
        self.assertEqual(data['body'][1]['product_category_name'], 'category2')

    @patch('src.api.get_db')
    def test_get_product_category_empty(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.return_value.all.return_value = []

        response = self.app.get('/api/get_products_category')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], "No Product Categories data available")
        self.assertEqual(len(data['body']), 0)

    @patch('src.api.get_db')
    def test_get_product_category_error(self, mock_get_db):
        mock_session = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_session

        mock_session.query.side_effect = Exception('An error occurred')

        response = self.app.get('/api/get_products_category')
        self.assertEqual(response.status_code, 500)
        data = json.loads(response.data.decode('utf-8'))
        self.assertEqual(data['status'], 'error')   

if __name__ == '__main__':
    unittest.main()

                        
                     