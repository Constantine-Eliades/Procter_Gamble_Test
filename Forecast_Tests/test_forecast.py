import pytest
from load_data import DataLoader, DataLoaderConfig
from common import *

@pytest.fixture
def config_data():
    # Load configuration data from config.yml
    with open('config.yml', 'r') as config_file:
        return yaml.safe_load(config_file)

@pytest.fixture
def data_loader(config_data):    
    config = DataLoaderConfig(**config_data)    
    return DataLoader(config)

def test_customers_table_not_empty(data_loader):
    data_loader.load_data_customers()
    assert not data_loader.customers.empty, "Customers table is empty"

def test_orders_table_not_empty(data_loader):
    data_loader.load_data_orders()
    assert not data_loader.orders.empty, "Orders table is empty"

def test_order_items_table_not_empty(data_loader):
    data_loader.load_order_items()
    assert not data_loader.order_items.empty, "Order Items table is empty"

def test_products_table_not_empty(data_loader):
    data_loader.load_data_products()
    assert not data_loader.products.empty, "Products table is empty"