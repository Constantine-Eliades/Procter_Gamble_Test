from common import *
from pydantic import BaseModel
import pandas as pd

class DataLoaderConfig(BaseModel):
    data_paths: dict[str, FilePath]
    output_path: DirectoryPath
    partition_by: List[str]

class DataLoader:    
    def __init__(self, config: DataLoaderConfig):
        """
        Initializes the DataLoader with the provided configuration.

        Args:
            config (DataLoaderConfig): Configuration object specifying data paths, output path, and partitioning.
        """
        self.output_path = config.output_path
        self.data_paths = config.data_paths
        self.partition_by = config.partition_by
        self.customers = None
        self.orders = None
        self.order_items = None
        self.products = None
        self.merged_data = None
        self.weekly_sales = None
        
    def load_data(self, table_name: string) -> pd.DataFrame:
        """
        Loads a CSV file into a Pandas DataFrame.

        Args:
            table_name (str): Name of the table corresponding to the data path.

        Returns:
            pd.DataFrame: Loaded data.
        """
        try:
            return pd.read_csv(self.data_paths[table_name])
        except FileNotFoundError:
            print(f"File not found: {table_name}")
            return pd.DataFrame()
        except pd.errors.EmptyDataError:
            print(f"Empty dataset: {table_name}")
            return pd.DataFrame()
        except pd.errors.ParserError:
            print(f"Error parsing file: {table_name}")
            return pd.DataFrame()

    def load_data_customers(self):
        """Loads the customers data."""
        self.customers = self.load_data('customers')

    def load_data_orders(self):
        """Loads the orders data."""
        self.orders = self.load_data('orders')

    def load_order_items(self):
        """Loads the order items data."""
        self.order_items = self.load_data('order_items')

    def load_data_products(self):
        """Loads the products data."""
        self.products = self.load_data('products')

    def merge_tables(self):
        """Combines tables (orders, order_items, customers, and products) into a merged DataFrame."""
        self.merged_data = pd.merge(self.orders, self.order_items, on='order_id', how='inner')
        self.merged_data = pd.merge(self.merged_data, self.customers, on='customer_id', how='inner')        
        self.merged_data = pd.merge(self.merged_data, self.products, on='product_id', how='inner')        
        # Select columns and rename them
        selected_columns = {
            'product_id': 'product_id',
            'order_purchase_timestamp': 'week_start_date',
            'product_category_name': 'product_category_name',
            'price': 'price_sum'
        }
        self.merged_data = self.merged_data.rename(columns=selected_columns)

        # Keep only the selected columns
        self.merged_data = self.merged_data[selected_columns.values()]                 

    def transform_data(self):
        """Performs necessary data transformations."""
        self.merged_data['week_start_date'] = pd.to_datetime(self.merged_data['week_start_date'])                
        self.merged_data = self.merged_data.set_index('week_start_date')

    def create_weekly_dataset(self):
        """Creates a weekly dataset for sales forecasting."""
        self.weekly_sales = self.merged_data.resample('W-Mon').sum()
        
    def save_weekly_sales_to_parquet(self):
        """
        Saves the weekly sales dataset to Parquet format, partitioned by the specified column.

        Raises:
            Exception: If there is an error saving the Parquet file.
        """
        try:            
            # Convert columns datatypes from object to strings
            self.weekly_sales['product_id'] = self.weekly_sales['product_id'].astype("string")
            self.weekly_sales['product_category_name'] = self.weekly_sales['product_category_name'].astype("string")                                                                         
            
            # Partitioned by the specified column(s)
            partition_cols = self.partition_by if isinstance(self.partition_by, List) else [self.partition_by]
            self.weekly_sales.to_parquet(self.output_path, partition_cols=partition_cols)                       
                        
            print(f"Parquet file saved successfully at: {self.output_path}")

        except Exception as e:
            print(f"Error saving Parquet file: {e}")            
            
    def load_data_save_to_parquet(self): 
        """
        Loads data from CSV files, performs data processing, and saves the weekly sales dataset to Parquet.

        This method encapsulates the entire data loading, processing, and saving workflow.
        """
        self.load_data_customers()
        self.load_data_orders()
        self.load_order_items()
        self.load_data_products()
        self.merge_tables()
        self.transform_data()
        self.create_weekly_dataset()
        self.save_weekly_sales_to_parquet()