from pyspark.sql import SparkSession, DataFrame
from utils.spark_utils import SparkUtils
from config.app_config import AppConfig
import logging

class DataUtils:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark_utils = SparkUtils()
        
    def load_csv(self, path: str, schema=None) -> DataFrame:
        """Load CSV file with optional schema"""
        try:
            if schema:
                return self.spark.read.csv(path, header=True, schema=schema)
            return self.spark.read.csv(path, header=True)
        except Exception as e:
            logging.error(f"Error loading CSV from {path}: {str(e)}")
            raise
            
    def save_csv(self, df: DataFrame, path: str) -> None:
        """Save DataFrame to CSV"""
        try:
            df.write.mode('overwrite').csv(path, header=True)
        except Exception as e:
            logging.error(f"Error saving CSV to {path}: {str(e)}")
            raise
    
    def write_to_database(self, df: DataFrame, table_name: str) -> None:
        """Write DataFrame to database"""
        try:
            df.write.format('jdbc') \
                .option('url', AppConfig.DATABASE_CONFIG['url']) \
                .option('dbtable', table_name) \
                .option('user', AppConfig.DATABASE_CONFIG['user']) \
                .option('password', AppConfig.DATABASE_CONFIG['password']) \
                .option('driver', AppConfig.DATABASE_CONFIG['driver']) \
                .mode('append') \
                .save()
        except Exception as e:
            logging.error(f"Error writing to database table {table_name}: {str(e)}")
            raise