from pyspark.sql import SparkSession
from pyspark import SparkConf

class SparkConfigManager:
    def __init__(self, app_name="EmployeeManagementSystem"):
        self.app_name = app_name
        self.conf = SparkConf()
        
    def get_spark_session(self):
        """Create and configure Spark session"""
        spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")
        return spark