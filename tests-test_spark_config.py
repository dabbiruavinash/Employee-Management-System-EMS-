import unittest
from config.spark_config import SparkConfigManager
from pyspark.sql import SparkSession

class TestSparkConfigManager(unittest.TestCase):
    def test_spark_session_creation(self):
        """Test Spark session creation"""
        manager = SparkConfigManager("TestApp")
        spark = manager.get_spark_session()
        
        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(spark.sparkContext.appName, "TestApp")
        self.assertEqual(spark.sparkContext.getConf().get("spark.executor.memory"), "2g")
        
        spark.stop()

    def test_spark_session_singleton(self):
        """Test that getOrCreate returns same session"""
        manager = SparkConfigManager()
        spark1 = manager.get_spark_session()
        spark2 = manager.get_spark_session()
        
        self.assertEqual(spark1, spark2)
        spark1.stop()

if __name__ == '__main__':
    unittest.main()