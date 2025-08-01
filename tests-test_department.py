import unittest
from pyspark.sql import SparkSession
from core.department import Department
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

class TestDepartment(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestDepartment").getOrCreate()
        
        schema = StructType([
            StructField("department_id", IntegerType(), False),
            StructField("department_name", StringType(), False),
            StructField("location", StringType(), False),
            StructField("budget", DoubleType(), False)
        ])
        
        data = [
            (10, "Engineering", "New York", 1000000.0),
            (20, "Marketing", "Chicago", 500000.0)
        ]
        
        cls.df = cls.spark.createDataFrame(data, schema)
    
    def test_from_dataframe(self):
        """Test creating Department from DataFrame"""
        departments = Department.from_dataframe(self.df)
        
        self.assertEqual(len(departments), 2)
        dept = departments[0]
        
        self.assertEqual(dept.department_id, 10)
        self.assertEqual(dept.department_name, "Engineering")
        self.assertEqual(dept.budget, 1000000.0)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()