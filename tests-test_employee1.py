import unittest
from datetime import date
from pyspark.sql import SparkSession
from core.employee import Employee
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType

class TestEmployee(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestEmployee").getOrCreate()
        
        schema = StructType([
            StructField("employee_id", IntegerType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("email", StringType(), False),
            StructField("phone_number", StringType(), True),
            StructField("hire_date", DateType(), False),
            StructField("job_title", StringType(), False),
            StructField("salary", DoubleType(), False),
            StructField("department_id", IntegerType(), False),
            StructField("manager_id", IntegerType(), True)
        ])
        
        data = [
            (1001, "John", "Doe", "john@example.com", "1234567890", 
             date(2020, 1, 1), "Engineer", 75000.0, 10, None)
        ]
        
        cls.df = cls.spark.createDataFrame(data, schema)
    
    def test_from_dataframe(self):
        """Test creating Employee from DataFrame"""
        employees = Employee.from_dataframe(self.df)
        
        self.assertEqual(len(employees), 1)
        emp = employees[0]
        
        self.assertEqual(emp.employee_id, 1001)
        self.assertEqual(emp.first_name, "John")
        self.assertEqual(emp.job_title, "Engineer")
        self.assertEqual(emp.salary, 75000.0)
        self.assertIsNone(emp.manager_id)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()