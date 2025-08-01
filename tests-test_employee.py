import unittest
from pyspark.sql import SparkSession
from services.employee_service import EmployeeService
from core.employee import Employee
from datetime import date

class TestEmployeeService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("EmployeeServiceTest") \
            .master("local[2]") \
            .getOrCreate()
            
        cls.employee_service = EmployeeService(cls.spark)
    
    def test_add_employee(self):
        test_employee = Employee(
            employee_id=9999,
            first_name="Test",
            last_name="User",
            email="test@company.com",
            phone_number="123-456-7890",
            hire_date=date.today().strftime('%Y-%m-%d'),
            job_title="Tester",
            salary=50000.00,
            department_id=99,
            manager_id=None
        )
        
        result = self.employee_service.add_employee(test_employee)
        self.assertTrue(result)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()