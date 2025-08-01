import unittest
import tempfile
import shutil
import os
from pyspark.sql import SparkSession
from services.employee_service import EmployeeService
from core.employee import Employee
from datetime import date
from utils.logging_utils import LoggingUtils

class TestEmployeeService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        LoggingUtils.setup_logging()
        cls.spark = SparkSession.builder.master("local[1]").appName("TestEmployeeService").getOrCreate()
        
        # Create temp directory for test data
        cls.test_dir = tempfile.mkdtemp()
        cls.employee_csv = os.path.join(cls.test_dir, "employees.csv")
        
        # Create test data
        data = [
            "employee_id,first_name,last_name,email,phone_number,hire_date,job_title,salary,department_id,manager_id",
            "1001,John,Doe,john@example.com,1234567890,2020-01-01,Engineer,75000.0,10,",
            "1002,Jane,Smith,jane@example.com,9876543210,2019-05-15,Manager,90000.0,10,"
        ]
        
        with open(cls.employee_csv, 'w') as f:
            f.write('\n'.join(data))
    
    def setUp(self):
        self.emp_service = EmployeeService(self.spark)
        # Patch the data path to use our test file
        self.emp_service.data_utils.load_csv = lambda _: self.spark.read.csv(self.employee_csv, header=True)
    
    def test_load_employees(self):
        """Test loading employees from CSV"""
        df = self.emp_service.load_employees()
        self.assertEqual(df.count(), 2)
        self.assertEqual(len(df.columns), 10)
    
    def test_add_employee(self):
        """Test adding a new employee"""
        new_emp = Employee(
            employee_id=1003,
            first_name="Alice",
            last_name="Johnson",
            email="alice@example.com",
            phone_number="5551234567",
            hire_date=date.today().strftime('%Y-%m-%d'),
            job_title="Developer",
            salary=80000.0,
            department_id=20,
            manager_id=1002
        )
        
        result = self.emp_service.add_employee(new_emp)
        self.assertTrue(result)
    
    def test_get_employees_by_department(self):
        """Test filtering employees by department"""
        employees = self.emp_service.get_employees_by_department(10)
        self.assertEqual(employees.count(), 2)
        
        employees = self.emp_service.get_employees_by_department(99)  # Non-existent department
        self.assertEqual(employees.count(), 0)
    
    def test_get_employee_salary_stats(self):
        """Test salary statistics calculation"""
        stats = self.emp_service.get_employee_salary_stats()
        self.assertEqual(stats.count(), 1)  # Only one department in test data
        row = stats.collect()[0]
        self.assertEqual(row['department_id'], 10)
        self.assertAlmostEqual(row['avg_salary'], 82500.0)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls.test_dir)

if __name__ == '__main__':
    unittest.main()