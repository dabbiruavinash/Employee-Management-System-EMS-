import unittest
import tempfile
import shutil
import os
from pyspark.sql import SparkSession
from services.department_service import DepartmentService
from core.department import Department

class TestDepartmentService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestDepartmentService").getOrCreate()
        
        # Create temp directory for test data
        cls.test_dir = tempfile.mkdtemp()
        cls.dept_csv = os.path.join(cls.test_dir, "departments.csv")
        
        # Create test data
        data = [
            "department_id,department_name,location,budget",
            "10,Engineering,New York,1000000.0",
            "20,Marketing,Chicago,500000.0"
        ]
        
        with open(cls.dept_csv, 'w') as f:
            f.write('\n'.join(data))
    
    def setUp(self):
        self.dept_service = DepartmentService(self.spark)
        # Patch the data path to use our test file
        self.dept_service.data_utils.load_csv = lambda _: self.spark.read.csv(self.dept_csv, header=True)
    
    def test_load_departments(self):
        """Test loading departments from CSV"""
        df = self.dept_service.load_departments()
        self.assertEqual(df.count(), 2)
        self.assertEqual(len(df.columns), 4)
    
    def test_add_department(self):
        """Test adding a new department"""
        new_dept = Department(
            department_id=30,
            department_name="HR",
            location="Boston",
            budget=300000.0
        )
        
        result = self.dept_service.add_department(new_dept)
        self.assertTrue(result)
    
    def test_get_department_budget_summary(self):
        """Test budget summary calculation"""
        summary = self.dept_service.get_department_budget_summary()
        self.assertEqual(summary.count(), 2)  # Two locations in test data
        
        # Verify the total budget for New York
        ny_row = summary.filter("location = 'New York'").collect()[0]
        self.assertEqual(ny_row['total_budget'], 1000000.0)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls.test_dir)

if __name__ == '__main__':
    unittest.main()