import unittest
from utils.validation_utils import ValidationUtils
from core.employee import Employee
from core.department import Department
from datetime import date

class TestValidationUtils(unittest.TestCase):
    def setUp(self):
        self.validator = ValidationUtils()
    
    def test_validate_employee(self):
        """Test employee validation"""
        valid_employee = Employee(
            employee_id=1001,
            first_name="John",
            last_name="Doe",
            email="john@example.com",
            phone_number="1234567890",
            hire_date="2020-01-01",
            job_title="Engineer",
            salary=75000.0,
            department_id=10,
            manager_id=None
        )
        
        self.assertTrue(self.validator.validate_employee(valid_employee))
        
        # Test invalid cases
        invalid_employee = valid_employee
        invalid_employee.employee_id = 0  # Invalid ID
        self.assertFalse(self.validator.validate_employee(invalid_employee))
        
        invalid_employee = valid_employee
        invalid_employee.email = "invalid-email"  # Invalid email
        self.assertFalse(self.validator.validate_employee(invalid_employee))
        
        invalid_employee = valid_employee
        invalid_employee.salary = -1000.0  # Negative salary
        self.assertFalse(self.validator.validate_employee(invalid_employee))
    
    def test_validate_department(self):
        """Test department validation"""
        valid_department = Department(
            department_id=10,
            department_name="Engineering",
            location="New York",
            budget=1000000.0
        )
        
        self.assertTrue(self.validator.validate_department(valid_department))
        
        # Test invalid cases
        invalid_department = valid_department
        invalid_department.department_id = 0  # Invalid ID
        self.assertFalse(self.validator.validate_department(invalid_department))
        
        invalid_department = valid_department
        invalid_department.department_name = ""  # Empty name
        self.assertFalse(self.validator.validate_department(invalid_department))
        
        invalid_department = valid_department
        invalid_department.budget = -50000.0  # Negative budget
        self.assertFalse(self.validator.validate_department(invalid_department))
    
    def test_validate_email(self):
        """Test email validation"""
        self.assertTrue(self.validator._validate_email("valid@example.com"))
        self.assertTrue(self.validator._validate_email("first.last@sub.domain.com"))
        self.assertFalse(self.validator._validate_email("invalid-email"))
        self.assertFalse(self.validator._validate_email("missing@.com"))
        self.assertFalse(self.validator._validate_email("@missingname.com"))
    
    def test_validate_date(self):
        """Test date validation"""
        self.assertTrue(self.validator._validate_date("2020-01-01"))
        self.assertTrue(self.validator._validate_date("1999-12-31"))
        self.assertFalse(self.validator._validate_date("01-01-2020"))  # Wrong format
        self.assertFalse(self.validator._validate_date("2020/01/01"))  # Wrong separator
        self.assertFalse(self.validator._validate_date("not-a-date"))  # Not a date

if __name__ == '__main__':
    unittest.main()