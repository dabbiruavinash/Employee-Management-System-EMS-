import re
from datetime import datetime
from core.employee import Employee
from core.department import Department

class ValidationUtils:
    def validate_employee(self, employee: Employee) -> bool:
        """Validate employee data"""
        if not employee.employee_id or employee.employee_id <= 0:
            return False
            
        if not employee.first_name or not employee.last_name:
            return False
            
        if not self._validate_email(employee.email):
            return False
            
        if not self._validate_date(employee.hire_date):
            return False
            
        if employee.salary <= 0:
            return False
            
        return True
    
    def validate_department(self, department: Department) -> bool:
        """Validate department data"""
        if not department.department_id or department.department_id <= 0:
            return False
            
        if not department.department_name:
            return False
            
        if department.budget <= 0:
            return False
            
        return True
    
    def _validate_email(self, email: str) -> bool:
        """Validate email format"""
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        return re.match(pattern, email) is not None
    
    def _validate_date(self, date_str: str) -> bool:
        """Validate date format (YYYY-MM-DD)"""
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False