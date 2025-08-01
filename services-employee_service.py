from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, avg, count
from core.employee import Employee
from utils.data_utils import DataUtils
from utils.validation_utils import ValidationUtils
import logging

class EmployeeService:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_utils = DataUtils(spark)
        self.validator = ValidationUtils()
        
    def load_employees(self) -> DataFrame:
        """Load employee data from source"""
        return self.data_utils.load_csv(AppConfig.DATA_PATHS['employees'])
    
    def add_employee(self, employee: Employee) -> bool:
        """Add new employee to the system"""
        try:
            # Validate employee data
            if not self.validator.validate_employee(employee):
                raise ValueError("Invalid employee data")
                
            # Create DataFrame for new employee
            new_emp_df = self.spark.createDataFrame([{
                'employee_id': employee.employee_id,
                'first_name': employee.first_name,
                'last_name': employee.last_name,
                'email': employee.email,
                'phone_number': employee.phone_number,
                'hire_date': employee.hire_date,
                'job_title': employee.job_title,
                'salary': employee.salary,
                'department_id': employee.department_id,
                'manager_id': employee.manager_id
            }])
            
            # Append to existing data
            existing_df = self.load_employees()
            updated_df = existing_df.union(new_emp_df)
            
            # Save back to source
            self.data_utils.save_csv(updated_df, AppConfig.DATA_PATHS['employees'])
            return True
        except Exception as e:
            logging.error(f"Error adding employee: {str(e)}")
            return False
    
    def get_employees_by_department(self, dept_id: int) -> DataFrame:
        """Get all employees in a department"""
        emp_df = self.load_employees()
        return emp_df.filter(col('department_id') == dept_id)
    
    def get_employee_salary_stats(self) -> DataFrame:
        """Calculate salary statistics by department"""
        emp_df = self.load_employees()
        return emp_df.groupBy('department_id') \
            .agg(
                avg('salary').alias('avg_salary'),
                count('employee_id').alias('employee_count')
            )