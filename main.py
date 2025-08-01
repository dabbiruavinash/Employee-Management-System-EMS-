from config.spark_config import SparkConfigManager
from services.employee_service import EmployeeService
from services.department_service import DepartmentService
from services.analytics_service import AnalyticsService
from core.employee import Employee
from core.department import Department
from datetime import date
import logging

def main():
    # Initialize Spark
    spark_manager = SparkConfigManager()
    spark = spark_manager.get_spark_session()
    
    try:
        # Initialize services
        emp_service = EmployeeService(spark)
        dept_service = DepartmentService(spark)
        analytics_service = AnalyticsService(spark)
        
        # Example usage
        # 1. Add a new employee
        new_employee = Employee(
            employee_id=1001,
            first_name="John",
            last_name="Doe",
            email="john.doe@company.com",
            phone_number="123-456-7890",
            hire_date=date.today().strftime('%Y-%m-%d'),
            job_title="Software Engineer",
            salary=85000.00,
            department_id=10,
            manager_id=100
        )
        
        if emp_service.add_employee(new_employee):
            print("Employee added successfully")
        
        # 2. Get employees by department
        engineering_employees = emp_service.get_employees_by_department(10)
        engineering_employees.show()
        
        # 3. Get salary statistics
        salary_stats = emp_service.get_employee_salary_stats()
        salary_stats.show()
        
        # 4. Analyze attendance trends
        attendance_trends = analytics_service.analyze_attendance_trends()
        attendance_trends.show()
        
        # 5. Cluster employees by salary
        salary_clusters = analytics_service.cluster_employees_by_salary()
        salary_clusters.show()
        
    except Exception as e:
        logging.error(f"Application error: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()