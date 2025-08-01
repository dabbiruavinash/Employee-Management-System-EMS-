from config.spark_config import SparkConfigManager
from services.employee_service import EmployeeService
from services.department_service import DepartmentService
from services.payroll_service import PayrollService
from services.attendance_service import AttendanceService
from services.performance_service import PerformanceService
from services.analytics_service import AnalyticsService
from core.employee import Employee
from core.department import Department
from core.payroll import Payroll
from core.attendance import Attendance
from core.performance import PerformanceReview
from utils.logging_utils import LoggingUtils
from utils.date_utils import DateUtils
from datetime import date, datetime, time
import logging

def main():
    # Initialize logging
    LoggingUtils.setup_logging('ems_application.log')
    logging.info("Starting Employee Management System")
    
    try:
        # Initialize Spark
        spark_manager = SparkConfigManager()
        spark = spark_manager.get_spark_session()
        
        # Initialize services
        emp_service = EmployeeService(spark)
        dept_service = DepartmentService(spark)
        payroll_service = PayrollService(spark)
        attendance_service = AttendanceService(spark)
        performance_service = PerformanceService(spark)
        analytics_service = AnalyticsService(spark)
        
        # 1. Department Operations
        logging.info("Processing Department Operations")
        
        # Create new department
        new_dept = Department(
            department_id=50,
            department_name="Research & Development",
            location="Boston",
            budget=1500000.00
        )
        
        if dept_service.add_department(new_dept):
            logging.info("Added new department successfully")
        
        # Show department budget summary
        dept_budget = dept_service.get_department_budget_summary()
        print("\nDepartment Budget Summary:")
        dept_budget.show()
        
        # 2. Employee Operations
        logging.info("Processing Employee Operations")
        
        # Create new employee
        new_employee = Employee(
            employee_id=1050,
            first_name="Sarah",
            last_name="Johnson",
            email="sarah.johnson@company.com",
            phone_number="555-123-4567",
            hire_date=date.today().strftime('%Y-%m-%d'),
            job_title="Research Scientist",
            salary=95000.00,
            department_id=50,
            manager_id=1001
        )
        
        if emp_service.add_employee(new_employee):
            logging.info("Added new employee successfully")
        
        # Show employees in department 50
        rnd_employees = emp_service.get_employees_by_department(50)
        print("\nEmployees in R&D Department:")
        rnd_employees.show()
        
        # Show salary statistics
        salary_stats = emp_service.get_employee_salary_stats()
        print("\nSalary Statistics by Department:")
        salary_stats.show()
        
        # 3. Payroll Operations
        logging.info("Processing Payroll Operations")
        
        # Process payroll for current month
        current_month = datetime.now().strftime('%Y-%m')
        payroll_data = payroll_service.process_payroll(current_month)
        print(f"\nPayroll for {current_month}:")
        payroll_data.show(truncate=False)
        
        # Generate payroll report
        payroll_report = payroll_service.generate_payroll_report()
        print("\nPayroll Summary Report:")
        payroll_report.show()
        
        # 4. Attendance Operations
        logging.info("Processing Attendance Operations")
        
        # Get date range for last 30 days
        start_date, end_date = DateUtils.get_date_range(30)
        
        # Calculate attendance summary
        attendance_summary = attendance_service.calculate_attendance_summary(start_date, end_date)
        print("\nAttendance Summary (Last 30 Days):")
        attendance_summary.show()
        
        # 5. Performance Operations
        logging.info("Processing Performance Operations")
        
        # Add performance review
        performance_review = PerformanceReview(
            review_id=5001,
            employee_id=1050,
            review_date=date.today().strftime('%Y-%m-%d'),
            reviewer_id=1001,
            performance_score=4.5,
            strengths="Excellent research skills, strong teamwork",
            areas_for_improvement="Time management for administrative tasks",
            comments="Top performer in the team"
        )
        
        # Get performance reviews
        performance_reviews = performance_service.get_performance_reviews()
        print("\nPerformance Reviews:")
        performance_reviews.show(truncate=False)
        
        # Get average scores by department
        dept_performance = performance_service.get_average_scores_by_department()
        print("\nAverage Performance Scores by Department:")
        dept_performance.show()
        
        # 6. Analytics Operations
        logging.info("Processing Analytics Operations")
        
        # Analyze attendance trends
        attendance_trends = analytics_service.analyze_attendance_trends()
        print("\nMonthly Attendance Trends:")
        attendance_trends.show()
        
        # Cluster employees by salary
        salary_clusters = analytics_service.cluster_employees_by_salary(k=3)
        print("\nEmployee Salary Clusters:")
        salary_clusters.show()
        
        # 7. Generate Reports
        logging.info("Generating Reports")
        
        # Save reports to output directory
        salary_stats.write.mode('overwrite').csv(
            'data/output/salary_stats',
            header=True
        )
        
        attendance_summary.write.mode('overwrite').csv(
            'data/output/attendance_summary',
            header=True
        )
        
        logging.info("Reports generated successfully")
        
    except Exception as e:
        logging.error(f"Application error: {str(e)}", exc_info=True)
        print(f"An error occurred: {str(e)}")
    finally:
        spark.stop()
        logging.info("Application shutdown complete")

if __name__ == "__main__":
    main()