from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum
from core.department import Department
from utils.data_utils import DataUtils
import logging

class DepartmentService:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_utils = DataUtils(spark)
        
    def load_departments(self) -> DataFrame:
        """Load department data from source"""
        return self.data_utils.load_csv(AppConfig.DATA_PATHS['departments'])
    
    def add_department(self, department: Department) -> bool:
        """Add new department to the system"""
        try:
            new_dept_df = self.spark.createDataFrame([{
                'department_id': department.department_id,
                'department_name': department.department_name,
                'location': department.location,
                'budget': department.budget
            }])
            
            existing_df = self.load_departments()
            updated_df = existing_df.union(new_dept_df)
            
            self.data_utils.save_csv(updated_df, AppConfig.DATA_PATHS['departments'])
            return True
        except Exception as e:
            logging.error(f"Error adding department: {str(e)}")
            return False
    
    def get_department_budget_summary(self) -> DataFrame:
        """Calculate total budget by location"""
        dept_df = self.load_departments()
        return dept_df.groupBy('location') \
            .agg(spark_sum('budget').alias('total_budget'))