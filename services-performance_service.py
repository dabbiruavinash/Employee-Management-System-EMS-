from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg
from utils.data_utils import DataUtils

class PerformanceService:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_utils = DataUtils(spark)
        
    def get_performance_reviews(self, employee_id: int = None) -> DataFrame:
        """Get performance reviews, optionally filtered by employee"""
        reviews_df = self.data_utils.load_csv('data/input/performance_reviews.csv')
        
        if employee_id:
            return reviews_df.filter(col('employee_id') == employee_id)
        return reviews_df
    
    def get_average_scores_by_department(self) -> DataFrame:
        """Calculate average performance scores by department"""
        reviews_df = self.get_performance_reviews()
        emp_df = self.data_utils.load_csv(AppConfig.DATA_PATHS['employees'])
        
        joined = reviews_df.join(emp_df, 'employee_id')
        return joined.groupBy('department_id') \
            .agg(avg('performance_score').alias('avg_performance_score'))