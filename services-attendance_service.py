from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when
from utils.data_utils import DataUtils

class AttendanceService:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_utils = DataUtils(spark)
        
    def calculate_attendance_summary(self, start_date: str, end_date: str) -> DataFrame:
        """Calculate attendance summary for date range"""
        attendance_df = self.data_utils.load_csv(AppConfig.DATA_PATHS['attendance'])
        
        filtered = attendance_df.filter(
            (col('date') >= start_date) & (col('date') <= end_date)
        )
        
        return filtered.groupBy('employee_id') \
            .agg(
                count(when(col('status') == 'Present', True)).alias('present_days'),
                count(when(col('status') == 'Absent', True)).alias('absent_days'),
                count(when(col('status') == 'Late', True)).alias('late_days')
            )