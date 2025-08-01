from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, month, count, avg
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from utils.data_utils import DataUtils

class AnalyticsService:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_utils = DataUtils(spark)
        
    def analyze_attendance_trends(self) -> DataFrame:
        """Analyze monthly attendance trends"""
        attendance_df = self.data_utils.load_csv(AppConfig.DATA_PATHS['attendance'])
        
        return attendance_df.withColumn('year', year('date')) \
            .withColumn('month', month('date')) \
            .groupBy('year', 'month') \
            .agg(count('*').alias('attendance_count')) \
            .orderBy('year', 'month')
    
    def cluster_employees_by_salary(self, k=3) -> DataFrame:
        """Cluster employees by salary using K-means"""
        emp_df = self.data_utils.load_csv(AppConfig.DATA_PATHS['employees'])
        
        # Prepare data for clustering
        assembler = VectorAssembler(
            inputCols=['salary'],
            outputCol='features'
        )
        emp_features = assembler.transform(emp_df)
        
        # Train K-means model
        kmeans = KMeans().setK(k).setSeed(1)
        model = kmeans.fit(emp_features)
        
        # Add cluster predictions
        clustered = model.transform(emp_features)
        return clustered.select('employee_id', 'salary', 'prediction')