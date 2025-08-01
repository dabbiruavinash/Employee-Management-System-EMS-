from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum
from core.payroll import Payroll
from utils.data_utils import DataUtils

class PayrollService:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_utils = DataUtils(spark)
        
    def process_payroll(self, payroll_period: str) -> DataFrame:
        """Process payroll for a given period"""
        payroll_df = self.data_utils.load_csv(AppConfig.DATA_PATHS['payroll'])
        return payroll_df.filter(col('pay_period') == payroll_period)
    
    def generate_payroll_report(self) -> DataFrame:
        """Generate payroll summary report"""
        payroll_df = self.data_utils.load_csv(AppConfig.DATA_PATHS['payroll'])
        return payroll_df.groupBy('pay_period') \
            .agg(
                spark_sum('basic_salary').alias('total_salary'),
                spark_sum('allowances').alias('total_allowances'),
                spark_sum('deductions').alias('total_deductions'),
                spark_sum('net_salary').alias('total_net_pay')
            )