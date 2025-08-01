import unittest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from utils.spark_utils import SparkUtils

class TestSparkUtils(unittest.TestCase):
    def test_get_employee_schema(self):
        """Test employee schema definition"""
        schema = SparkUtils.get_employee_schema()
        
        self.assertIsInstance(schema, StructType)
        self.assertEqual(len(schema.fields), 10)
        
        # Check some fields
        self.assertEqual(schema["employee_id"].dataType, IntegerType())
        self.assertEqual(schema["first_name"].dataType, StringType())
        self.assertEqual(schema["salary"].dataType, DoubleType())
        self.assertEqual(schema["hire_date"].dataType, DateType())
    
    def test_get_department_schema(self):
        """Test department schema definition"""
        schema = SparkUtils.get_department_schema()
        
        self.assertIsInstance(schema, StructType)
        self.assertEqual(len(schema.fields), 4)
        
        # Check some fields
        self.assertEqual(schema["department_id"].dataType, IntegerType())
        self.assertEqual(schema["budget"].dataType, DoubleType())
    
    def test_cast_columns(self):
        """Test column type casting"""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("TestSparkUtils").getOrCreate()
        
        data = [("1", "John", "75000.50")]
        df = spark.createDataFrame(data, ["id", "name", "salary"])
        
        # Cast columns
        cast_df = SparkUtils.cast_columns(df, {
            "id": IntegerType(),
            "salary": DoubleType()
        })
        
        # Verify types
        self.assertEqual(cast_df.schema["id"].dataType, IntegerType())
        self.assertEqual(cast_df.schema["salary"].dataType, DoubleType())
        
        spark.stop()

if __name__ == '__main__':
    unittest.main()