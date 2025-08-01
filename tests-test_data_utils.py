import unittest
import tempfile
import shutil
import os
from pyspark.sql import SparkSession
from utils.data_utils import DataUtils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class TestDataUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestDataUtils").getOrCreate()
        
        # Create temp directory for test data
        cls.test_dir = tempfile.mkdtemp()
        cls.test_csv = os.path.join(cls.test_dir, "test.csv")
        
        # Create test CSV
        data = [
            "id,name,value",
            "1,Alice,100",
            "2,Bob,200",
            "3,Charlie,300"
        ]
        
        with open(cls.test_csv, 'w') as f:
            f.write('\n'.join(data))
    
    def setUp(self):
        self.data_utils = DataUtils(self.spark)
    
    def test_load_csv(self):
        """Test loading CSV file"""
        df = self.data_utils.load_csv(self.test_csv)
        
        self.assertEqual(df.count(), 3)
        self.assertEqual(len(df.columns), 3)
        
        # Verify some data
        names = [row['name'] for row in df.collect()]
        self.assertIn("Alice", names)
        self.assertIn("Bob", names)
    
    def test_load_csv_with_schema(self):
        """Test loading CSV with schema"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("value", IntegerType(), False)
        ])
        
        df = self.data_utils.load_csv(self.test_csv, schema)
        
        # Verify schema was applied
        self.assertEqual(df.schema["id"].dataType, IntegerType())
        self.assertEqual(df.schema["value"].dataType, IntegerType())
    
    def test_save_csv(self):
        """Test saving DataFrame to CSV"""
        # Create test DataFrame
        data = [("4", "David", "400"), ("5", "Eve", "500")]
        df = self.spark.createDataFrame(data, ["id", "name", "value"])
        
        # Save to new file
        output_path = os.path.join(self.test_dir, "output.csv")
        self.data_utils.save_csv(df, output_path)
        
        # Verify file was created
        self.assertTrue(os.path.exists(output_path))
        
        # Can add more verification by reading back the file
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls.test_dir)

if __name__ == '__main__':
    unittest.main()