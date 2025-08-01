import unittest
import os
import logging
from utils.logging_utils import LoggingUtils

class TestLoggingUtils(unittest.TestCase):
    def setUp(self):
        self.log_file = "test_log.log"
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
    
    def test_setup_logging(self):
        """Test logging configuration"""
        LoggingUtils.setup_logging(self.log_file)
        
        # Verify log file was created
        self.assertTrue(os.path.exists(self.log_file))
        
        # Test logging works
        logging.info("Test log message")
        
        # Verify message was written to file
        with open(self.log_file, 'r') as f:
            content = f.read()
            self.assertIn("Test log message", content)
    
    def test_log_operation_decorator(self):
        """Test operation logging decorator"""
        @LoggingUtils.log_operation("test operation")
        def test_function():
            return "success"
        
        # Call the decorated function
        result = test_function()
        self.assertEqual(result, "success")
        
        # Verify the log was written
        with open(self.log_file, 'r') as f:
            content = f.read()
            self.assertIn("Starting test operation", content)
            self.assertIn("Completed test operation", content)
    
    def tearDown(self):
        if os.path.exists(self.log_file):
            os.remove(self.log_file)

if __name__ == '__main__':
    unittest.main()