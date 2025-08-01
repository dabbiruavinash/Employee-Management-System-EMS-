import unittest
from datetime import datetime, timedelta
from utils.date_utils import DateUtils

class TestDateUtils(unittest.TestCase):
    def test_get_current_date(self):
        """Test current date formatting"""
        current_date = DateUtils.get_current_date()
        
        # Should be in YYYY-MM-DD format
        self.assertEqual(len(current_date), 10)
        self.assertEqual(current_date[4], '-')  # Year-month separator
        self.assertEqual(current_date[7], '-')  # Month-day separator
        
        # Should be a valid date
        datetime.strptime(current_date, '%Y-%m-%d')  # Will raise if invalid
    
    def test_get_date_range(self):
        """Test date range calculation"""
        start, end = DateUtils.get_date_range(7)
        
        # Verify format
        datetime.strptime(start, '%Y-%m-%d')
        datetime.strptime(end, '%Y-%m-%d')
        
        # Verify the range is 7 days
        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end_dt = datetime.strptime(end, '%Y-%m-%d')
        self.assertEqual((end_dt - start_dt).days, 7)
    
    def test_is_weekend(self):
        """Test weekend detection"""
        # Known weekend dates
        self.assertTrue(DateUtils.is_weekend("2023-04-01"))  # Saturday
        self.assertTrue(DateUtils.is_weekend("2023-04-02"))  # Sunday
        
        # Known weekdays
        self.assertFalse(DateUtils.is_weekend("2023-04-03"))  # Monday
        self.assertFalse(DateUtils.is_weekend("2023-04-07"))  # Friday

if __name__ == '__main__':
    unittest.main()