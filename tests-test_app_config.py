import unittest
from config.app_config import AppConfig

class TestAppConfig(unittest.TestCase):
    def test_data_paths(self):
        """Test data paths configuration"""
        self.assertIn('employees', AppConfig.DATA_PATHS)
        self.assertIn('departments', AppConfig.DATA_PATHS)
        self.assertTrue(AppConfig.DATA_PATHS['employees'].endswith('.csv'))
    
    def test_database_config(self):
        """Test database configuration"""
        self.assertIn('url', AppConfig.DATABASE_CONFIG)
        self.assertIn('user', AppConfig.DATABASE_CONFIG)
        self.assertIn('password', AppConfig.DATABASE_CONFIG)
        self.assertIn('driver', AppConfig.DATABASE_CONFIG)
        self.assertTrue(AppConfig.DATABASE_CONFIG['url'].startswith('jdbc:'))