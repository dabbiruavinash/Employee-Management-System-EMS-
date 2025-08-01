import logging
from datetime import datetime

class LoggingUtils:
    @staticmethod
    def setup_logging(log_file='ems.log'):
        """Configure logging for the application"""
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
    @staticmethod
    def log_operation(operation: str):
        """Decorator to log function execution"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                start_time = datetime.now()
                logging.info(f"Starting {operation}")
                
                try:
                    result = func(*args, **kwargs)
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    logging.info(f"Completed {operation} in {duration:.2f} seconds")
                    return result
                except Exception as e:
                    logging.error(f"Error during {operation}: {str(e)}")
                    raise
                    
            return wrapper
        return decorator