from datetime import datetime, timedelta

class DateUtils:
    @staticmethod
    def get_current_date() -> str:
        """Get current date in YYYY-MM-DD format"""
        return datetime.now().strftime('%Y-%m-%d')
    
    @staticmethod
    def get_date_range(days: int) -> tuple:
        """Get date range from today to N days ago"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return (
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
    
    @staticmethod
    def is_weekend(date_str: str) -> bool:
        """Check if a date is weekend"""
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return date_obj.weekday() >= 5  # 5=Saturday, 6=Sunday