class AppConfig:
    DATA_PATHS = {
        'employees': 'data/input/employees.csv',
        'departments': 'data/input/departments.csv',
        'attendance': 'data/input/attendance.csv',
        'payroll': 'data/input/payroll.csv'
    }
    
    OUTPUT_PATHS = {
        'reports': 'data/output/reports/',
        'analytics': 'data/output/analytics/'
    }
    
    DATABASE_CONFIG = {
        'url': 'jdbc:postgresql://localhost:5432/ems',
        'user': 'admin',
        'password': 'password',
        'driver': 'org.postgresql.Driver'
    }