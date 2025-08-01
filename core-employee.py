from pyspark.sql import DataFrame
from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class Employee:
    """Employee domain model class"""
    employee_id: int
    first_name: str
    last_name: str
    email: str
    phone_number: str
    hire_date: date
    job_title: str
    salary: float
    department_id: int
    manager_id: Optional[int] = None
    
    @classmethod
    def from_dataframe(cls, df: DataFrame):
        """Create Employee objects from Spark DataFrame"""
        employees = []
        for row in df.collect():
            employees.append(cls(
                employee_id=row['employee_id'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                email=row['email'],
                phone_number=row['phone_number'],
                hire_date=row['hire_date'],
                job_title=row['job_title'],
                salary=row['salary'],
                department_id=row['department_id'],
                manager_id=row.get('manager_id')
            ))
        return employees