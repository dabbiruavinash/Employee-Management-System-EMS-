from pyspark.sql import DataFrame
from dataclasses import dataclass

@dataclass
class Department:
    """Department domain model class"""
    department_id: int
    department_name: str
    location: str
    budget: float
    
    @classmethod
    def from_dataframe(cls, df: DataFrame):
        """Create Department objects from Spark DataFrame"""
        departments = []
        for row in df.collect():
            departments.append(cls(
                department_id=row['department_id'],
                department_name=row['department_name'],
                location=row['location'],
                budget=row['budget']
            ))
        return departments