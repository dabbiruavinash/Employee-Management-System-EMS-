from dataclasses import dataclass
from datetime import date

@dataclass
class PerformanceReview:
    review_id: int
    employee_id: int
    review_date: date
    reviewer_id: int
    performance_score: float
    strengths: str
    areas_for_improvement: str
    comments: str