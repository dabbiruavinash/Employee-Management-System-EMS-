from dataclasses import dataclass
from datetime import date, time

@dataclass
class Attendance:
    attendance_id: int
    employee_id: int
    date: date
    check_in: time
    check_out: time
    status: str  # Present, Absent, Late, Half-day, etc.
    remarks: str