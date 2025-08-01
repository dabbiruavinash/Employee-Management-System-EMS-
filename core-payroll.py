from dataclasses import dataclass
from datetime import date

@dataclass
class Payroll:
    payroll_id: int
    employee_id: int
    pay_period_start: date
    pay_period_end: date
    basic_salary: float
    allowances: float
    deductions: float
    net_salary: float
    payment_date: date
    payment_status: str