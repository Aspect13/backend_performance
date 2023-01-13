from datetime import datetime
from typing import List, Union, Optional

from pydantic import BaseModel, validator


class StatusField(BaseModel):
    status: str = 'Pending...'
    percentage: int = 0
    description: str = 'Check if there are enough workers to perform the test'


class ReportDefaultSerializer(BaseModel):
    id: int
    project_id: int
    test_uid: str
    name: str
    environment: str
    type: str
    failures: int
    total: int
    thresholds_missed: int
    throughput: float
    vusers: int
    pct50: float
    pct75: float
    pct90: float
    pct95: float
    pct99: float
    max: float
    min: float
    mean: float
    build_id: str
    lg_type: str
    onexx: int
    twoxx: int
    threexx: int
    fourxx: int
    fivexx: int
    tags: List[str] = []
    test_status: Optional[StatusField] = StatusField()
    test_config: dict

    start_time: datetime
    end_time: Optional[datetime]
    duration: float
    requests: List[str] = []
    engagement: Optional[str]

    class Config:
        orm_mode = True
        fields = {'max': '_max', 'min': '_min'}


class ReportGetSerializer(ReportDefaultSerializer):
    start_time: Union[datetime, str]
    end_time: Union[datetime, str, None]
    failure_rate: Optional[float] = 0

    @validator('failure_rate', always=True)
    def compute_failure_rate(cls, value: Optional[float], values: dict) -> float:
        if value:
            return value
        try:
            return round((values['failures'] / values['total']) * 100, 2)
        except ZeroDivisionError:
            return 0

    @validator('start_time', 'end_time')
    def format_dates(cls, value: Optional[datetime]):
        if not value:
            return value
        return value.isoformat()


class ReportCreateSerializer(ReportDefaultSerializer):
    id: Optional[int]
    failures: int = 0
    total: int = 0
    thresholds_missed: int = 0
    throughput: float = 0
    pct50: float = 0
    pct75: float = 0
    pct90: float = 0
    pct95: float = 0
    pct99: float = 0
    max: float = 0
    min: float = 0
    mean: float = 0
    onexx: int = 0
    twoxx: int = 0
    threexx: int = 0
    fourxx: int = 0
    fivexx: int = 0
    requests: list = []

    start_time: datetime
    end_time: Optional[datetime]

    duration: float

    @validator('start_time', 'end_time')
    def format_dates(cls, value: Union[datetime, str, None]):
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return value

    @validator('duration', always=True)
    def set_duration(cls, value: int, values: dict):
        if all((values['start_time'], values['end_time'])):
            return round((values['end_time'] - values['start_time']).total_seconds(), 1)
        return value

    @validator('throughput')
    def round_throughput(cls, value: int):
        return round(value, 1)