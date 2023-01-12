from pydantic import BaseModel, conint
from pylon.core.tools import log


class QualityGate(BaseModel):
    error_rate: conint(ge=-1, le=100)
    degradation_rate: conint(ge=-1, le=100)
    missed_thresholds: conint(ge=-1, le=100)
