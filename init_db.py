from tools import db


def init_db():
    from .models.api_reports import APIReport
    from .models.api_baseline import APIBaseline
    from .models.api_tests import PerformanceApiTest
    from .models.api_thresholds import APIThresholds
    db.Base.metadata.create_all(bind=db.engine)

