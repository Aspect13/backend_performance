from tools import db


def init_db():
    from .models.reports import Report
    from .models.baselines import Baseline
    from .models.tests import Test
    from .models.thresholds import Threshold
    db.Base.metadata.create_all(bind=db.engine)

