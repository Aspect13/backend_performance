from tools import db


def init_db():
    from .models.report import Report
    from .models.baseline import Baseline
    from .models.test import Test
    from .models.threshold import Threshold
    db.Base.metadata.create_all(bind=db.engine)

