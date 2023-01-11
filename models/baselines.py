from sqlalchemy import Column, Integer, String, JSON, ARRAY

from tools import db_tools, db


class Baseline(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = "backend_baselines"
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, unique=False, nullable=False)
    report_id = Column(Integer, unique=False, nullable=False)
    test = Column(String, unique=False, nullable=False)
    environment = Column(String, unique=False, nullable=False)
    summary = Column(ARRAY(JSON), unique=False, nullable=False)

    def insert(self):
        Baseline.query.filter(
            Baseline.project_id == self.project_id,
            Baseline.test == self.test,
            Baseline.environment == self.environment
        ).delete()
        super().insert()
