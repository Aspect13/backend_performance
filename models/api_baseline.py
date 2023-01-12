from sqlalchemy import Column, Integer, String, JSON, ARRAY

from tools import db_tools, db


class APIBaseline(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = "api_baseline"
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, unique=False, nullable=False)
    report_id = Column(Integer, unique=False, nullable=False)
    test = Column(String, unique=False, nullable=False)
    environment = Column(String, unique=False, nullable=False)
    summary = Column(ARRAY(JSON), unique=False, nullable=False)

    def insert(self):
        APIBaseline.query.filter(
            APIBaseline.project_id == self.project_id,
            APIBaseline.test == self.test,
            APIBaseline.environment == self.environment
        ).delete()
        super().insert()
