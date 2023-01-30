#     Copyright 2020 getcarrier.io
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
from uuid import uuid4

from sqlalchemy import String, Column, Integer, Float, Text, ARRAY, JSON

from tools import db_tools, db


class Report(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = "backend_reports"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, unique=False, nullable=False)
    test_uid = Column(String(128), unique=False, nullable=False)
    uid = Column(String(128), unique=True, nullable=False)
    name = Column(String(128), unique=False)
    environment = Column(String(128), unique=False)
    type = Column(String(128), unique=False)
    end_time = Column(String(128), unique=False, nullable=True)
    start_time = Column(String(128), unique=False)
    failures = Column(Integer, unique=False)
    total = Column(Integer, unique=False)
    thresholds_missed = Column(Integer, unique=False, nullable=True)
    throughput = Column(Float, unique=False)
    vusers = Column(Integer, unique=False)
    pct50 = Column(Float, unique=False)
    pct75 = Column(Float, unique=False)
    pct90 = Column(Float, unique=False)
    pct95 = Column(Float, unique=False)
    pct99 = Column(Float, unique=False)
    _max = Column(Float, unique=False)
    _min = Column(Float, unique=False)
    mean = Column(Float, unique=False)
    duration = Column(Integer, unique=False)
    build_id = Column(String(128), unique=True)
    lg_type = Column(String(16), unique=False)
    onexx = Column(Integer, unique=False)
    twoxx = Column(Integer, unique=False)
    threexx = Column(Integer, unique=False)
    fourxx = Column(Integer, unique=False)
    fivexx = Column(Integer, unique=False)
    requests = Column(ARRAY(String), default=[])
    tags = Column(ARRAY(String), default=[])
    test_status = Column(
        JSON,
        default={
            "status": "Pending...",
            "percentage": 0,
            "description": "Check if there are enough workers to perform the test"
        }
    )
    test_config = Column(JSON, nullable=False, unique=False)
    # engagement id
    engagement = Column(String(64), nullable=True, default=None)


    @property
    def serialized(self):
        from .pd.report import ReportGetSerializer
        return ReportGetSerializer.from_orm(self)

    def to_json(self, exclude_fields: tuple = ()) -> dict:
        return self.serialized.dict(exclude=set(exclude_fields))

    def insert(self):
        if not self.uid:
            self.uid = str(uuid4())
        if not self.test_config:
            from .tests import Test
            self.test_config = Test.query.filter(
                Test.uid == self.test_uid
            ).first().api_json()
        super().insert()
