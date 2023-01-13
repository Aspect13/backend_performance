from datetime import datetime
from typing import Optional
from sqlalchemy import JSON, cast, Integer, String, literal_column, desc, asc, func
from collections import OrderedDict

from pylon.core.tools import web, log
from ..models.baseline import Baseline
from ..models.report import Report

from tools import rpc_tools

columns = OrderedDict((
    ('id', Report.id),
    ('group', literal_column("'backend_performance'").label('group')),
    ('name', Report.name),
    ('start_time', Report.start_time),
    ('test_type', Report.type),
    ('test_env', Report.environment),
    ('aggregation_min', Report._min),
    ('aggregation_max', Report._max),
    ('aggregation_mean', Report.mean),
    ('aggregation_pct50', Report.pct50),
    ('aggregation_pct75', Report.pct75),
    ('aggregation_pct90', Report.pct90),
    ('aggregation_pct95', Report.pct95),
    ('aggregation_pct99', Report.pct99),
    ('throughput', Report.throughput),
    ('status', Report.test_status['status']),
    ('duration', Report.duration),
    ('total', Report.total),
    ('failures', Report.failures),
    ('tags', Report.tags)
))


class RPC:
    @web.rpc('performance_analysis_test_runs_backend_performance')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def test_runs(self, project_id: int,
                               start_time, end_time=None) -> tuple:
        log.info('backend_performance rpc | %s | %s', project_id, [start_time, end_time, ])

        query = Report.query.with_entities(
            *columns.values()
        ).filter(
            Report.project_id == project_id,
            Report.start_time >= start_time,
            func.lower(Report.test_status['status'].cast(String)).in_(('"finished"', '"failed"', '"success"'))
        ).order_by(
            asc(Report.start_time)
        )

        if end_time:
            query.filter(Report.end_time <= end_time)

        return tuple(zip(columns.keys(), i) for i in query.all())

    @web.rpc('backend_performance_get_baseline_report_id')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_baseline_report_id(self, project_id: int, test_name: str, test_env: str) -> Optional[int]:
        result = Baseline.query.with_entities(Baseline.report_id).filter(
            Baseline.project_id == project_id,
            Baseline.test == test_name,
            Baseline.environment == test_env,
        ).first()
        try:
            return result[0]
        except (TypeError, IndexError):
            return

    @web.rpc('backend_performance_get_results_by_ids')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_results_by_ids(self, project_id: int, report_ids: list):
        query = Report.query.with_entities(
            *columns.values()
        ).filter(
            Report.project_id == project_id,
            Report.id.in_(report_ids),
            # func.lower(Report.test_status['status'].cast(String)).in_(('"finished"', '"failed"', '"success"'))
        )

        return tuple(zip(columns.keys(), i) for i in query.all())
