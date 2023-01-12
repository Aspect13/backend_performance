from typing import Optional
from sqlalchemy import JSON, cast, Integer, String, literal_column, desc, asc, func
from collections import OrderedDict

from pylon.core.tools import web, log
from ..models.api_baseline import APIBaseline
from ..models.api_reports import APIReport

from tools import rpc_tools

columns = OrderedDict((
    ('id', APIReport.id),
    ('group', literal_column("'backend_performance'").label('group')),
    ('name', APIReport.name),
    ('start_time', APIReport.start_time),
    ('test_type', APIReport.type),
    ('test_env', APIReport.environment),
    ('aggregation_min', APIReport._min),
    ('aggregation_max', APIReport._max),
    ('aggregation_mean', APIReport.mean),
    ('aggregation_pct50', APIReport.pct50),
    ('aggregation_pct75', APIReport.pct75),
    ('aggregation_pct90', APIReport.pct90),
    ('aggregation_pct95', APIReport.pct95),
    ('aggregation_pct99', APIReport.pct99),
    ('throughput', APIReport.throughput),
    ('status', APIReport.test_status['status']),
    ('duration', APIReport.duration),
    ('total', APIReport.total),
    ('failures', APIReport.failures),
    ('tags', APIReport.tags)
))


class RPC:
    @web.rpc('performance_analysis_test_runs_backend_performance')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def backend_performance_tr(self, project_id: int,
                               start_time, end_time=None) -> tuple:
        log.info('backend_performance rpc | %s | %s', project_id, [start_time, end_time, ])

        query = APIReport.query.with_entities(
            *columns.values()
        ).filter(
            APIReport.project_id == project_id,
            APIReport.start_time >= start_time,
            func.lower(APIReport.test_status['status'].cast(String)).in_(('"finished"', '"failed"', '"success"'))
        ).order_by(
            asc(APIReport.start_time)
        )

        if end_time:
            query.filter(APIReport.end_time <= end_time)

        return tuple(zip(columns.keys(), i) for i in query.all())

    @web.rpc('backend_performance_get_baseline_report_id')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_baseline_report_id(self, project_id: int, test_name: str, test_env: str) -> Optional[int]:
        result = APIBaseline.query.with_entities(APIBaseline.report_id).filter(
            APIBaseline.project_id == project_id,
            APIBaseline.test == test_name,
            APIBaseline.environment == test_env,
        ).first()
        try:
            return result[0]
        except (TypeError, IndexError):
            return

    @web.rpc('backend_performance_get_results_by_ids')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def get_results_by_ids(self, project_id: int, report_ids: list):
        query = APIReport.query.with_entities(
            *columns.values()
        ).filter(
            APIReport.project_id == project_id,
            APIReport.id.in_(report_ids),
            # func.lower(APIReport.test_status['status'].cast(String)).in_(('"finished"', '"failed"', '"success"'))
        )

        return tuple(zip(columns.keys(), i) for i in query.all())

