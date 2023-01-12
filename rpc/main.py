from typing import Optional, Union

from tools import rpc_tools
from ..models.api_reports import APIReport
from pylon.core.tools import web, log
from pydantic import ValidationError

from ..models.api_tests import PerformanceApiTest
from ..models.pd.performance_test import TestCommon, TestOverrideable
from ..models.pd.test_parameters import PerformanceTestParams, PerformanceTestParamsCreate, PerformanceTestParamsRun
from ..models.pd.quality_gate import QualityGate
from ..utils.utils import run_test


class RPC:
    @web.rpc('backend_results_or_404', 'results_or_404')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def backend_results_or_404(self, run_id):
        return APIReport.query.get_or_404(run_id)

    @web.rpc('backend_performance_test_create_common_parameters', 'parse_common_test_parameters')
    # @rpc_tools.wrap_exceptions(RuntimeError)
    # @rpc_tools.wrap_exceptions(ValidationError)
    def parse_common_test_parameters(self, project_id: int, test_params: dict, **kwargs) -> dict:
        overrideable_only = kwargs.pop('overrideable_only', False)
        if overrideable_only:
            pd_object = TestOverrideable(
                **test_params
            )
        else:
            pd_object = TestCommon(
                project_id=project_id,
                **test_params
            )
        return pd_object.dict(**kwargs)

    @web.rpc('backend_performance_test_create_test_parameters', 'parse_test_parameters')
    @rpc_tools.wrap_exceptions(ValidationError)
    def parse_test_parameters(self, data: Union[list, dict], **kwargs) -> dict:
        purpose = kwargs.pop('purpose', None)
        if purpose == 'run':
            pd_object = PerformanceTestParamsRun(test_parameters=data)
        elif purpose == 'control_tower':
            pd_object = PerformanceTestParamsCreate.from_control_tower(data)
        else:
            pd_object = PerformanceTestParamsCreate(test_parameters=data)
        return pd_object.dict(**kwargs)

    @web.rpc('backend_performance_run_scheduled_test', 'run_scheduled_test')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def run_scheduled_test(self, test_id: int, test_params: list) -> dict:
        test = PerformanceApiTest.query.filter(PerformanceApiTest.id == test_id).one()
        test_params_schedule_pd = PerformanceTestParams(test_parameters=test_params)
        test_params_existing_pd = PerformanceTestParams.from_orm(test)
        test_params_existing_pd.update(test_params_schedule_pd)
        test.__dict__.update(test_params_existing_pd.dict())
        return run_test(test)

    @web.rpc('backend_performance_job_type_by_uid')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def job_type_by_uid(self, project_id: int, test_uid: str) -> Optional[str]:
        test = PerformanceApiTest.query.filter(
                PerformanceApiTest.get_api_filter(project_id, test_uid)
        ).first()
        if test:
            return test.job_type

    @web.rpc(f'backend_performance_test_create_integration_validate_quality_gate')
    @rpc_tools.wrap_exceptions(ValidationError)
    def backend_performance_test_create_integration_validate(self, data: dict, pd_kwargs: Optional[dict] = None, **kwargs) -> dict:
        if not pd_kwargs:
            pd_kwargs = {}
        pd_object = QualityGate(**data)
        return pd_object.dict(**pd_kwargs)

    @web.rpc('backend_performance_execution_json_config_quality_gate')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def make_execution_json_config(self, integration_data: dict) -> dict:
        """ Prepare execution_json for this integration """
        # no extra data to add to execution json
        # but rpc needs to exist
        return integration_data
