import json
from queue import Empty
from typing import Union, Tuple

import docker
import re
from datetime import datetime
from uuid import uuid4
from pydantic import ValidationError
from pylon.core.tools import log

from ..constants import JOB_CONTAINER_MAPPING, JOB_TYPE_MAPPING

from tools import task_tools, rpc_tools


def compile_tests(project_id, file_name, runner):
    from flask import current_app
    client = docker.from_env()
    container_name = JOB_CONTAINER_MAPPING.get(runner)["container"]
    secrets = current_app.config["CONTEXT"].rpc_manager.call.get_secrets(project_id=project_id)
    env_vars = {"artifact": file_name, "bucket": "tests",
                "galloper_url": secrets["galloper_url"],
                "token": secrets["auth_token"], "project_id": project_id, "compile": "true"}
    client.containers.run(container_name, stderr=True, remove=True, environment=env_vars,
                          tty=True, user='0:0')


def get_backend_test_data(event):
    users_count = 0
    duration = 0
    vusers_var_names = ["vusers", "users", "users_count", "ramp_users", "user_count"]
    lg_type = JOB_TYPE_MAPPING.get(event["job_type"], "other")
    tests_count = 1
    if lg_type == 'jmeter':
        for i in range(tests_count):
            exec_params = json.loads(event["execution_params"])["cmd"] + " "
            test_type = re.findall('-Jtest_type=(.+?) ', exec_params)
            test_type = test_type[0] if len(test_type) else 'demo'
            environment = re.findall("-Jenv_type=(.+?) ", exec_params)
            environment = environment[0] if len(environment) else 'demo'
            test_name = re.findall("-Jtest_name=(.+?) ", exec_params)
            test_name = test_name[0] if len(test_name) else 'test'
            duration = re.findall("-JDURATION=(.+?) ", exec_params)
            duration = float(duration[0]) if len(duration) else 0
            for each in vusers_var_names:
                if f'-j{each}' in exec_params.lower():
                    pattern = f'-j{each}=(.+?) '
                    vusers = re.findall(pattern, exec_params.lower())
                    users_count += int(vusers[0]) * event["concurrency"]
                    break
    elif lg_type == 'gatling':
        for i in range(tests_count):
            exec_params = json.loads(event["execution_params"])
            test_type = exec_params['test_type'] if exec_params.get('test_type') else 'demo'
            # test_name = exec_params['test'].split(".")[1].lower() if exec_params.get(
            #     'test') else 'test'
            test_name = event['job_name']
            environment = exec_params['env'] if exec_params.get('env') else 'demo'
            if exec_params.get('GATLING_TEST_PARAMS'):
                if '-dduration' in exec_params['GATLING_TEST_PARAMS'].lower():
                    duration = re.findall("-dduration=(.+?) ",
                                          exec_params['GATLING_TEST_PARAMS'].lower())[0]
                for each in vusers_var_names:
                    if f'-d{each}' in exec_params['GATLING_TEST_PARAMS'].lower():
                        pattern = f'-d{each}=(.+?) '
                        vusers = re.findall(pattern,
                                            exec_params['GATLING_TEST_PARAMS'].lower())
                        users_count += int(vusers[0]) * event["concurrency"]
                        break
    else:
        return {}
    start_time = datetime.utcnow().isoformat("T") + "Z"

    data = {'build_id': f'build_{uuid4()}', 'test_name': test_name, 'lg_type': lg_type,
            'type': test_type,
            'duration': duration, 'vusers': users_count, 'environment': environment,
            'start_time': start_time,
            'missed': 0}
    return data


def _calculate_limit(limit, total):
    return len(total) if limit == 'All' else limit


def run_test(test: 'PerformanceApiTest', config_only: bool = False, execution: bool = False, engagement_id: str = None
) -> dict:
    event = test.configure_execution_json(
        execution=execution
    )

    if config_only:
        return event

    test_data = get_backend_test_data(event)
    from ..models.api_reports import APIReport
    report = APIReport(
        name=test_data["test_name"],
        project_id=test.project_id,
        environment=test_data["environment"],
        type=test_data["type"],
        end_time="",
        start_time=test_data["start_time"],
        failures=0,
        total=0,
        thresholds_missed=0,
        throughput=0,
        vusers=test_data["vusers"],
        pct50=0, pct75=0, pct90=0, pct95=0, pct99=0,
        _max=0, _min=0, mean=0,
        duration=test_data["duration"],
        build_id=test_data["build_id"],
        lg_type=test_data["lg_type"],
        onexx=0, twoxx=0, threexx=0, fourxx=0, fivexx=0,
        requests="",
        test_uid=test.test_uid,
        test_config=test.api_json(),
        engagement=engagement_id
    )
    report.insert()
    event["cc_env_vars"]["REPORT_ID"] = str(report.id)
    event["cc_env_vars"]["build_id"] = test_data["build_id"]

    resp = task_tools.run_task(test.project_id, [event])
    resp['redirect'] = f'/task/{resp["task_id"]}/results'  # todo: where this should lead to?

    test.rpc.call.increment_statistics(test.project_id, 'performance_test_runs')

    resp['result_id'] = report.id  # for test rerun
    return resp


class ValidationErrorPD(Exception):
    def __init__(self, loc: Union[str, list], msg: str):
        self.loc = [loc] if isinstance(loc, str) else loc
        self.msg = msg
        super().__init__({'loc': self.loc, 'msg': msg})

    def json(self):
        return json.dumps(self.dict())

    def dict(self):
        return {'loc': self.loc, 'msg': self.msg}


def parse_test_data(project_id: int, request_data: dict,
        *,
        rpc=None, common_kwargs: dict = None,
        test_create_rpc_kwargs: dict = None,
        raise_immediately: bool = False,
        skip_validation_if_undefined: bool = True,
) -> Tuple[dict, list]:
    """
    Parses data while creating test

    :param project_id: Project id
    :param request_data: data from request json to validate
    :param rpc: instance of rpc_manager or None(will be initialized)
    :param common_kwargs: kwargs for common_test_parameters
            (test parameters apart from test_params table. E.g. name, description)
    :param test_create_rpc_kwargs: for each test_data key a rpc is called - these kwargs will be passed to rpc call
    :param raise_immediately: weather to raise validation error on first encounter or raise after collecting all errors
    :param skip_validation_if_undefined: if no rpc to validate test_data key is found
            data will remain untouched if True or erased if False
    :return:
    """
    if not rpc:
        rpc = rpc_tools.RpcMixin().rpc

    common_kwargs = common_kwargs or dict()
    test_create_rpc_kwargs = test_create_rpc_kwargs or dict()
    errors = list()

    common_params = request_data.pop('common_params', {})
    cloud_settings = common_params.get('env_vars', {}).get('cloud_settings')

    if cloud_settings:
        integration_name = cloud_settings.get("integration_name")

        cloud_settings["cpu_cores_limit"] = common_params['env_vars']["cpu_quota"]
        cloud_settings["memory_limit"] = common_params['env_vars']["memory_quota"]
        cloud_settings["concurrency"] = common_params['parallel_runners']

        request_data["integrations"]["clouds"] = {}
        request_data["integrations"]["clouds"][integration_name] = cloud_settings

    try:
        test_data = rpc.call.backend_performance_test_create_common_parameters(
            project_id=project_id,
            test_params=common_params,
            **common_kwargs
        )
    except ValidationError as e:
        test_data = dict()
        errors.extend(e.errors())
        if raise_immediately:
            return test_data, errors

    for k, v in request_data.items():
        try:
            test_data.update(rpc.call_function_with_timeout(
                func=f'backend_performance_test_create_{k}',
                timeout=2,
                data=v,
                **test_create_rpc_kwargs
            ))
        except Empty:
            log.warning(f'Cannot find parser for {k}')
            if skip_validation_if_undefined:
                test_data.update({k: v})
        except ValidationError as e:
            for i in e.errors():
                i['loc'] = (k, *i['loc'])
            errors.extend(e.errors())

            if raise_immediately:
                return test_data, errors
        except Exception as e:
            from traceback import format_exc
            log.warning(
                f'Exception as e {type(e)} in backend_performance_test_create_{k}\n'
                f'{format_exc()}')
            e.loc = [k, *getattr(e, 'loc', [])]
            errors.append(ValidationErrorPD(e.loc, str(e)).dict())
            if raise_immediately:
                return test_data, errors

    return test_data, errors
