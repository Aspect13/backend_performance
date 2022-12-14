#     Copyright 2021 getcarrier.io
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

import json
from collections import defaultdict
from queue import Empty
from typing import List, Union, Optional

from pylon.core.tools import log
from sqlalchemy import Column, Integer, String, JSON, ARRAY, and_

from tools import db_tools, db, rpc_tools, secrets_tools
from tools import constants as c
from .pd.execution_json import ExecutionParams, CcEnvVars
from ..constants import JOB_CONTAINER_MAPPING
from .pd.test_parameters import PerformanceTestParams


class PerformanceApiTest(db_tools.AbstractBaseMixin, db.Base, rpc_tools.RpcMixin):
    __tablename__ = "performance_tests_api"
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, unique=False, nullable=False)
    test_uid = Column(String(128), unique=True, nullable=False)
    name = Column(String(128), nullable=False)

    parallel_runners = Column(Integer, nullable=False)
    location = Column(String(128), nullable=False)

    entrypoint = Column(String(128), nullable=False)
    runner = Column(String(128), nullable=False)

    # reporting = Column(ARRAY(JSON), nullable=False)  #- integrations?

    test_parameters = Column(ARRAY(JSON), nullable=True)

    integrations = Column(JSON, nullable=True)

    schedules = Column(ARRAY(Integer), nullable=True, default=[])

    env_vars = Column(JSON)
    customization = Column(JSON)
    cc_env_vars = Column(JSON)

    source = Column(JSON, nullable=False)

    @property
    def container(self):
        return JOB_CONTAINER_MAPPING.get(self.runner, {}).get('container')

    @property
    def job_type(self):
        return JOB_CONTAINER_MAPPING.get(self.runner, {}).get('job_type')

    @property
    def influx_db(self):
        return JOB_CONTAINER_MAPPING.get(self.runner, {}).get('influx_db')

    @property
    def default_params_mapping(self) -> dict:
        return {
            "influx.db": self.influx_db,
            "influx.port": "{{secret.influx_port}}",
            "influx.host": "{{secret.influx_ip}}",
            "influx.username": "{{secret.influx_user}}",
            "influx.password": "{{secret.influx_password}}",
            "galloper_url": "{{secret.galloper_url}}",
            "comparison_db": "{{secret.comparison_db}}",
            "telegraf_db": "{{secret.telegraf_db}}",
            "loki_host": "{{secret.loki_host}}",
            "loki_port": "{{secret.loki_port}}",
            "test_name": self.name
        }

    @property
    def default_test_parameters(self) -> PerformanceTestParams:
        return PerformanceTestParams(test_parameters=[
            {'name': k, 'default': v, 'description': 'default parameter'}
            for k, v in self.default_params_mapping.items()
        ])

    @property
    def all_test_parameters(self) -> PerformanceTestParams:
        tp = self.default_test_parameters
        tp.update(PerformanceTestParams.from_orm(self))
        return tp

    @property
    def docker_command(self):
        cmd_template = 'docker run -e project_id={project_id} -e galloper_url={galloper_url}' \
                       ' -e token={token} getcarrier/control_tower:{control_tower_version} --test_id={test_id}'
        return cmd_template.format(
            project_id=self.project_id,
            galloper_url=secrets_tools.unsecret("{{secret.galloper_url}}", project_id=self.project_id),
            token=secrets_tools.unsecret("{{secret.auth_token}}", project_id=self.project_id),
            control_tower_version=c.CURRENT_RELEASE,
            test_id=self.test_uid
        )

    def filtered_test_parameters_unsecret(self, test_parameters: Optional[dict] = None) -> list:
        if not test_parameters:
            test_parameters = self.test_parameters

        def filter_func(item):
            return item['default'] != secrets_tools.unsecret(
                self.default_params_mapping.get(item['name']),
                project_id=self.project_id
            )
        return list(filter(filter_func, test_parameters))

    def add_schedule(self, schedule_data: dict, commit_immediately: bool = True):
        schedule_data['test_id'] = self.id
        schedule_data['project_id'] = self.project_id
        try:
            schedule_id = self.rpc.timeout(2).scheduling_backend_performance_create_schedule(data=schedule_data)
            log.info(f'Created schedule_id {schedule_id}')
            updated_schedules = set(self.schedules)
            updated_schedules.add(schedule_id)
            self.schedules = list(updated_schedules)
            if commit_immediately:
                self.commit()
            log.info(f'All test.schedules {self.schedules}')
        except Empty:
            log.warning('No scheduling rpc found')

    def handle_change_schedules(self, schedules_data: List[dict]):
        new_schedules_ids = set(i['id'] for i in schedules_data if i['id'])
        ids_to_delete = set(self.schedules).difference(new_schedules_ids)
        self.schedules = []
        for s in schedules_data:
            log.info('Adding schedule %s', s)
            self.add_schedule(s, commit_immediately=False)
        try:
            self.rpc.timeout(2).scheduling_delete_schedules(ids_to_delete)
        except Empty:
            ...
        self.commit()

    @classmethod
    def get_api_filter(cls, project_id: int, test_id: Union[int, str]):
        if isinstance(test_id, int):
            return and_(
                cls.project_id == project_id,
                cls.id == test_id
            )
        return and_(
            cls.project_id == project_id,
            cls.test_uid == test_id
        )

    def configure_execution_json(self, execution: bool = False) -> dict:
        exec_params = ExecutionParams.from_orm(self).dict(exclude_none=True)

        mark_for_delete = defaultdict(list)
        for section, integration in self.integrations.items():
            for integration_name, integration_data in integration.items():
                try:
                    # we never commit, so this is fine
                    integration[integration_name] = self.rpc.call_function_with_timeout(
                        func=f'backend_performance_execution_json_config_{integration_name}',
                        timeout=3,
                        integration_data=integration_data,
                    )
                except Empty:
                    log.error(f'Cannot find execution json compiler for {integration_name}')
                    mark_for_delete[section].append(integration_name)
                except Exception as e:
                    log.error('Error making config for %s %s', integration_name, str(e))
                    mark_for_delete[section].append(integration_name)

        for section, integrations in mark_for_delete.items():
            for i in integrations:
                log.warning(f'Some error occurred while building params for {section}/{i}. '
                            f'Removing from execution json')
                self.integrations[section].pop(i)
        # remove empty sections
        for section in mark_for_delete.keys():
            if not self.integrations[section]:
                self.integrations.pop(section)
        location = self.location
        self.location = "__internal" if self.location.startswith(
            "kubernetes") else self.location

        execution_json = {
            'test_id': self.test_uid,
            "container": self.container,
            "execution_params": json.dumps(exec_params),
            "cc_env_vars": CcEnvVars.from_orm(self).dict(exclude_none=True),
            "job_name": self.name,
            "job_type": self.job_type,
            "concurrency": self.parallel_runners,
            "channel": self.location,
            **self.rpc.call.parse_source(self.source).execution_json,
            'integrations': self.integrations
        }
        self.location = location
        if execution:
            execution_json = secrets_tools.unsecret(execution_json, project_id=self.project_id)

        return execution_json

    def to_json(self, exclude_fields: tuple = (), keep_custom_test_parameters: bool = True) -> dict:
        test = super().to_json(exclude_fields=exclude_fields)
        if 'job_type' not in exclude_fields:
            test['job_type'] = self.job_type
        if test.get('test_parameters') and 'test_parameters' not in exclude_fields:
            if keep_custom_test_parameters:
                exclude_fields = set(exclude_fields) - set(
                    i.name for i in PerformanceTestParams.from_orm(self).test_parameters
                )
            test['test_parameters'] = self.all_test_parameters.exclude_params(
                exclude_fields
            ).dict()['test_parameters']
        return test

    def api_json(self):
        return self.to_json(
            exclude_fields=tuple(
                tp.name for tp in self.default_test_parameters.test_parameters
                if tp.name != 'test_name'  # leave test_name here
            ),
            keep_custom_test_parameters=True  # explicitly
        )
