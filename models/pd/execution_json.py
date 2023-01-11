from pydantic import BaseModel, validator, AnyUrl, parse_obj_as, root_validator, constr
from typing import Optional

from .test_parameters import PerformanceTestParams


class ExecutionParams(PerformanceTestParams):
    job_type: str
    entrypoint: str

    cmd: str = ''

    influxdb_database: str = "{{secret.gatling_db}}"
    influxdb_host: str = "{{secret.influx_ip}}"
    influxdb_user: str = "{{secret.influx_user}}"
    influxdb_password: str = "{{secret.influx_password}}"
    influxdb_comparison: str = "{{secret.comparison_db}}"
    influxdb_telegraf: str = "{{secret.telegraf_db}}"
    loki_host: str = "{{secret.loki_host}}"
    loki_port: str = "3100"

    additional_files: dict = {}

    test: Optional[str]
    GATLING_TEST_PARAMS: Optional[str]

    env_vars: dict = {}

    class Config:
        fields = {
            'additional_files': 'customization'
        }

    @validator('entrypoint', always=True)
    def validate_entrypoint(cls, value: str, values: dict):
        from os import path
        if values['job_type'] == 'perfmeter' and not path.exists(value):
            return path.join('/mnt/jmeter', value)
        return value

    @validator('cmd', always=True)
    def validate_cmd(cls, value: str, values: dict):
        if values['job_type'] == 'perfmeter':
            value = f"-n -t {values['entrypoint']}"
            for i in values['test_parameters']:
                value += f" -J{i.name}={i.default}"
        return value

    @validator('env_vars', always=True)
    def validate_env_vars(cls, value: dict, values: dict):
        for k in list(value.keys()):
            if k in cls.__fields__:
                values[k] = value[k]
                del value[k]
        return value

    @validator('test', always=True)
    def validate_test(cls, value, values):
        if values['job_type'] == "perfgun":
            value = values['entrypoint']
        return value

    @validator('GATLING_TEST_PARAMS', always=True)
    def validate_GATLING_TEST_PARAMS(cls, value, values):
        if values['job_type'] == "perfgun":
            value = ""
            for i in values['test_parameters']:
                value += f"-D{i.name}={i.default} "
        return value

    def dict(self, *args, **kwargs) -> dict:
        kwargs['exclude'] = kwargs.get('exclude', set())
        kwargs['exclude'].update({'job_type', 'entrypoint', 'env_vars', 'test_parameters'})
        temp = super().dict(*args, **kwargs)
        temp.update(self.env_vars)
        return temp

    @classmethod
    def from_orm(cls, db_object: 'Test'):
        return cls(**dict(
            job_type=db_object.job_type,
            entrypoint=db_object.entrypoint,
            test_parameters=db_object.all_test_parameters.dict()['test_parameters'],
            customization=db_object.customization,
            env_vars=db_object.env_vars,
            influxdb_database=db_object.influx_db
        ))


class CcEnvVars(BaseModel):
    RABBIT_HOST: str = "{{secret.rabbit_host}}"
    RABBIT_USER: str = "{{secret.rabbit_user}}"
    RABBIT_PASSWORD: str = "{{secret.rabbit_password}}"
    GALLOPER_WEB_HOOK: str = "{{secret.post_processor}}"
    RABBIT_VHOST: str = "carrier"
    cc_env_vars: dict = {}

    @validator('cc_env_vars', always=True)
    def validate_env_vars(cls, value: dict, values: dict):
        for k in list(value.keys()):
            if k in cls.__fields__:
                values[k] = value[k]
                del value[k]
        return value

    def dict(self, *args, **kwargs) -> dict:
        kwargs['exclude'] = kwargs.get('exclude', set())
        kwargs['exclude'].update({'cc_env_vars'})
        temp = super().dict(*args, **kwargs)
        temp.update(self.cc_env_vars)
        return temp

    @classmethod
    def from_orm(cls, db_object: 'Test'):
        public_queues = db_object.rpc.call.get_rabbit_queues("carrier")
        if db_object.location not in public_queues:
            return cls(
                cc_env_vars=db_object.cc_env_vars,
                RABBIT_USER="{{secret.rabbit_project_user}}",
                RABBIT_PASSWORD="{{secret.rabbit_project_password}}",
                RABBIT_VHOST="{{secret.rabbit_project_vhost}}"
            )
        return cls(
            cc_env_vars=db_object.cc_env_vars
        )
