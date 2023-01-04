from pylon.core.tools import log
from typing import Optional
from uuid import uuid4
from pydantic import BaseModel, validator, AnyUrl, parse_obj_as, root_validator, constr
import string

from ...constants import JOB_CONTAINER_MAPPING

from tools import rpc_tools


class TestOverrideable(BaseModel):
    parallel_runners: Optional[int]
    location: str = 'default'
    env_vars: dict = {}
    customization: dict = {}
    cc_env_vars: dict = {}

    @validator('customization')
    def validate_customization(cls, value: dict):
        for k, v in list(value.items()):
            if any((k, v)):
                assert all((k, v)), 'All fields must be filled'
            else:
                del value[k]
        return value


class TestCommon(TestOverrideable):
    """
    Model of test itself without test_params or other plugin module's data
    """
    project_id: int
    uid: Optional[str]
    name: str
    test_type: str
    env_type: str
    parallel_runners: int
    entrypoint: str
    runner: str
    source: dict

    @root_validator
    def set_uuid(cls, values):
        if not values.get('uid'):
            values['uid'] = str(uuid4())
        return values

    @root_validator(pre=True, allow_reuse=True)
    def empty_str_to_none(cls, values):
        removed = []
        for k in list(values.keys()):
            if values[k] == '':
                removed.append(k)
                del values[k]
        return values

    @validator('name', 'test_type', 'env_type')
    def check_allowed_chars(cls, value):
        try:
            int(value[0])
            assert False, 'Can not start with a number'
        except ValueError:
            ...

        valid_chars = f'{string.ascii_letters}{string.digits}_'
        assert all(c in valid_chars for c in value), 'Only letters, numbers and "_" allowed'
        return value

    @validator('runner')
    def validate_runner(cls, value):
        assert value in JOB_CONTAINER_MAPPING.keys(), \
            "Runner version is not supported. Available versions: {}".format(
                list(JOB_CONTAINER_MAPPING.keys())
            )
        return value

    @validator('source')
    def validate_sources(cls, value: dict, values):
        validated = rpc_tools.RpcMixin().rpc.call.parse_source(value)
        return {
            'name': value['name'],
            **validated.dict()
        }
