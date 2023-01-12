#   Copyright 2022 getcarrier.io
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from datetime import datetime, timedelta
from tools import constants as c
from tools import MinioClient, rpc_tools
from ..models.api_reports import APIReport
from pylon.core.tools import log


def get_project_id(build_id: str) -> int:
    resp = APIReport.query.with_entities(APIReport.project_id).filter(APIReport.build_id == build_id).first()
    return resp[0]


def get_client(project_id) -> MinioClient:
    rpc = rpc_tools.RpcMixin().rpc
    return MinioClient(rpc.call.project_get_or_404(project_id))


def calculate_auto_aggregation(build_id, test_name, lg_type, start_time, end_time):
    project_id = get_project_id(build_id)
    client = get_client(project_id)
    bucket_name = f'p--{project_id}.{test_name}'
    aggregation = "1s"
    aggr_list = ["1s", "5s", "30s", "1m", "5m", "10m"]
    for i in range(len(aggr_list)):
        aggr = aggr_list[i]
        file_name = f'{build_id}_{aggr}.csv.gz'
        response = client.select_object_content(bucket_name, file_name)
        if len(response) > c.MAX_DOTS_ON_CHART and aggregation != "10m":
            aggregation = aggr_list[i + 1]
        if len(response) == 0 and aggregation != "1s":
            aggregation = "30s"
            break
    return aggregation


def calculate_timestamps(start_time: str, end_time: str) -> list:
    t_format = "%Y-%m-%dT%H:%M:%S.000Z"
    start_time = datetime.strptime(start_time, t_format)
    end_time = datetime.strptime(end_time, t_format)
    timestamps = []
    while start_time <= end_time:
        timestamps.append(start_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        start_time += timedelta(seconds=1)
    return timestamps


def get_backend_users(build_id, test_name, aggregation):
    project_id = get_project_id(build_id)
    client = get_client(project_id)
    bucket_name = f'p--{project_id}.{test_name}'
    file_name = f'users_{build_id}_{aggregation}.csv.gz'
    response = client.select_object_content(bucket_name, file_name)
    timestamps = []
    results = {"users": {}}
    # aggregation of users
    _tmp = []
    if 'm' in aggregation:
        aggregation = f"{str(int(aggregation.replace('m', '')) * 60)}s"
    for line in response:
        _tmp.append(int(line['sum']) if line['sum'] != 'None' else 0)
        results["users"][line['time']] = None
        if line['time'] not in timestamps:
            timestamps.append(line['time'])
        if (len(_tmp) % int(aggregation.replace('s', ''))) == 0:
            results["users"][line['time']] = max(_tmp)
            _tmp = []
    return timestamps, results


def get_requests_summary_data(build_id, test_name, lg_type, start_time, end_time, aggregation, sampler,
                              timestamps=None, users=None, scope=None, aggr='pct95', status='all'):
    scope_addon = ""
    status_addon = ""
    group_by = False
    project_id = get_project_id(build_id)
    client = get_client(project_id)
    bucket_name = f'p--{project_id}.{test_name}'
    file_name = f'{build_id}_{aggregation}.csv.gz'
    aggr = aggr.lower()
    
    if scope and scope != 'All':
        scope_addon = f" where request_name='{scope}'"
    if scope != 'All':
        group_by = True
    if status != 'all':
        status_addon = f" and status='{status.upper()}'"
    
    expression_addon = scope_addon + status_addon
    
    if not (timestamps and users):
        timestamps, users = get_backend_users(build_id, test_name, aggregation)
    timestamps = calculate_timestamps(start_time, end_time)
    
    response = client.select_object_content(bucket_name, file_name, expression_addon)
    
    results = {}
    
    if group_by:
        for line in response:
            if not line.get('request_name'):
                continue
            if line['request_name'] not in results:
                results[line['request_name']] = {}
                for ts in timestamps:
                    results[line['request_name']][ts] = None
            results[line['request_name']][line['time']] = int(line[aggr])
    else:
        results['response'] = {}
        for ts in timestamps:
            results['response'][ts] = None
        for line in response:
            results['response'][line['time']] = int(line[aggr])
    
    return timestamps, results, users
