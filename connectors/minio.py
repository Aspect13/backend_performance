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

from datetime import datetime, timezone
import boto3
from json import loads
from tools import constants as c
from ..models.api_reports import APIReport
from pylon.core.tools import log


def get_requests_summary_data(args):

    s3 = boto3.client('s3',
                      endpoint_url='http://127.0.0.1:9000',
                      aws_access_key_id='admin',
                      aws_secret_access_key='password',
                      region_name='us-east-1')

    # TODO use args for parametrization (Bucket, Key)
    r = s3.select_object_content(
        Bucket='p--1.basicecommerce',
        Key='build_06c68d6c-0bca-41e2-91f3-10960c89916e_1s.csv.gz',
        ExpressionType='SQL',
        Expression="select * from s3object s",
        InputSerialization={
            'CSV': {
                "FileHeaderInfo": "USE",
            },
            # 'JSON': {"Type": "Lines"},
            'CompressionType': 'GZIP',
        },
        OutputSerialization={'JSON': {}},
    )
    _res = []
    for event in r['Payload']:
        # print(event)
        if 'Records' in event:
            x = event['Records']['Payload'].decode('utf-8')
            # print(x)
            _ = []
            for each in x.split("\n"):
                try:
                    rec = loads(each)
                    # print(type(rec))
                    _.append(rec)
                except:
                    pass

            _res = _[0]["_1"]
    return _res
