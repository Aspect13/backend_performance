from traceback import format_exc
from typing import List

from pydantic import parse_obj_as
from sqlalchemy import and_
from json import loads

from flask_restful import Resource
from datetime import datetime
from io import BytesIO
from urllib.parse import urlunparse, urlparse
import requests
from pylon.core.tools import log
from flask import current_app, request

from ...models.pd.report import ReportCreateSerializer, ReportGetSerializer
from ...models.pd.test_parameters import PerformanceTestParamsRun
from ...models.baselines import Baseline
from ...models.reports import Report
from ...models.tests import Test
from ...connectors.influx import get_test_details, delete_test_data
from tools import MinioClient, api_tools
from influxdb.exceptions import InfluxDBClientError


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        args = request.args
        if args.get("report_id"):
            report = Report.query.filter_by(project_id=project_id, id=args.get("report_id")).first()
            return ReportGetSerializer.from_orm(report).dict(), 200
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        total, res = api_tools.get(project.id, args, Report)
        reports = parse_obj_as(List[ReportGetSerializer], res)
        return {"total": total, "rows": [i.dict() for i in reports]}, 200

    def post(self, project_id: int):
        '''
            create report from control tower?
        '''
        args = request.json
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)

        # TODO: we need to check api performance tests quota here
        # if not ProjectQuota.check_quota(project_id=project_id, quota='performance_test_runs'):
        #     return {"Forbidden": "The number of performance test runs allowed in the project has been exceeded"}

        # test_config = None
        if 'test_params' in args:
            try:
                test = Test.query.filter(
                    Test.uid == args.get('test_id')
                ).first()
                # test._session.expunge(test) # maybe we'll need to detach object from orm
                test.__dict__['test_parameters'] = test.filtered_test_parameters_unsecret(
                    PerformanceTestParamsRun.from_control_tower_cmd(
                        args['test_params']
                    ).dict()['test_parameters']
                )
            except Exception as e:
                log.error('Error parsing params from control tower %s', format_exc())
                return f'Error parsing params from control tower: {e}', 400

        report_model = ReportCreateSerializer(**args, project_id=project.id)
        # report = Report(
        #     name=args["test_name"],
        #     project_id=project.id,
        #     environment=args["environment"],
        #     type=args["type"],
        #     end_time="",
        #     start_time=args["start_time"],
        #     failures=0,
        #     total=0,
        #     thresholds_missed=0,
        #     throughput=0,
        #     vusers=args["vusers"],
        #     pct50=0,
        #     pct75=0,
        #     pct90=0,
        #     pct95=0,
        #     pct99=0,
        #     _max=0,
        #     _min=0,
        #     mean=0,
        #     duration=args["duration"],
        #     build_id=args["build_id"],
        #     lg_type=args["lg_type"],
        #     onexx=0,
        #     twoxx=0,
        #     threexx=0,
        #     fourxx=0,
        #     fivexx=0,
        #     requests="",
        #     test_uid=args.get("test_id")
        # )
        # if test_config:
        #     report.test_config = test_config
        report = Report(**report_model.dict())
        report.insert()
        # statistic = Statistic.query.filter_by(project_id=project_id).first()
        # setattr(statistic, 'performance_test_runs', Statistic.performance_test_runs + 1)
        # statistic.commit()
        self.module.context.rpc_manager.call.increment_statistics(project_id, 'performance_test_runs')
        return report.to_json(), 200

    def put(self, project_id: int):
        args = request.json
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)

        report = Report.query.filter(
            Report.project_id == project.id,
            Report.build_id == args["build_id"]
        ).first()

        report_model = get_test_details(ReportCreateSerializer.from_orm(report))

        updated_model = report_model.copy(update=loads(args["response_times"]))
        if args.get('missed'):
            updated_model.thresholds_missed = args['missed']

        for k, v in updated_model.dict(exclude_unset=True, exclude_defaults=True, by_alias=True).items():
            setattr(report, k, v)
        report.commit()
        if report.test_status['status'].lower() in ['finished', 'error', 'failed', 'success']:
            write_test_run_logs_to_minio_bucket(report, project)
        return {"message": "updated"}, 201

    def delete(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            delete_ids = list(map(int, request.args["id[]"].split(',')))
        except TypeError:
            return 'IDs must be integers', 400
        # query only needed fields
        query_result = Report.query.with_entities(
            Report.build_id, Report.name, Report.lg_type
        ).filter(
            and_(Report.project_id == project.id, Report.id.in_(delete_ids))
        ).all()
        for build_id, name, lg_type in query_result:
            log.info('DELETE query (%s)', (build_id, name, lg_type))
            try:
                delete_test_data(build_id, name, lg_type)
            except InfluxDBClientError as e:
                log.warning('InfluxDBClientError %s', e)

        # bulk delete baselines
        Baseline.query.filter(
            Baseline.project_id == project.id,
            Baseline.report_id.in_(delete_ids)
        ).delete()

        # bulk delete reports
        Report.query.filter(
            Report.project_id == project.id,
            Report.id.in_(delete_ids)
        ).delete()
        return {"message": "deleted"}, 204


def write_test_run_logs_to_minio_bucket(test: Report, project):
    loki_settings_url = urlparse(current_app.config["CONTEXT"].settings.get('loki', {}).get('url'))
    if loki_settings_url:
        #
        build_id = test.build_id
        report_id = test.id
        project_id = test.project_id
        test_name = test.name
        #
        logs_query = "{" + f'build_id="{build_id}",report_id="{report_id}",project="{project_id}"' + "}"
        #
        loki_url = urlunparse((
            loki_settings_url.scheme,
            loki_settings_url.netloc,
            '/loki/api/v1/query_range',
            None,
            'query=' + logs_query,
            None
        ))
        response = requests.get(loki_url)

        if response.ok:
            results = response.json()
            log.info(results)
            enc = 'utf-8'
            file_output = BytesIO()

            file_output.write(f'Test {test_name} (id={report_id}) run log:\n'.encode(enc))

            unpacked_values = []
            for i in results['data']['result']:
                for ii in i['values']:
                    unpacked_values.append(ii)
            for unix_ns, log_line in sorted(unpacked_values, key=lambda x: int(x[0])):
                timestamp = datetime.fromtimestamp(int(unix_ns) / 1e9).strftime("%Y-%m-%d %H:%M:%S")
                file_output.write(
                    f'{timestamp}\t{log_line}\n'.encode(enc)
                )
            minio_client = MinioClient(project)
            file_output.seek(0)
            bucket_name = str(test_name).replace("_", "").replace(" ", "").lower()
            file_name = f"{build_id}.log"
            if bucket_name not in minio_client.list_bucket():
                minio_client.create_bucket(bucket=bucket_name, bucket_type='autogenerated')
            minio_client.upload_file(bucket_name, file_output, file_name)
        else:
            log.warning('Request to loki failed with status %s', response.status_code)
