from traceback import format_exc
from sqlalchemy import and_
from json import loads

from flask_restful import Resource
from datetime import datetime
from io import BytesIO
from urllib.parse import urlunparse, urlparse
import requests
from pylon.core.tools import log
from flask import current_app, request, make_response

from ...models.pd.test_parameters import PerformanceTestParamsRun
# from ....projects.models.statistics import Statistic
from ...models.api_baseline import APIBaseline
from ...models.api_reports import APIReport
from ...models.api_tests import PerformanceApiTest
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
            report = APIReport.query.filter_by(project_id=project_id, id=args.get("report_id")).first().to_json()
            return report
        reports = []
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        total, res = api_tools.get(project.id, args, APIReport)
        for each in res:
            each_json = each.to_json()
            each_json["start_time"] = each_json["start_time"].replace("T", " ").split(".")[0]
            each_json["duration"] = int(each_json["duration"] if each_json["duration"] else 0)
            try:
                each_json["failure_rate"] = round((each_json["failures"] / each_json["total"]) * 100, 2)
            except ZeroDivisionError:
                each_json["failure_rate"] = 0
            reports.append(each_json)
        return {"total": total, "rows": reports}

    def post(self, project_id: int):
        args = request.json
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)

        # TODO: we need to check api performance tests quota here
        # if not ProjectQuota.check_quota(project_id=project_id, quota='performance_test_runs'):
        #     return {"Forbidden": "The number of performance test runs allowed in the project has been exceeded"}

        test_config = None
        if 'test_params' in args:
            try:
                test = PerformanceApiTest.query.filter(
                    PerformanceApiTest.test_uid == args.get('test_id')
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

        report = APIReport(
            name=args["test_name"],
            project_id=project.id,
            environment=args["environment"],
            type=args["type"],
            end_time="",
            start_time=args["start_time"],
            failures=0,
            total=0,
            thresholds_missed=0,
            throughput=0,
            vusers=args["vusers"],
            pct50=0,
            pct75=0,
            pct90=0,
            pct95=0,
            pct99=0,
            _max=0,
            _min=0,
            mean=0,
            duration=args["duration"],
            build_id=args["build_id"],
            lg_type=args["lg_type"],
            onexx=0,
            twoxx=0,
            threexx=0,
            fourxx=0,
            fivexx=0,
            requests="",
            test_uid=args.get("test_id")
        )
        if test_config:
            report.test_config = test_config
        report.insert()
        # statistic = Statistic.query.filter_by(project_id=project_id).first()
        # setattr(statistic, 'performance_test_runs', Statistic.performance_test_runs + 1)
        # statistic.commit()
        self.module.context.rpc_manager.call.increment_statistics(project_id, 'performance_test_runs')
        return report.to_json()

    def put(self, project_id: int):
        args = request.json
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        test_data = get_test_details(project_id=project_id, build_id=args["build_id"], test_name=args["test_name"],
                                     lg_type=args["lg_type"])
        response_times = loads(args["response_times"])
        report = APIReport.query.filter(
            and_(APIReport.project_id == project.id, APIReport.build_id == args["build_id"])
        ).first()
        report.end_time = test_data["end_time"]
        report.start_time = test_data["start_time"]
        report.failures = test_data["failures"]
        report.total = test_data["total"]
        report.thresholds_missed = args.get("missed", 0)
        report.throughput = test_data["throughput"]
        report.pct50 = response_times["pct50"]
        report.pct75 = response_times["pct75"]
        report.pct90 = response_times["pct90"]
        report.pct95 = response_times["pct95"]
        report.pct99 = response_times["pct99"]
        report._max = response_times["max"]
        report._min = response_times["min"]
        report.mean = response_times["mean"]
        report.onexx = test_data["1xx"]
        report.twoxx = test_data["2xx"]
        report.threexx = test_data["3xx"]
        report.fourxx = test_data["4xx"]
        report.fivexx = test_data["5xx"]
        report.requests = ";".join(test_data["requests"])
        report.test_status = args["test_status"]
        report.vusers = args["vusers"]
        report.duration = args["duration"]
        report.commit()
        if report.test_status['status'].lower() in ['finished', 'error', 'failed', 'success']:
            write_test_run_logs_to_minio_bucket(report, project)
        return {"message": "updated"}

    def delete(self, project_id: int):
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            delete_ids = list(map(int, request.args["id[]"].split(',')))
        except TypeError:
            return make_response('IDs must be integers', 400)
        query_result = APIReport.query.filter(
            and_(APIReport.project_id == project.id, APIReport.id.in_(delete_ids))
        ).all()
        for each in query_result:
            try:
                delete_test_data(each.build_id, each.name, each.lg_type)
            except InfluxDBClientError as e:
                log.warning('InfluxDBClientError %s', e)
            baseline = APIBaseline.query.filter_by(project_id=project.id, report_id=each.id).first()
            if baseline:
                baseline.delete()
            each.delete()
        return {"message": "deleted"}


def write_test_run_logs_to_minio_bucket(test: APIReport, project):
    loki_settings_url = urlparse(current_app.config["CONTEXT"].settings.get('loki', {}).get('url'))
    if loki_settings_url:
        #
        build_id = test.build_id
        report_id = test.id
        project_id = test.project_id
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

            file_output.write(f'Test {test.name} (id={test.id}) run log:\n'.encode(enc))

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
            bucket_name = str(test.name).replace("_", "").replace(" ", "").lower()
            file_name = f"{test.build_id}.log"
            if bucket_name not in minio_client.list_bucket():
                minio_client.create_bucket(bucket=bucket_name, bucket_type='autogenerated')
            minio_client.upload_file(bucket_name, file_output, file_name)
        else:
            log.warning('Request to loki failed with status %s', response.status_code)
