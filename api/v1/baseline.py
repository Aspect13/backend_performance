from flask import request, make_response
from flask_restful import Resource
from ...models.api_baseline import APIBaseline
from ...models.api_reports import APIReport
from ...connectors.influx import get_aggregated_test_results


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        baseline = APIBaseline.query.filter(
            APIBaseline.project_id == project.id,
            APIBaseline.test == request.args.get("test_name"),
            APIBaseline.environment == request.args.get("env")
        ).first()
        try:
            return {"baseline": baseline.summary, "report_id": baseline.report_id}, 200
        except AttributeError:
            return 'Baseline not found', 404

    def post(self, project_id: int):
        try:
            report_id = int(request.json['report_id'])
        except (KeyError, ValueError):
            return 'report_id must be provided', 400
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        report: APIReport = APIReport.query.filter(
            APIReport.project_id == project.id,
            APIReport.id == report_id
        ).first()
        if not report:
            return 'Not found', 404
        test = get_aggregated_test_results(report.name, report.build_id)
        summary = [i for i in test[0]]

        baseline = APIBaseline(test=report.name,
                               environment=report.environment,
                               project_id=project.id,
                               report_id=report.id,
                               summary=summary)
        baseline.insert()
        return baseline.to_json(), 200
