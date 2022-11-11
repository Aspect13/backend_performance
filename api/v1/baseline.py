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
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        baseline = APIBaseline.query.filter_by(project_id=project.id, test=args.get("test_name"),
                                               environment=args.get("env")).first()
        test = baseline.summary if baseline else []
        report_id = baseline.report_id if baseline else 0
        return {"baseline": test, "report_id": report_id}

    def post(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        report: APIReport = APIReport.query.get_or_404(request.json['report_id'])
        if report.project_id != project.id:
            return 'Not found', 404
        test = get_aggregated_test_results(report.name, report.build_id)
        summary = []
        for req in test[0]:
            summary.append(req)
        APIBaseline.query.filter_by(
            project_id=project.id, test=report.name,
            environment=report.environment
        ).delete()
        baseline = APIBaseline(test=report.name,
                               environment=report.environment,
                               project_id=project.id,
                               report_id=report.id,
                               summary=summary)
        baseline.insert()
        return baseline.to_json(), 200
