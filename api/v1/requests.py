from flask import request
from flask_restful import Resource
from pylon.core.tools import log

from ...models.api_reports import APIReport


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        args = request.args
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        requests_data = set()
        query_result = APIReport.query.with_entities(APIReport.requests).filter(
            APIReport.name == args.get('name'),
            APIReport.environment == args.get('environment'),
            APIReport.project_id == project.id
        ).all()
        for i in query_result:
            requests_data.update(set(i))
        try:
            requests_data.remove('All')
        except KeyError:
            ...
        return list(requests_data), 200
