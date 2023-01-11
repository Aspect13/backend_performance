from flask import request
from flask_restful import Resource
from ...models.reports import Report
from sqlalchemy import and_


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        query_result = Report.query.with_entities(Report.environment).filter(
            and_(
                Report.name == request.args.get("name"),
                Report.project_id == project.id
            )
        ).distinct(Report.environment).all()
        return [i[0] for i in query_result], 200
