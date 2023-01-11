from sqlalchemy import and_
from flask import request
from flask_restful import Resource

from ...models.thresholds import Threshold

from tools import api_tools
from pydantic import ValidationError
from ...models.pd.thresholds import ThresholdPD


class API(Resource):
    url_params = [
        '<int:project_id>',
        '<int:project_id>/<int:threshold_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        if request.args.get("test") and request.args.get("env"):
            res = Threshold.query.filter(and_(
                Threshold.project_id == project.id,
                Threshold.test == request.args.get("test"),
                Threshold.environment == request.args.get("env")
            )).all()
            return [th.to_json() for th in res], 200
        total, res = api_tools.get(project_id, request.args, Threshold)
        return {'total': total, 'rows': [i.to_json() for i in res]}, 200

    def post(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            pd_obj = ThresholdPD(project_id=project_id, **request.json)
        except ValidationError as e:
            return e.errors(), 400
        th = Threshold(**pd_obj.dict())
        th.insert()
        return th.to_json(), 201

    def delete(self, project_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            delete_ids = list(map(int, request.args["id[]"].split(',')))
        except TypeError:
            return 'IDs must be integers', 400

        filter_ = and_(
            Threshold.project_id == project.id,
            Threshold.id.in_(delete_ids)
        )
        Threshold.query.filter(
            filter_
        ).delete()
        Threshold.commit()
        return {'ids': delete_ids}, 204

    def put(self, project_id: int, threshold_id: int):
        project = self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        try:
            pd_obj = ThresholdPD(project_id=project_id, **request.json)
        except ValidationError as e:
            return e.errors(), 400
        th_query = Threshold.query.filter(
            Threshold.project_id == project_id,
            Threshold.id == threshold_id
        )
        th_query.update(pd_obj.dict())
        Threshold.commit()
        return th_query.one().to_json(), 200
