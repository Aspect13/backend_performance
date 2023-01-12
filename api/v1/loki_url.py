from ...models.api_reports import APIReport
import flask
from flask import make_response
from flask_restful import Resource
from ....shared.tools.constants import APP_HOST


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):

        report_id = flask.request.args.get("report_id", None)

        if not report_id:
            return make_response({"message": ""}, 404)

        build_id = APIReport.query.get_or_404(report_id).to_json()["build_id"]

        websocket_base_url = APP_HOST.replace("http://", "ws://").replace("https://", "wss://")
        websocket_base_url += "/loki/api/v1/tail"
        logs_query = "{" + f'report_id="{report_id}",project="{project_id}",build_id="{build_id}"' + "}"

        logs_start = 0
        logs_limit = 10000000000

        return make_response(
            {"websocket_url": f"{websocket_base_url}?query={logs_query}&start={logs_start}&limit={logs_limit}"},
            200
        )
