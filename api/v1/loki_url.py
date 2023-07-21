from ...models.reports import Report
from flask import request

from tools import api_tools, constants as c


class API(api_tools.APIBase):
    url_params = [
        '<int:project_id>',
        '<string:mode>/<int:project_id>',
    ]

    def get(self, project_id: int, **kwargs):

        report_id = request.args.get("report_id", None)

        if not report_id:
            return {"message": ""}, 404

        build_id = Report.query.get_or_404(report_id).build_id

        websocket_base_url = c.APP_HOST.replace("http://", "ws://").replace("https://", "wss://")
        websocket_base_url += "/loki/api/v1/tail"
        logs_query = "{" + f'report_id="{report_id}",project="{project_id}",build_id="{build_id}"' + "}"

        logs_start = 0
        logs_limit = 10000000000

        return {
            "websocket_url": f"{websocket_base_url}?query={logs_query}&start={logs_start}&limit={logs_limit}"
        }, 200
