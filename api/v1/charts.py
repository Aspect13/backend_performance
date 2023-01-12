from functools import partial

from flask import request
from flask_restful import Resource

from ...utils.charts_utils import requests_summary, requests_hits, avg_responses, summary_table, \
    get_data_from_influx, get_issues, engine_health


class API(Resource):
    url_params = [
        '<string:source>/<string:target>',
    ]

    mapping = {
        "requests": {
            "summary": requests_summary,
            "hits": requests_hits,
            "average": avg_responses,
            "table": summary_table,
            "data": get_data_from_influx
        },
        "errors": {
            "table": get_issues
        },
        "engine_health": {
            "cpu": partial(engine_health, part='cpu'),
            "memory": partial(engine_health, part='memory'),
            "load": partial(engine_health, part='load'),
            "all": engine_health,
        }
    }

    def __init__(self, module):
        self.module = module

    def get(self, source: str, target: str):
        args = request.args.to_dict(flat=True)
        for i in request.args.keys():
            if i.endswith('[]'):
                args[i] = request.args.getlist(i)
        return self.mapping[source][target](args)
