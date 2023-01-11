from flask_restful import Resource

from ...models.reports import Report
from ...models.tests import Test
from ...utils.utils import run_test


class API(Resource):
    url_params = [
        '<int:result_id>',
    ]

    def __init__(self, module):
        self.module = module

    def post(self, result_id: int):
        """
        Post method for re-running test
        """
        report = Report.query.get_or_404(result_id)
        config = report.test_config
        config.pop('job_type', None)
        proxy_test = Test(**config)
        resp = run_test(proxy_test, config_only=False, execution=False)
        return resp, resp.get('code', 200)

