from pylon.core.tools import web, log  # pylint: disable=E0611,E0401
from tools import auth, theme  # pylint: disable=E0401
from ..connectors.influx import get_sampler_types
from ..models.api_reports import APIReport
from ..utils.report_utils import render_analytics_control


class Slot:  # pylint: disable=E1101,R0903
    @web.slot('results_content')
    def content(self, context, slot, payload):
        # log.info('slot: [%s] || payload: [%s]', slot, payload)
        # log.info('payload request args: [%s]', payload.request.args)
        result_id = payload.request.args.get('result_id')
        if result_id:
            test_data = APIReport.query.get_or_404(result_id).to_json()
            try:
                test_data["failure_rate"] = round((test_data["failures"] / test_data["total"]) * 100, 2)
            except:
                test_data["failure_rate"] = 0

            # TODO set tags in model
            test_data["tags"] = []
            test_data["samplers"] = get_sampler_types(test_data["project_id"], test_data["build_id"],
                                                      test_data["name"], test_data["lg_type"])
            analytics_control = render_analytics_control(test_data["requests"])
            log.info("*****************************")
            log.info(test_data)
            log.info("*****************************")
            log.info(analytics_control)
            log.info("*****************************")

            with context.app.app_context():
                return self.descriptor.render_template(
                    'results/content.html',
                    test_data=test_data,
                    analytics_control=analytics_control
                )
        return theme.empty_content

    @web.slot('results_scripts')
    def scripts(self, context, slot, payload):
        # log.info('slot: [%s], payload: %s', slot, payload)
        result_id = payload.request.args.get('result_id')
        source_data = {}
        if result_id:
            test_data = APIReport.query.get_or_404(result_id).to_json()
            source_data = test_data['test_config'].get('source')
            analytics_control = render_analytics_control(test_data["requests"])

        with context.app.app_context():
            return self.descriptor.render_template(
                'results/scripts.html',
                source_data=source_data,
                test_data=test_data,
                analytics_control=analytics_control,
            )

    @web.slot('results_styles')
    def styles(self, context, slot, payload):
        # log.info('slot: [%s], payload: %s', slot, payload)
        with context.app.app_context():
            return self.descriptor.render_template(
                'results/styles.html',
            )
