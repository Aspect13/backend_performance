from pylon.core.tools import web, log  # pylint: disable=E0611,E0401
from tools import auth  # pylint: disable=E0401

from ..constants import JMETER_MAPPING, GATLING_MAPPING


class Slot:  # pylint: disable=E1101,R0903
    @web.slot('backend_performance_content')
    def content(self, context, slot, payload):
        project_id = context.rpc_manager.call.project_get_id()
        public_regions = context.rpc_manager.call.get_rabbit_queues("carrier")
        public_regions.remove("__internal")
        project_regions = context.rpc_manager.call.get_rabbit_queues(f"project_{project_id}_vhost")
        cloud_regions = context.rpc_manager.timeout(5).integrations_get_cloud_integrations(
                project_id)
        with context.app.app_context():
            return self.descriptor.render_template(
                'backend_performance/content.html',
                runners={
                    'jMeter': list(map(lambda i: {'version': i}, JMETER_MAPPING.keys())),
                    'Gatling': list(map(lambda i: {'version': i}, GATLING_MAPPING.keys()))
                },
                locations={
                    'public_regions': public_regions,
                    'project_regions': project_regions,
                    "cloud_regions": cloud_regions
                }
            )

    @web.slot('backend_performance_scripts')
    def scripts(self, context, slot, payload):
        with context.app.app_context():
            return self.descriptor.render_template(
                'backend_performance/scripts.html',
            )

    @web.slot('backend_performance_styles')
    def styles(self, context, slot, payload):
        with context.app.app_context():
            return self.descriptor.render_template(
                'backend_performance/styles.html',
            )
