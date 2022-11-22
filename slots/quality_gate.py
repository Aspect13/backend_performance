from pylon.core.tools import web  # pylint: disable=E0611,E0401


class Slot:
    @web.slot(f'backend_performance_processing_content')
    def content(self, context, slot, payload):
        with context.app.app_context():
            return self.descriptor.render_template(
                'quality_gate/content.html',
                instance_name_prefix=payload.get('instance_name_prefix', '')
            )

    @web.slot('backend_performance_processing_scripts')
    def scripts(self, context, slot, payload):
        with context.app.app_context():
            return self.descriptor.render_template(
                'quality_gate/scripts.html',
            )
