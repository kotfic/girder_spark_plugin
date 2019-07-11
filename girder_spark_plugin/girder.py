from girder import plugin
from girder_spark_plugin.rest import spark_job_endpoint

class GirderPlugin(plugin.GirderPlugin):
    DISPLAY_NAME = 'spark_plugin'

    def load(self, info):
        info['apiRoot'].item.route('POST', (':id', 'spark_job'), spark_job_endpoint)
