from girder_worker import GirderWorkerPluginABC
from girder import plugin
from girder_spark_plugin.rest import spark_job_endpoint

class GirderPlugin(plugin.GirderPlugin):
    DISPLAY_NAME = 'spark_plugin'

    def load(self, info):
        info['apiRoot'].item.route('POST', (':id', 'spark_job'), spark_job_endpoint)



class GirderWorkerPlugin(GirderWorkerPluginABC):
    def __init__(self, app, *args, **kwargs):
        self.app = app

    def task_imports(self):
        # Return a list of python importable paths to the
        # plugin's path directory
        return ['girder_spark_plugin.tasks']
