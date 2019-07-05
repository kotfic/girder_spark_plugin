from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import boundHandler, filtermodel
from girder.models.item import Item
from girder.constants import AccessType

from girder_jobs.models.job import Job as JobModel
from girder_spark_plugin.tasks import spark_job
from girder_spark_plugin.utils import GirderFiletoNamedPath



@access.user
@boundHandler
@filtermodel(model=JobModel)
@autoDescribeRoute(
    Description('Launch a Spark Job')
    .notes('This endpoint launches a spark job defined by a pyspark file '
           'associated with an item.')
    .modelParam('id', model=Item, level=AccessType.READ)
    .errorResponse()
    .errorResponse('You are not an administrator.', 403))
def spark_job_endpoint(self, item):
    _file = list(Item().childFiles(item))[0]
    async_result = spark_job.delay(GirderFiletoNamedPath(_file))

    return async_result.job
