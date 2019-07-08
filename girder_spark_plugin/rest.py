from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import boundHandler, filtermodel
from girder.constants import AccessType
from girder.exceptions import RestException
from girder.models.item import Item

from girder_jobs.models.job import Job as JobModel
from girder_spark_plugin.tasks import spark_job

from girder_spark_plugin.utils import GirderFiletoNamedPath, SparkOptsSchema, make_cli_opts


@access.user
@boundHandler
@filtermodel(model=JobModel)
@autoDescribeRoute(
    Description('Launch a Spark Job')
    .notes('This endpoint launches a spark job defined by a pyspark file '
           'associated with an item.')
    .modelParam('id', model=Item, level=AccessType.READ)
    .param('master', 'spark://host:port, mesos://host:port, yarn, k8s://https://host:port, or local (Default: local[*]).)')
    .errorResponse()
    .errorResponse('You are not an administrator.', 403))
def spark_job_endpoint(self, item, master):
    _spark_opts = {
        'master': master,
    }
    result = SparkOptsSchema().load(_spark_opts)

    if len(result.errors):
        error_msg = 'Errors were found in your arguments:\n'
        for key, value in result.errors.items():
            error_msg += f'  {key}: {value}\n'

        raise RestException(error_msg, code=422)

    _file = list(Item().childFiles(item))[0]

    async_result = spark_job.delay(
        GirderFiletoNamedPath(_file),
        spark_submit_opts=make_cli_opts(result.data)
    )

    return async_result.job
