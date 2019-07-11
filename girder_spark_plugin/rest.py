from marshmallow import ValidationError

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
    .modelParam('id', model=Item, level=AccessType.READ, required=True)
    .param('image_path', 'path to singularity image for submitting spark job', required=True)
    .param('spark_opts', 'Options for spark', paramType='body', required=True)
    .errorResponse()
    .errorResponse('You are not an administrator.', 403))
def spark_job_endpoint(self, item, image_path, *args):
    _spark_opts = self.getBodyJson()
    try:
        result = SparkOptsSchema().load(_spark_opts)
    except ValidationError as e:
        raise RestException(', '.join(["{}: {}".format(k,v) for k,v in e.messages.items()]),
                            code=422)

    _file = list(Item().childFiles(item))[0]

    async_result = spark_job.delay(
        GirderFiletoNamedPath(_file),
        spark_submit_cmd=['singularity', 'run', image_path],
        spark_submit_opts=make_cli_opts(result)
    )

    return async_result.job
