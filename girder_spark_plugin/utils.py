import os
import shutil
import tempfile

from girder_worker_utils.transforms.girder_io import GirderClientTransform
from marshmallow import Schema, fields, post_load

class GirderFiletoNamedPath(GirderClientTransform):
    """
    This transform downloads a Girder File to the local machine and passes its
    local path into the function.
    :param _id: The ID of the file to download.
    :type _id: str
    """
    def __init__(self, file_model, **kwargs):
        super().__init__(**kwargs)
        self.file_id = str(file_model['_id'])
        self.file_name = str(file_model['name'])
        self.file_path = None

    def _repr_model_(self):
        return "{}('{}')".format(self.__class__.__name__, self.file_id)

    def transform(self):
        self.file_path = os.path.join(
            tempfile.mkdtemp(), '{}'.format(self.file_name))

        self.gc.downloadFile(self.file_id, self.file_path)

        return self.file_path

    def cleanup(self):
        shutil.rmtree(os.path.dirname(self.file_path),
                      ignore_errors=True)


SPARK_OPT_MAP = {
    'master': '--master',
    'name': '--name',
    'class_name': '--class',
    'jars': '--jars',
    'packages': '--pacakges',
    'exclude_packages': '--exclude-packages',
    'repositories': '--repositories',
    'py_files': '--py-files',
    'files': '--files',
    'conf': '--conf',
    'properties_file': '--properties-file'
}

class SparkOptsSchema(Schema):
    master = fields.Str(required=True)
    name = fields.Str(required=False)
    class_name = fields.Str(required=False)
    jars = fields.List(fields.Str(), required=False)
    packages = fields.List(fields.Str(), required=False)
    exclude_packages = fields.List(fields.Str(), required=False)
    repositories =  fields.List(fields.Str(), required=False)
    py_files = fields.List(fields.Str(), required=False)
    files = fields.List(fields.Str(), required=False)
    conf = fields.Dict(keys=fields.Str(), values=fields.Str(), required=False)
    properties_file = fields.Str(required=False)


def make_cli_opts(data, schema_cls=SparkOptsSchema):
    '''Make cli option list from data'''
    schema = schema_cls()
    cli_opts = []
    for key, val in data.items():
        if isinstance(schema.fields[key], fields.List):
            cli_opts.append(f'{SPARK_OPT_MAP[key]} {",".join(val)}')
        elif isinstance(schema.fields[key], fields.Dict):
            for prop, spark_val in val.items():
                cli_opts.append(f'{SPARK_OPT_MAP[key]} {prop}={spark_val}')
        else:
            cli_opts.append(f'{SPARK_OPT_MAP[key]} {val}')

    return cli_opts
