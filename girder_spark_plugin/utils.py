import os
import shutil
import tempfile

from girder_worker_utils.transforms.girder_io import GirderClientTransform

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
