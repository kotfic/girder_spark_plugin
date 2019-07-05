from girder_worker.app import app
from girder_worker.utils import girder_job


@girder_job(title='Spark Job')
@app.task(bind=True)
def spark_job(self, file_path):
    return file_path
