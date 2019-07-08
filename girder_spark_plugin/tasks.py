import os
import io
import subprocess

from celery.utils.log import get_task_logger
from girder_worker.app import app
from girder_worker.utils import girder_job

SPARK_HOME='/home/kotfic/kitware/projects/hpcmp/demo/july_2019/spark/spark-2.4.3-bin-hadoop2.7'
DEFAULT_SPARK_SUBMIT_CMD=f'{SPARK_HOME}/bin/spark-submit'


@girder_job(title='Spark Job')
@app.task(bind=True)
def spark_job(self, file_path,
              spark_submit_cmd=DEFAULT_SPARK_SUBMIT_CMD,
              spark_submit_opts=()):
    logger = get_task_logger(__name__)

    spark_cmd = [spark_submit_cmd]
    spark_cmd.extend(spark_submit_opts)
    spark_cmd.append(file_path)

    env = os.environ.copy()
    env['SPARK_HOME'] =  SPARK_HOME

    self.job_manager.write(f'[INFO] Running spark cmd {spark_cmd}')
    self.job_manager.write(f'\n\n')

    proc = subprocess.Popen(spark_cmd,
                            env=env,
                            stderr=subprocess.STDOUT,
                            stdout=subprocess.PIPE)

    output = ''
    for line in io.TextIOWrapper(proc.stdout, encoding="utf-8"):
        output += line
        self.job_manager.write(f'[INFO] Output:   {line}')

    self.job_manager.write(f'\n\n')
    self.job_manager.write(f'[INFO] Complete')

    proc.wait()
    if proc.returncode != 0:
            raise RuntimeError(f'\n\nSpark job did not successfully submit: {proc.returncode}'
                           f'\n{spark_cmd}'
                           f'\n\n{output}')

    return proc.returncode
