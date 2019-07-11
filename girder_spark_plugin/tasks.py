import os
import io
import subprocess

from celery.utils.log import get_task_logger
from girder_worker.app import app
from girder_worker.utils import girder_job

SPARK_HOME='/home/kotfic/kitware/projects/hpcmp/demo/july_2019/spark/spark-2.4.3-bin-hadoop2.7'
DEFAULT_SPARK_SUBMIT_CMD='{}/bin/spark-submit'.format(SPARK_HOME)


@girder_job(title='Spark Job')
@app.task(bind=True)
def spark_job(self, file_path,
              spark_submit_cmd=None,
              spark_submit_opts=()):
    logger = get_task_logger(__name__)

    spark_cmd = spark_submit_cmd
    if spark_cmd is None:
        spark_cmd = [DEFAULT_SPARK_SUBMIT_CMD]

    spark_cmd.extend(spark_submit_opts)
    spark_cmd.append(file_path)

    env = os.environ.copy()
    env['SPARK_HOME'] =  SPARK_HOME

    self.job_manager.write('[INFO] Running spark cmd {}'.format(spark_cmd))
    self.job_manager.write('\n\n')

    proc = subprocess.Popen(spark_cmd,
                            env=env,
                            stderr=subprocess.STDOUT,
                            stdout=subprocess.PIPE)

    output = ''
    for line in io.TextIOWrapper(proc.stdout, encoding="utf-8"):
        output += line
        self.job_manager.write('[INFO] Output:   {}'.format(line))

    self.job_manager.write('\n\n')
    self.job_manager.write('[INFO] Complete')

    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError('\n\nSpark job did not successfully submit: {}'.format(proc.returncode) +
                           '\n{}'.format(spark_cmd) +
                           '\n\n{}'.format(output))

    return proc.returncode
