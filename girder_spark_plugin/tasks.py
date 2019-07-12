import os
import io
import subprocess

from girder_worker.app import app
from girder_worker.utils import girder_job

DEFAULT_SPARK_SUBMIT_CMD='{}/bin/spark-submit'.format(os.environ['SPARK_HOME'])

@girder_job(title='Spark Job')
@app.task(bind=True)
def spark_job(self, file_path,
              spark_submit_cmd=None,
              spark_submit_opts=()):
    spark_cmd = spark_submit_cmd
    if spark_cmd is None:
        spark_cmd = [DEFAULT_SPARK_SUBMIT_CMD]

    spark_cmd.extend(spark_submit_opts)
    spark_cmd.append(file_path)

    self.job_manager.write('[INFO] Running spark cmd {}'.format(spark_cmd))
    self.job_manager.write('\n\n')

    proc = subprocess.Popen(spark_cmd,
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
