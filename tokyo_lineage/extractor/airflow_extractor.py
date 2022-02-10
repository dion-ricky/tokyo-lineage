from airflow.models.dagrun import DagRun

from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance

from tokyo_lineage.extractor.base_extractor import BaseExtractor
from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.utils.airflow import get_dagbag, \
    get_task_instances_from_dagrun, get_dag_from_dagbag, get_task_from_dag, \
    instantiate_task_from_ti

class AirflowExtractor(BaseExtractor):
    def __init__(self):
        super(AirflowExtractor, self).__init__()

    def handle_job_run(self, job: DagRun):
        dagbag = get_dagbag()
        task_instances = get_task_instances_from_dagrun(job)
        dag = get_dag_from_dagbag(dagbag, job.dag_id)

        if len(task_instances) < 1:
            return

        for task_instance in task_instances:
            _task = get_task_from_dag(dag, task_instance.task_id)
            _task, _ = instantiate_task_from_ti(_task, task_instance)

            self._handle_task_run(_task, task_instance)
    
    def _handle_task_run(
        self,
        task: BaseOperator,
        task_instance: TaskInstance
    ):
        _task = AirflowTask(task.task_id, task, task_instance)
        self.handle_task_run(_task)

    def handle_task_run(self, task: AirflowTask):
        # extract metadata
        # register start_task
        # register finish_task or fail_task
        pass

    def report_task(self, task: AirflowTask):
        pass