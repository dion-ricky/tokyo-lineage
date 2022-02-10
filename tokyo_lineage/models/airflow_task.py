import attr

from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstance

from tokyo_lineage.models.base_task import BaseTask

class AirflowTaskMismatch(Exception):
    pass

class AirflowOperatorMismatch(Exception):
    pass

@attr.s
class AirflowTask(BaseTask):
    task: BaseOperator = attr.ib(init=True, default=None)
    task_instance: TaskInstance = attr.ib(init=True, default=None)

    @task_instance.validator
    def _check_task_instance(self, attribute, value):
        try:
            assert (self.task.task_id == value.task_id) and \
                    (value.task_id == self.task_id)
        except:
            raise AirflowTaskMismatch("Task and TaskInstance task_id should match."\
                    "{} != {}".format(self.task.task_id, value.task_id))
        
        try:
            assert (self.operator == value.operator)
        except:
            raise AirflowOperatorMismatch("Operator name should match."\
                "{} != {}".format(self.operator, value.operator))