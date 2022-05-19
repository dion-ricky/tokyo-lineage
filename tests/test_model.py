import unittest
from unittest.mock import MagicMock

from tokyo_lineage.models.airflow_task import AirflowTask, AirflowTaskMismatch
from tokyo_lineage.models.airflow_dag import AirflowDag

class TestTaskModel(unittest.TestCase):

    def test_airflow_task(self):
        test_task_id = 'test_task_id'
        test_operator_name = 'test_operator'

        task = MagicMock()
        task.task_id = test_task_id

        task_instance = MagicMock()
        task_instance.task_id = test_task_id
        task_instance.operator = test_operator_name

        AirflowTask(task_id=test_task_id, operator_name=test_operator_name, task=task, task_instance=task_instance)
    
    def test_airflow_task_mismatch(self):
        test_task_id = 'test_task_id'
        test_operator_name = 'test_operator'

        task = MagicMock()
        task.task_id = test_task_id

        task_instance = MagicMock()
        task_instance.task_id = 'different_test_id'
        task_instance.operator = test_operator_name

        with self.assertRaises(AirflowTaskMismatch):
            AirflowTask(task_id=test_task_id, operator_name=test_operator_name, task=task, task_instance=task_instance)
    
    def test_airflow_dag(self):
        test_dag_id = 'test_dag_id'

        dag = MagicMock()
        dag.dag_id = test_dag_id

        dagrun = MagicMock()
        
        AirflowDag(test_dag_id, dag, dagrun)