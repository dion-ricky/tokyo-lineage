import unittest
from unittest.mock import MagicMock, PropertyMock

from tokyo_lineage.models.airflow_task import AirflowTask, AirflowTaskMismatch

class TestTaskModel(unittest.TestCase):

    def test_airflow_task(self):
        task = MagicMock()
        task_id = PropertyMock(return_value='test_task_id')
        type(task).task_id = task_id

        task_instance = MagicMock()
        task_id2 = PropertyMock(return_value='test_task_id')
        type(task_instance).task_id = task_id2

        AirflowTask(task_id='test_task_id', task=task, task_instance=task_instance)
    
    def test_airflow_task_mismatch(self):
        task = MagicMock()
        task_id = PropertyMock(return_value='test_task_id')
        type(task).task_id = task_id

        task_instance = MagicMock()
        task_id2 = PropertyMock(return_value='test_task_id_not_match')
        type(task_instance).task_id = task_id2

        with self.assertRaises(AirflowTaskMismatch):
            AirflowTask(task_id='test_task_id', task=task, \
                task_instance=task_instance)