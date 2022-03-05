import unittest
from unittest.mock import Mock
from datetime import datetime

from airflow.utils.state import State

from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.metadata_extractor.airflow import PostgresExtractor

class TestPostgresMetaExtractor(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)

        self.TASK_ID = 'test_task'
        self.OPERATOR = 'test_operator'

        task_id = self.TASK_ID
        operator_name = self.OPERATOR

        self.DAG_ID = 'test_dag'
        self.POSTGRES_CONN_ID = 'test_connection'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = self.DAG_ID
        _task.postgres_conn_id = self.POSTGRES_CONN_ID

        task_instance = Mock()
        task_instance.state = State.SUCCESS
        task_instance.task_id = task_id
        task_instance.operator = operator_name
        task_instance.try_number = 1
        task_instance.execution_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.start_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.end_date = datetime(2022, 2, 10, 8, 0, 0)

        task = AirflowTask(task_id, operator_name, _task, task_instance)

        meta_extractor = PostgresExtractor(task)

        self.task = task
        self.task_instance = task_instance
        self.meta_extractor = meta_extractor
    
    def test_get_conn_id(self):
        meta_extractor = self.meta_extractor

        self.assertEqual(meta_extractor._conn_id(), self.POSTGRES_CONN_ID)