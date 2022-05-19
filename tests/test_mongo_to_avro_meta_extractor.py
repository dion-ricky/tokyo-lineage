import unittest
from datetime import datetime
from unittest.mock import Mock

from airflow.utils.state import State

from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.metadata_extractor.airflow import MongoToAvroExtractor


class TestMongoToAvroMetaExtractor(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        task_id = 'test_task'
        operator_name = 'test_operator'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = 'test_dag'
        _task.avro_output_path = '/test/path.avro'

        mongo_conn = Mock()
        mongo_conn.host = 'localhost'
        mongo_conn.port = '27017'
        mongo_conn.get_uri = lambda: 'mongodb://mongo:mongo@localhost:27017/?authSource=admin'

        task_instance = Mock()
        task_instance.state = State.SUCCESS
        task_instance.task_id = task_id
        task_instance.operator = operator_name
        task_instance.try_number = 1
        task_instance.execution_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.start_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.end_date = datetime(2022, 2, 10, 8, 0, 0)

        task = AirflowTask(task_id, operator_name, _task, task_instance)

        meta_extractor = MongoToAvroExtractor(task)

        # Attach mock connection
        meta_extractor.conn = mongo_conn

        self.task = task
        self.task_instance = task_instance
        self.meta_extractor = meta_extractor
    
    def test_mongo_scheme(self):
        meta_extractor = self.meta_extractor

        self.assertEqual(meta_extractor._get_mongo_scheme(), 'mongo')
    
    def test_mongo_connection_uri(self):
        meta_extractor = self.meta_extractor

        self.assertEqual(meta_extractor._get_mongo_connection_uri(), 'mongodb://localhost:27017/?authSource=admin')

    def test_mongo_authority(self):
        meta_extractor = self.meta_extractor

        self.assertEqual(meta_extractor._get_mongo_authority(), 'localhost:27017')