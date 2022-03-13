import json
import getpass
import platform
import unittest
from datetime import datetime
from unittest.mock import Mock

from avro import schema
from airflow.utils.state import State

from openlineage.common.dataset import Source, Field

from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.metadata_extractor.airflow import FileToGcsExtractor
from tokyo_lineage.metadata_extractor.airflow.file_to_gcs_extractor import EXPORTER_OPERATOR_CLASSNAMES

class TestFileToGcsMetaExtractor(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        task_id = 'test_task'
        operator_name = 'test_operator'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = 'test_dag'
        _task.bucket = 'test_bucket'
        
        ul1 = Mock()
        ul1.__class__.__name__ = 'RandomUselessOperator'
        ul1.dag_id = 'useless_dag_id'
        ul1.task_id = 'useless_task_id'

        ul2 = Mock()
        ul2.__class__.__name__ = 'PostgresToAvroOperator'
        ul2.dag_id = 'test_dag'
        ul2.task_id = 'test_export_to_avro'

        _task.upstream_list = [ul1, ul2]

        task_instance = Mock()
        task_instance.state = State.SUCCESS
        task_instance.task_id = task_id
        task_instance.operator = operator_name
        task_instance.try_number = 1
        task_instance.execution_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.start_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.end_date = datetime(2022, 2, 10, 8, 0, 0)

        task = AirflowTask(task_id, operator_name, _task, task_instance)

        meta_extractor = FileToGcsExtractor(task)

        mock_connection = Mock()
        mock_connection.get_extra = lambda: '{"extra__google_cloud_platform__project": "test_project"}'
        meta_extractor._get_gcs_connection = lambda: mock_connection

        self.task = task
        self.task_instance = task_instance
        self.meta_extractor = meta_extractor
    
    def test_gcs_scheme(self):
        self.assertEqual(self.meta_extractor._get_gcs_scheme(), 'gs')
    
    def test_gcs_connection_uri(self):
        self.assertEqual(self.meta_extractor._get_gcs_connection_uri(), 'gs://test_project/test_bucket')
    
    def test_gcs_authority(self):
        self.assertEqual(self.meta_extractor._get_gcs_authority(), 'test_project')
    
    def test_get_nearest_exporter(self):
        meta_extractor = self.meta_extractor
        
        exporter = meta_extractor._get_nearest_exporter_upstream()

        self.assertTrue(exporter.__class__.__name__ in EXPORTER_OPERATOR_CLASSNAMES)
    
    def test_get_input_dataset_name(self):
        self.assertEqual(self.meta_extractor._get_input_dataset_name(), 'test_dag_test_export_to_avro')