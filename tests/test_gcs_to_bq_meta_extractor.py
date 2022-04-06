import json
import unittest
from datetime import datetime
from unittest.mock import Mock

from airflow.utils.state import State

from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.metadata_extractor.airflow import GcsToBigQueryExtractor

class TestGcsToBqExtractor(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        task_id = 'test_task'
        operator_name = 'test_operator'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = 'test_dag'
        _task.bucket = 'test_bucket'
        _task.dst = '/test/path.avro'
        _task.destination_project_dataset_table = 'p.d.t'
        
        ul1 = Mock()
        ul1.__class__.__name__ = 'RandomUselessOperator'
        ul1.dag_id = 'useless_dag_id'
        ul1.task_id = 'useless_task_id'

        ul2 = Mock()
        ul2.__class__.__name__ = 'FileToGoogleCloudStorageOperator'
        ul2.dag_id = 'test_dag'
        ul2.task_id = 'test_upload_to_gcs'

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

        meta_extractor = GcsToBigQueryExtractor(task)

        mock_connection = Mock()
        mock_connection.get_extra = lambda: '{"extra__google_cloud_platform__project": "test_project"}'
        meta_extractor._get_gcs_connection = lambda: mock_connection
        
        mock_connection_bq = Mock()
        mock_connection_bq.get_extra = lambda: '{"extra__google_cloud_platform__project": "test_project"}'
        meta_extractor._get_bq_connection = lambda: mock_connection_bq

        self.task = task
        self.task_instance = task_instance
        self.meta_extractor = meta_extractor
    
    def test_gcs_scheme(self):
        meta_extractor = self.meta_extractor
        self.assertEqual(meta_extractor._get_gcs_scheme(), 'gs')
    
    def test_gcs_authority(self):
        meta_extractor = self.meta_extractor
        self.assertEqual(meta_extractor._get_gcs_authority(), meta_extractor.operator.bucket)

    def test_gcs_connection_uri(self):
        meta_extractor = self.meta_extractor
        scheme = meta_extractor._get_gcs_scheme()
        bucket = meta_extractor.operator.bucket
        path = meta_extractor.operator.dst
        self.assertEqual(meta_extractor._get_gcs_connection_uri(), f"{scheme}://{bucket}{path}")
    
    def test_get_project_dataset_table(self):
        meta_extractor = self.meta_extractor
        p, d, t = meta_extractor._get_project_dataset_table()

        self.assertEqual(p, 'p')
        self.assertEqual(t, 't')
        self.assertEqual(d, 'd')
    
    def test_bq_scheme(self):
        meta_extractor = self.meta_extractor
        self.assertEqual(meta_extractor._get_bq_scheme(), 'bigquery')
    
    def test_bq_authority(self):
        meta_extractor = self.meta_extractor
        self.assertEqual(meta_extractor._get_bq_authority(), '')
    
    def test_bq_connection_uri(self):
        meta_extractor = self.meta_extractor
        scheme = meta_extractor._get_bq_scheme()
        _, dataset, table = meta_extractor._get_project_dataset_table()

        conn = meta_extractor._get_bq_connection()
        extras = json.loads(conn.get_extra())
        project_id = extras['extra__google_cloud_platform__project']
        self.assertEqual(meta_extractor._get_bq_connection_uri(), f"{scheme}:{project_id}.{dataset}.{table}")
    
    def test_output_dataset_name(self):
        meta_extractor = self.meta_extractor
        _, dataset, table = meta_extractor._get_project_dataset_table()
        self.assertEqual(meta_extractor._get_output_dataset_name(), f"{dataset}.{table}")
