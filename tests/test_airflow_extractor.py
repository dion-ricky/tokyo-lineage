from datetime import datetime
import unittest
from unittest.mock import Mock

from openlineage.airflow.extractors.base import TaskMetadata

from tokyo_lineage.utils.airflow import get_dagruns
from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.extractor.airflow_extractor import AirflowExtractor, AirflowMetaExtractor

class TestAirflowExctractor(unittest.TestCase):
    
    def test_init(self):
        AirflowExtractor()
    
    def test_handle_job_run(self):
        dagruns = get_dagruns()
        extractor = AirflowExtractor()

        extractor._handle_task_run = Mock()

        extractor.handle_jobs_from_dagrun(dagruns)

        extractor._handle_task_run.assert_called()
    
    def test_airflow_meta_extractor(self):
        task_id = 'test_task'
        operator_name = 'test_operator'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = 'test_dag'

        task_instance = Mock()
        task_instance.task_id = task_id
        task_instance.operator = operator_name
        task_instance.start_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.end_date = datetime(2022, 2, 10, 8, 0, 0)

        task = AirflowTask(task_id, operator_name, _task, task_instance)

        meta_extractor = AirflowMetaExtractor(task)

        self.assertEqual(meta_extractor.extract(), TaskMetadata(name='test_dag.test_task'))