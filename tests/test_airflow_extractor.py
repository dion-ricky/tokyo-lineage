import unittest
from unittest.mock import Mock, MagicMock, PropertyMock

from tokyo_lineage.extractor.airflow_extractor import AirflowExtractor
from tokyo_lineage.utils.airflow import get_dagruns

class TestAirflowExctractor(unittest.TestCase):
    
    def test_init(self):
        AirflowExtractor()
    
    def test_handle_job_run(self):
        dagruns = get_dagruns()
        extractor = AirflowExtractor()

        extractor._handle_task_run = Mock()

        extractor.handle_jobs_run(dagruns)

        extractor._handle_task_run.assert_called()