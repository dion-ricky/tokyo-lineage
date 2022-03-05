import os
from datetime import datetime
import unittest
from unittest.mock import Mock, MagicMock, PropertyMock

from airflow.utils.state import State

from openlineage.airflow.utils import get_custom_facets
from openlineage.airflow.extractors.base import TaskMetadata

from tokyo_lineage.utils.airflow import get_dagruns, get_location
from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.models.airflow_dag import AirflowDag
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

    def test_prepare_start_task(self):
        task_id = 'test_task'
        operator_name = 'test_operator'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = 'test_dag'

        task_instance = Mock()
        task_instance.state = State.SUCCESS
        task_instance.task_id = task_id
        task_instance.operator = operator_name
        task_instance.try_number = 1
        task_instance.execution_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.start_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.end_date = datetime(2022, 2, 10, 8, 0, 0)

        task = AirflowTask(task_id, operator_name, _task, task_instance)

        dag_id = 'test_dag'

        dag = Mock()
        dag.dag_id = dag_id
        dag.description = 'Test DAG'
        dag.full_filepath = os.path.abspath(__file__)
        
        dagrun = Mock()
        dagrun.run_id = 'scheduled__test_run'
        dagrun.external_trigger = False

        job = AirflowDag(dag_id, dag, dagrun)

        extractor = AirflowExtractor()
        extractor.register_task_start = Mock()

        extractor._register_task_start(task, job)

        print(extractor.register_task_start.call_args)

        extractor.register_task_start.assert_called()

        extractor.register_task_start.assert_called_with(
            '40722faaca4b80d64dbfd04f32bfdbb0bc2db0f9',
            'test_dag.test_task',
            'Test DAG',
            '2022-02-10T07:00:00.000000Z',
            'scheduled__test_run',
            get_location(dag.full_filepath),
            '2022-02-10T07:00:00.000000Z',
            '2022-02-10T08:00:00.000000Z',
            extractor.get_extractor(task).extract(),
            get_custom_facets(_task, dagrun.external_trigger)
        )
    
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
    
    def test_lineage_run_id(self):
        task_id = 'test_task'
        operator_name = 'test_operator'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = 'test_dag'

        task_instance = Mock()
        task_instance.state = State.SUCCESS
        task_instance.task_id = task_id
        task_instance.operator = operator_name
        task_instance.try_number = 1
        task_instance.execution_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.start_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.end_date = datetime(2022, 2, 10, 8, 0, 0)

        task = AirflowTask(task_id, operator_name, _task, task_instance)

        dag_id = 'test_dag'

        dag = Mock()
        dag.dag_id = dag_id
        dag.description = 'Test DAG'
        dag.full_filepath = os.path.abspath(__file__)
        
        dagrun = Mock()
        dagrun.run_id = 'scheduled__test_run'
        dagrun.external_trigger = False

        job = AirflowDag(dag_id, dag, dagrun)

        extractor = AirflowExtractor()
        extractor.random_task_state = 11

        run_id = extractor._new_lineage_run_id(task, job)
        print(run_id)

        self.assertEqual(run_id, '8a227704c7b9159a8ab59eaef836bd1e1401803a')