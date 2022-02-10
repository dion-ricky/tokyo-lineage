import unittest
from datetime import datetime

from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance

from tokyo_lineage.utils.airflow import get_dagruns, get_dagbag, \
    get_dag_from_dagbag, get_task_instances, get_task_from_dag, \
    instantiate_task, instantiate_task_from_ti, get_task_instance_from_dagrun,\
    get_task_instances_from_dagrun

class TestUtils(unittest.TestCase):

    def test_get_dagruns(self):
        dagrun = get_dagruns()

        print(dagrun)

        self.assertIsInstance(dagrun, list)

        if len(dagrun) > 0:
            self.assertIsInstance(dagrun[0], DagRun)
    
    def test_get_dagruns_filter(self):
        dagrun = get_dagruns(DagRun.dag_id == 'example_bash_operator', DagRun.run_id.like('scheduled__%'))

        print(dagrun)

        self.assertIsInstance(dagrun, list)

        if len(dagrun) > 0:
            self.assertIsInstance(dagrun[0], DagRun)
    
    def test_get_dagbag(self):
        dagbag = get_dagbag()

        self.assertIsInstance(dagbag, DagBag)
    
    def test_get_dag(self):
        dagbag = get_dagbag()
        
        dag = get_dag_from_dagbag(dagbag, 'example_bash_operator')
        
        self.assertIsInstance(dag, DAG)
    
    def test_get_dag_fullpath(self):
        dagbag = get_dagbag()
        
        dag = get_dag_from_dagbag(dagbag, 'example_bash_operator')
        
        self.assertIsInstance(dag, DAG)

        full_filepath = dag.full_filepath

        print(full_filepath)

        self.assertIsInstance(full_filepath, str)
    
    def test_get_task_instances(self):
        task_instances = get_task_instances()

        print(task_instances)

        self.assertIsInstance(task_instances, list)

        if len(task_instances) > 0:
            self.assertIsInstance(task_instances[0], TaskInstance)
    
    def test_get_task_instances_filter(self):
        task_instances = get_task_instances(TaskInstance.task_id == 'runme_1')

        print(task_instances)

        self.assertIsInstance(task_instances, list)

        if len(task_instances) > 0:
            self.assertIsInstance(task_instances[0], TaskInstance)

    def test_get_task_instances_from_dagrun(self):
        dagruns = get_dagruns(DagRun.dag_id == 'example_bash_operator')
        dagrun = dagruns[-1]

        task_instances = get_task_instances_from_dagrun(dagrun)

        self.assertIsInstance(task_instances, list)

        if len(task_instances) > 0:
            self.assertIsInstance(task_instances[0], TaskInstance)

    def test_get_task_instance_from_dagrun(self):
        dagruns = get_dagruns(DagRun.dag_id == 'example_bash_operator')
        dagrun = dagruns[-1]

        task_instance = get_task_instance_from_dagrun(dagrun, 'runme_1')

        self.assertIsInstance(task_instance, TaskInstance)

    def test_get_task(self):
        dagbag = get_dagbag()
        
        dag = get_dag_from_dagbag(dagbag, 'example_bash_operator')

        task = get_task_from_dag(dag, "run_this_last")

        self.assertIsInstance(task, BaseOperator)
    
    def test_instantiate_task(self):
        dagbag = get_dagbag()
        
        dag = get_dag_from_dagbag(dagbag, 'example_bash_operator')

        task = get_task_from_dag(dag, "run_this_last")

        self.assertIsInstance(task, BaseOperator)

        task_, task_instance = instantiate_task(task, datetime(2022, 2, 8))
    
    def test_instantiate_task_from_ti(self):
        dagbag = get_dagbag()
        dag = get_dag_from_dagbag(dagbag, 'example_bash_operator')
        task = get_task_from_dag(dag, "runme_1")
        ti = get_task_instances(TaskInstance.task_id == 'runme_1')[-1]

        self.assertIsInstance(task, BaseOperator)

        task, _ = instantiate_task_from_ti(task, ti)

        self.assertEqual(ti.task, task)