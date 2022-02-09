from datetime import datetime

from airflow.operators import BaseOperator
from airflow.models import DAG as AIRFLOW_DAG
from airflow.utils.db import create_session
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance

def get_dagbag():
    from airflow.models.dagbag import DagBag # Prevent circular import
    return DagBag()

def get_dagruns(*filters):
    dagruns = None

    with create_session() as session:
        q = session.query(DagRun).filter(*filters)
        dagruns = q.all()
    
    return dagruns

def get_task_instances(*filters):
    taskinstances = None

    with create_session() as session:
        q = session.query(TaskInstance).filter(*filters)
        taskinstances = q.all()
    
    return taskinstances

def get_task_instances_from_dagrun(dagrun: DagRun, state=None):
    with create_session() as session:
        return dagrun.get_task_instances(state, session)

def get_task_instance_from_dagrun(dagrun: DagRun, task_id: str):
    with create_session() as session:
        return dagrun.get_task_instance(task_id, session)

def get_dag_from_dagbag(dagbag, dag_id: str):
    return dagbag.get_dag(dag_id)

def get_task_from_dag(dag: AIRFLOW_DAG, task_id: str):
    return dag.get_task(task_id)

def instantiate_task(task: BaseOperator, execution_date: datetime):
    task_instance = TaskInstance(task=task, execution_date=execution_date)
    
    task_instance.refresh_from_db()
    task_instance.render_templates()

    return task, task_instance

def instantiate_task_from_ti(task: BaseOperator, task_instance: TaskInstance):
    task_instance.task = task
    task_instance.refresh_from_db()
    task_instance.render_templates()

    return task, task_instance
