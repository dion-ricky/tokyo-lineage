from typing import Optional, Type, List

from airflow.models import BaseOperator, DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.utils import (
    DagUtils,
    get_custom_facets,
    new_lineage_run_id
)

from tokyo_lineage.extractor.base import BaseExtractor
from tokyo_lineage.metadata_extractor.base import BaseMetadataExtractor
from tokyo_lineage.models.base import BaseJob, BaseTask
from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.models.airflow_dag import AirflowDag
from tokyo_lineage.utils.airflow import (
    get_dagbag,
    get_task_instances_from_dagrun,
    get_dag_from_dagbag,
    get_task_from_dag,
    instantiate_task_from_ti,
    get_location
)

class AirflowExtractor(BaseExtractor):
    def __init__(self):
        super(AirflowExtractor, self).__init__()

    def get_extractor(
        self,
        task: Type[BaseTask]
    ) -> Type[BaseMetadataExtractor]:
        extractor = super().get_extractor(task)

        return extractor if extractor is not None else AirflowMetaExtractor(task)
    
    def handle_jobs_from_dagrun(self, _jobs: List[DagRun]):
        jobs = []
        dagbag = get_dagbag()

        for job in _jobs:
            dag = get_dag_from_dagbag(dagbag, job.dag_id)
            jobs.append(AirflowDag(dag.dag_id, dag, job))

        return self.handle_jobs_run(jobs)

    def handle_job_run(self, job: AirflowDag):
        task_instances = get_task_instances_from_dagrun(job.dagrun)

        if len(task_instances) < 1:
            return

        for task_instance in task_instances:
            _task = get_task_from_dag(job.dag, task_instance.task_id)
            _task, _ = instantiate_task_from_ti(_task, task_instance)

            self._handle_task_run(_task, task_instance, job)
    
    def _handle_task_run(
        self,
        task: Type[BaseOperator],
        task_instance: TaskInstance,
        job: Type[BaseJob]
    ):
        task_id = task.task_id
        operator = task_instance.operator
        _task = AirflowTask(task_id, operator, task, task_instance)
        self.handle_task_run(_task, job)

    def handle_task_run(self, task: Type[BaseTask], job: Type[BaseJob]):
        # Register start_task
        self._register_task_start(task, job)

        # Register finish_task or fail_task
        if task.task_instance.state == State.SUCCESS:
            self._register_task_finish(task)
        else:
            self._register_task_fail(task)
    
    def _register_task_start(self, task: AirflowTask, job: AirflowDag):
        _task = task.task
        task_instance = task.task_instance
        dag = job.dag
        dagrun = job.dagrun

        meta_extractor = self.get_extractor(task)
        task_metadata = meta_extractor.extract()

        run_id = new_lineage_run_id(dag.dag_id, task.task_id)
        job_name = f'{dag.dag_id}.{task.task_id}'
        job_description = dag.description
        event_time = DagUtils.to_iso_8601(task_instance.start_date)
        parent_run_id = dagrun.run_id
        code_location = get_location(dag.full_filepath)
        nominal_start_time = DagUtils.to_iso_8601(task_instance.start_date)
        nominal_end_time = DagUtils.to_iso_8601(task_instance.end_date)
        run_facets = {**task_metadata.run_facets, **get_custom_facets(_task, dagrun.external_trigger)}

        self.register_task_start(
            run_id,
            job_name,
            job_description,
            event_time,
            parent_run_id,
            code_location,
            nominal_start_time,
            nominal_end_time,
            task_metadata,
            run_facets
        )

class AirflowMetaExtractor(BaseMetadataExtractor):
    def __init__(self, task):
        super(AirflowMetaExtractor, self).__init__(task)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['*']
    
    def validate(self):
        assert True
    
    def extract(self) -> Optional[TaskMetadata]:
        return TaskMetadata(
            name=f'{self.task.task.dag_id}.{self.task.task_id}'
        )