from typing import Optional, Type, List, Dict
from abc import ABC, abstractmethod

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.facets import BaseFacet

from tokyo_lineage.models.base import BaseTask
from tokyo_lineage.metadata_extractor.base import BaseMetadataExtractor

_ADAPTER = OpenLineageAdapter()

class BaseExtractor(ABC):
    def __init__(
        self,
        custom_metadata_extractors: Optional[List[Type[BaseMetadataExtractor]]] = None
    ):
        self.metadata_extractors = []

        if custom_metadata_extractors:
            self.register_custom_metadata_extractors(custom_metadata_extractors)

    def get_extractor(
        self,
        task: Type[BaseTask]
    ) -> Type[BaseMetadataExtractor]:
        for meta_extractor in self.metadata_extractors:
            if task.operator_name in meta_extractor.get_operator_classnames():
                return meta_extractor(task)
        
        return None

    @abstractmethod
    def handle_job_run(self, job):
        pass

    def handle_jobs_run(self, jobs: List):
        for job in jobs:
            self.handle_job_run(job)

    @abstractmethod
    def handle_task_run(self, task, job):
        pass

    def handle_tasks_run(self, job, tasks: List):
        for task in tasks:
            self.handle_task_run(task, job)
    
    def register_task_start(
        self,
        run_id: str,
        job_name: str,
        job_description: str,
        event_time: str,
        parent_run_id: Optional[str],
        code_location: Optional[str],
        nominal_start_time: str,
        nominal_end_time: str,
        task: Optional[TaskMetadata],
        run_facets: Optional[Dict[str, Type[BaseFacet]]] = None
    ) -> str:
        return _ADAPTER.start_task(
            run_id,
            job_name,
            job_description,
            event_time,
            parent_run_id,
            code_location,
            nominal_start_time,
            nominal_end_time,
            task,
            run_facets
        )
    
    def register_task_finish(
        self,
        task_run_id: str,
        job_name: str,
        end_time: str,
        task_metadata: TaskMetadata
    ):
        _ADAPTER.complete_task(
            task_run_id,
            job_name,
            end_time,
            task_metadata
        )

    def register_task_fail(
        self,
        task_run_id: str,
        job_name: str,
        end_time: str,
        task_metadata: TaskMetadata
    ):
        _ADAPTER.fail_task(
            task_run_id,
            job_name,
            end_time,
            task_metadata
        )

    def register_custom_metadata_extractors(self, metadata_extractors: List[Type[BaseMetadataExtractor]]):
        self.metadata_extractors += metadata_extractors