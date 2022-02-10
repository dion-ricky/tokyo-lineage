from typing import List
from abc import ABC, abstractmethod

from tokyo_lineage.models.base_task import BaseTask
from tokyo_lineage.metadata_extractor.airflow_default import AIRFLOW_EXTRACTORS

class BaseExtractor(ABC):
    def __init__(self, custom_metadata_extractors):
        self.metadata_extractors = AIRFLOW_EXTRACTORS
        self.register_custom_metadata_extractors(custom_metadata_extractors)

    def register_custom_metadata_extractors(self, metadata_extractors):
        self.metadata_extractors += metadata_extractors

    def get_extractor(self, task: BaseTask):
        pass

    @abstractmethod
    def handle_job_run(self, job):
        pass

    def handle_jobs_run(self, jobs: List):
        for job in jobs:
            self.handle_job_run(job)

    @abstractmethod
    def handle_task_run(self, task):
        pass

    def handle_tasks_run(self, tasks: List):
        for task in tasks:
            self.handle_task_run(task)
    
    # blueprint
    @abstractmethod
    def report_task(self, task):
        pass