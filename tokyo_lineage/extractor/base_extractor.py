from typing import List
from abc import ABC, abstractmethod

class BaseExtractor(ABC):
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