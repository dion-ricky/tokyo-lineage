from typing import Type, List
from abc import ABC, abstractmethod, abstractclassmethod

from tokyo_lineage.models.base import BaseTask, BaseJob

class BaseMetadataExtractor(ABC):
    def __init__(self, task: Type[BaseTask], job: Type[BaseJob]):
        self.task = task
        self.job = job

    @abstractclassmethod
    def get_operator_classnames(cls) -> List(str):
        pass
    
    def validate(self):
        assert (self.task.operator in self.get_operator_classnames())
    
    @abstractmethod
    def extract(self):
        pass