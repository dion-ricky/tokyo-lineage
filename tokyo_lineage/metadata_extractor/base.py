from typing import Optional, List

from openlineage.airflow.extractors.base import TaskMetadata

from abc import ABC, abstractmethod, abstractclassmethod

class BaseMetadataExtractor(ABC):
    def __init__(self, operator):
        self.operator = operator

    @abstractclassmethod
    def get_operator_classnames(cls) -> List(str):
        pass
    
    def validate(self):
        assert (self.operator.__class__.__name__ in self.get_operator_classnames())
    
    @abstractmethod
    def extract(self) -> Optional[TaskMetadata]:
        pass