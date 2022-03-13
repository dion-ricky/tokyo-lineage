import json
import platform
from typing import Type, List, Optional

from airflow.models import BaseOperator

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.common.dataset import Source, Dataset

from tokyo_lineage.models.base import BaseTask
from tokyo_lineage.metadata_extractor.base import BaseMetadataExtractor

from tokyo_lineage.utils.airflow import get_connection

EXPORTER_OPERATOR_CLASSNAMES = ["PostgresToAvroOperator", "PostgresToJsonOperator"]

class FileToGcsExtractor(BaseMetadataExtractor):
    def __init__(self, task: Type[BaseTask]):
        super().__init__(task)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["FileToGoogleCloudStorageOperator"]
    
    @property
    def operator(self) -> Type[BaseOperator]:
        return self.task.task

    def extract(self) -> Optional[TaskMetadata]:
        filesystem_source = Source(
            scheme=self._get_fs_scheme(),
            authority=self._get_fs_authority(),
            connection_url=self._get_fs_connection_uri()
        )

        inputs = [
            Dataset(
                name=self._get_input_dataset_name(),
                source=filesystem_source
            )
        ]

        output_gcs_source = Source(
            scheme=self._get_gcs_scheme(),
            authority=self._get_gcs_authority(),
            connection_url=self._get_gcs_connection_uri()
        )

        outputs = [
            Dataset(
                name=self._get_output_dataset_name(),
                source=output_gcs_source
            )
        ]

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs]
        )

    def _get_gcs_connection(self):
        conn = get_connection(self.operator.google_cloud_storage_conn_id)
        return conn

    def _get_gcs_scheme(self) -> str:
        return 'gs'
    
    def _get_gcs_connection_uri(self) -> str:
        conn = self._get_gcs_connection()
        extras = json.loads(conn.get_extra())
        return f"{self._get_gcs_scheme()}://{extras['extra__google_cloud_platform__project']}/{self.operator.bucket}"

    def _get_gcs_authority(self) -> str:
        conn = self._get_gcs_connection()
        extras = json.loads(conn.get_extra())
        return f"{extras['extra__google_cloud_platform__project']}"

    def _get_output_dataset_name(self) -> str:
        bucket = self.operator.bucket
        task_id = self.operator.task_id
        return f"{bucket}.{task_id}"

    def _get_fs_scheme(self) -> str:
        return 'file'

    def _get_fs_connection_uri(self) -> str:
        scheme = self._get_fs_scheme()
        host = platform.uname().node
        return f'{scheme}://{host}'

    def _get_fs_authority(self) -> str:
        return platform.uname().node

    def _get_input_dataset_name(self) -> str:
        exporter = self._get_nearest_exporter_upstream()
        return '_'.join([exporter.dag_id, exporter.task_id])

    def _get_nearest_exporter_upstream(self) -> Type[BaseOperator]:
        operator = self.operator
        
        upstream_operators: List[BaseOperator] = operator.upstream_list[::-1]

        for operator in upstream_operators:
            if operator.__class__.__name__ in EXPORTER_OPERATOR_CLASSNAMES:
                return operator