import json
import posixpath
from typing import Any, Type, List, Optional
from contextlib import closing

from airflow.models import BaseOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.common.dataset import Source, Dataset, Field
from openlineage.airflow.utils import safe_import_airflow
from openlineage.common.models import (
    DbTableName,
    DbTableSchema,
    DbColumn
)

from tokyo_lineage.metadata_extractor.base import BaseMetadataExtractor
from tokyo_lineage.models.base import BaseTask

from tokyo_lineage.utils.airflow import get_connection, instantiate_task
from tokyo_lineage.utils.dataset_naming_helper import (
    # GCS dataset
    gcs_scheme,
    gcs_authority,
    gcs_connection_uri,
    # BigQuery dataset
    bq_scheme,
    bq_authority,
    bq_connection_uri
)


UPLOADER_OPERATOR_CLASSNAMES = ["FileToGoogleCloudStorageOperator"]
_TABLE_SCHEMA = 0
_TABLE_NAME = 1
_COLUMN_NAME = 2
_ORDINAL_POSITION = 3
_DATA_TYPE = 4


class GcsToBigQueryExtractor(BaseMetadataExtractor):
    def __init__(self, task: Type[BaseTask]):
        super().__init__(task)
    
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["GoogleCloudStorageToBigQueryOperator"]
    
    @property
    def operator(self) -> Type[BaseOperator]:
        return self.task.task
    
    def extract(self) -> Optional[TaskMetadata]:
        def gcs_source(bucket, path):
            return Source(
                scheme=self._get_gcs_scheme(),
                authority=self._get_gcs_authority(),
                connection_url=self._get_gcs_connection_uri(bucket, path)
            )

        # input_dataset_name is bucket name
        inputs = [
            Dataset(
                name=self._get_input_dataset_name(),
                source=gcs_source(self.operator.bucket, source_path)
            ) for source_path in self.operator.source_objects
        ]

        # output_source generated from bigquery_conn_id
        output_source = Source(
            scheme=self._get_bq_scheme(),
            authority=self._get_bq_authority(),
            connection_url=self._get_bq_connection_uri()
        )

        # output_dataset_name is dataset + table name
        outputs = [
            Dataset(
                name=self._get_output_dataset_name(),
                source=output_source
            )
        ]

        # Extract fields from destination table
        self._extract_table_fields(outputs)

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs]
        )

    def _get_gcs_connection(self):
        conn = get_connection(self.operator.google_cloud_storage_conn_id)
        return conn

    def _get_gcs_scheme(self) -> str:
        return gcs_scheme()
    
    def _get_gcs_authority(self) -> str:
        return gcs_authority(self.operator.bucket)

    def _get_gcs_connection_uri(self, bucket, path) -> str:
        return gcs_connection_uri(bucket, path)

    def _get_project_dataset_table(self):
        project_dataset_table = self.operator.destination_project_dataset_table
        filler = [None] * (3-len(project_dataset_table.split('.')))
        splitted = project_dataset_table.split('.')
        project, dataset, table = filler + splitted

        return project, dataset, table

    def _get_bq_connection(self):
        conn = get_connection(self.operator.bigquery_conn_id)
        return conn
    
    def _get_bq_scheme(self) -> str:
        return bq_scheme()
    
    def _get_bq_authority(self) -> str:
        conn = self._get_bq_connection()
        return bq_authority(conn)

    def _get_bq_connection_uri(self) -> str:
        _, dataset, table = self._get_project_dataset_table()
        conn = self._get_bq_connection()
        return bq_connection_uri(conn, dataset, table)
    
    def _get_output_dataset_name(self) -> str:
        project, dataset, table = self._get_project_dataset_table()
        
        if project is None:
            conn = self._get_bq_connection()
            extras = json.loads(conn.get_extra())
            project = extras['extra__google_cloud_platform__project']

        return f"{project}.{dataset}.{table}"

    def _get_input_dataset_name(self) -> str:
        uploader = self._get_nearest_uploader_upstream()
        execution_date = self.task.task_instance.execution_date

        uploader, _ = instantiate_task(uploader, execution_date)

        # make sure path starts from root
        dataset_name = posixpath.join("/", uploader.dst)

        return dataset_name

    def _get_nearest_uploader_upstream(self) -> Type[BaseOperator]:
        operator = self.operator
        
        upstream_operators: List[BaseOperator] = operator.upstream_list[::-1]

        for operator in upstream_operators:
            if operator.__class__.__name__ in UPLOADER_OPERATOR_CLASSNAMES:
                return operator
    
    def _information_schema_query(self, dataset_name: str, table_name: str):
        return f"""
        SELECT
            table_schema,
            table_name,
            column_name,
            ordinal_position,
            data_type
        FROM {dataset_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name='{table_name}';
        """
    
    def _get_hook(self) -> Any:
        BigQueryHook = safe_import_airflow(
            airflow_1_path="airflow.contrib.hooks.bigquery_hook.BigQueryHook",
            airflow_2_path="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook"
        )
        return BigQueryHook(
            bigquery_conn_id=self.operator.bigquery_conn_id,
            use_legacy_sql=False
        )
    
    def _get_table_schemas(
        self, table_names: List[DbTableName]
    ) -> List[DbTableSchema]:
        if not table_names:
            return []
        
        schemas_by_table = {}

        hook = self._get_hook()

        for table_name in table_names:
            dataset_name = table_name.schema
            _table_name = table_name.name

            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                        try:
                            cursor.execute(
                                self._information_schema_query(
                                    dataset_name, _table_name
                                )
                            )
                        except Exception as e:
                            self.log.error(str(e))
                            continue

                        for row in cursor.fetchall():
                            table_schema_name: str = row[_TABLE_SCHEMA]
                            table_name: DbTableName = DbTableName(row[_TABLE_NAME])
                            table_column: DbColumn = DbColumn(
                                name=row[_COLUMN_NAME],
                                type=row[_DATA_TYPE],
                                ordinal_position=row[_ORDINAL_POSITION]
                            )

                            # Attempt to get table schema
                            table_key: str = f"{table_schema_name}.{table_name}"
                            table_schema: Optional[DbTableSchema] = schemas_by_table.get(table_key)

                            if table_schema:
                                # Add column to existing table schema.
                                schemas_by_table[table_key].columns.append(table_column)
                            else:
                                # Create new table schema with column.
                                schemas_by_table[table_key] = DbTableSchema(
                                    schema_name=table_schema_name,
                                    table_name=table_name,
                                    columns=[table_column]
                                )
        
        return list(schemas_by_table.values())
    
    def _extract_table_fields(
        self,
        datasets: List[Dataset]
    ) -> List[Dataset]:
        for dataset in datasets:
            table_name = DbTableName(dataset.name)
            table_schema: DbTableSchema = self._get_table_schemas([table_name])[0]
            dataset.fields = [
                Field.from_column(column) for column in sorted(
                    table_schema.columns, key=lambda x: x.ordinal_position
                )
            ]
        return datasets