import json
from typing import Type, List, Any, Optional

from contextlib import closing
from airflow.models import BaseOperator

from openlineage.client.facet import SqlJobFacet
from openlineage.common.dataset import Source, Dataset, Field
from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.utils import safe_import_airflow
from openlineage.common.models import (
    DbTableName,
    DbTableSchema,
    DbColumn
)

from tokyo_lineage.utils.parser import Parser
from tokyo_lineage.models.base import BaseTask
from tokyo_lineage.metadata_extractor.base import BaseMetadataExtractor
from tokyo_lineage.utils.airflow import get_connection
from tokyo_lineage.utils.dataset_naming_helper import (
    bq_scheme,
    bq_authority,
    bq_connection_uri
)


_TABLE_SCHEMA = 0
_TABLE_NAME = 1
_COLUMN_NAME = 2
_ORDINAL_POSITION = 3
_DATA_TYPE = 4


class BigQueryExtractor(BaseMetadataExtractor):
    def __init__(self, task: Type[BaseTask]):
        super().__init__(task)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["BigQueryOperator"]

    @property
    def operator(self) -> Type[BaseOperator]:
        return self.task.task

    def extract(self) -> TaskMetadata:
        parser = Parser(self.operator.sql)
        parsed_tables = [DbTableName(table) for table in parser.tables]

        database = self._get_database()

        def source(dataset, table):
            return Source(
                scheme=self._get_bq_scheme(),
                authority=self._get_bq_authority(),
                connection_url=self._get_bq_connection_uri(dataset, table)
            )

        inputs = [
            Dataset.from_table(
                source=source(table_schema.schema_name, table_schema.table_name.name),
                table_name=table_schema.table_name.name,
                schema_name=table_schema.schema_name,
                database_name=database
            ) for table_schema in self._get_table_schemas(
                parsed_tables
            )
        ]

        # Extract fields from source
        self._extract_table_fields(inputs)

        # Output source
        _, dataset, table = self._safe_split_dataset_name(
                                self._get_output_dataset_name())

        bq_source_out = Source(
            scheme=self._get_bq_scheme(),
            authority=self._get_bq_authority(),
            connection_url=self._get_bq_connection_uri(dataset, table)
        )

        outputs = [
            Dataset(
                name=self._get_output_dataset_name(),
                source=bq_source_out
            )
        ]

        # Extract fields from destination table
        self._extract_table_fields(outputs)

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            job_facets={
                'sql': SqlJobFacet(self.operator.sql)
            }
        )

    def _get_bq_connection_uri(self, dataset, table) -> str:
        conn = self._get_bq_connection()
        return bq_connection_uri(conn, dataset, table)

    def _get_bq_scheme(self) -> str:
        return bq_scheme()

    def _get_bq_authority(self) -> str:
        return bq_authority(self._get_bq_connection())

    def _get_output_dataset_name(self) -> str:
        database = self._get_database()
        dataset_table = self.operator.destination_dataset_table
        return f"{database}.{dataset_table}"

    def _get_bq_connection(self):
        return get_connection(self._conn_id())

    def _conn_id(self) -> str:
        return self.operator.bigquery_conn_id

    def _get_database(self):
        bq_conn = self._get_bq_connection()
        extras = json.loads(bq_conn.get_extra())

        return extras['extra__google_cloud_platform__project']

    def _safe_split_dataset_name(self, dataset_name) -> List[str]:
        splitted = dataset_name.split('.')

        if len(splitted) >= 3:
            return splitted[0:3]
        elif len(splitted) < 3: 
            filler = [''] * (3 - len(splitted))
            return filler + splitted
    
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
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                for table_name in table_names:
                    dataset_name = table_name.schema
                    _table_name = table_name.name
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
                        table_key: str = f"{table_schema_name}.{_table_name}"
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