import imp
import json
from typing import Type, List, Any

from avro import schema as avro_schema
from contextlib import closing
from typing import Optional, List
from urllib.parse import urlparse

from airflow.models import BaseOperator

from openlineage.airflow.utils import (
    get_connection, safe_import_airflow
)
from openlineage.airflow.extractors.base import TaskMetadata

from openlineage.client.facet import SqlJobFacet
from openlineage.common.models import (
    DbTableName,
    DbTableSchema,
    DbColumn
)
from openlineage.common.sql import SqlMeta, SqlParser
from openlineage.common.dataset import Source, Dataset, Field

from tokyo_lineage.facets.annotation import Annotation
from tokyo_lineage.metadata_extractor.base import BaseMetadataExtractor
from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.utils.dataset_naming_helper import (
    # Local filesystem dataset
    fs_scheme,
    fs_authority,
    fs_connection_uri,
    # Postgres dataset
    pg_scheme,
    pg_authority,
    pg_connection_uri
)

_TABLE_SCHEMA = 0
_TABLE_NAME = 1
_COLUMN_NAME = 2
_ORDINAL_POSITION = 3
# Use 'udt_name' which is the underlying type of column
# (ex: int4, timestamp, varchar, etc)
_UDT_NAME = 4


class PostgresToAvroExtractor(BaseMetadataExtractor):
    default_schema = 'public'

    def __init__(self, task: Type[AirflowTask]):
        super().__init__(task)
    
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["PostgresToAvroOperator"]
    
    @property
    def operator(self) -> Type[BaseOperator]:
        return self.task.task

    def extract(self) -> TaskMetadata:
        # (1) Parse sql statement to obtain input / output tables.
        sql_meta: SqlMeta = SqlParser.parse(self.operator.sql, self.default_schema)

        # (2) Get database connection
        self.conn = get_connection(self._conn_id())

        # (3) Default all inputs / outputs to current connection.
        # NOTE: We'll want to look into adding support for the `database`
        # property that is used to override the one defined in the connection.
        postgres_source = Source(
            scheme=self._get_pg_scheme(),
            authority=self._get_pg_authority(),
            connection_url=self._get_pg_connection_uri()
        )

        database = self._get_database()

        # (4) Map input / output tables to dataset objects with source set
        # as the current connection. We need to also fetch the schema for the
        # input tables to format the dataset name as:
        # {schema_name}.{table_name}
        inputs = [
            Dataset.from_table(
                source=postgres_source,
                table_name=in_table_schema.table_name.name,
                schema_name=in_table_schema.schema_name,
                database_name=database
            ) for in_table_schema in self._get_table_schemas(
                sql_meta.in_tables
            )
        ]

        # Extracting annotation from source
        if hasattr(self.operator, 'tokyolineage_params'):
            try:
                if self.operator.tokyolineage_params['is_annotation_available']:
                    self._extract_annotations(inputs)
            except KeyError as e:
                pass

        filesystem_source = Source(
            scheme=self._get_fs_scheme(),
            authority=self._get_fs_authority(),
            connection_url=self._get_fs_connection_uri()
        )
        
        outputs = [
            Dataset(
                name=self._get_output_dataset_name(),
                source=filesystem_source,
                fields=self._get_avro_fields()
            )
        ]

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            job_facets={
                'sql': SqlJobFacet(self.operator.sql)
            }
        )
    
    def _get_fs_scheme(self) -> str:
        return fs_scheme()

    def _get_fs_connection_uri(self) -> str:
        return fs_connection_uri(self.operator.avro_output_path)
    
    def _get_fs_authority(self) -> str:
        return fs_authority()
    
    def _get_output_dataset_name(self) -> str:
        return self.operator.avro_output_path

    def _get_avro_fields(self) -> List[Field]:
        schema = self._get_avro_schema()
        fields = schema.props['fields']

        fields = [
            Field(
                name=f.name,
                type=self._filter_avro_field_type(f.type)
            ) for f in fields
        ]

        return fields
    
    def _get_avro_schema(self, schema: str = None):
        if not schema:
            avro_schema_json = json.loads(open(self.operator.avro_schema_path, 'r+').read())
        else:
            avro_schema_json = json.loads(schema)
        
        return avro_schema.parse(json.dumps(avro_schema_json))

    def _filter_avro_field_type(self, types) -> str:
        if not hasattr(types, '_schemas'):
            try:
                return types.logical_type
            except:
                return types.type

        for f in types._schemas:
            if f.type != 'null':
                try:
                    return f.logical_type
                except:
                    return f.type

    def _get_pg_connection_uri(self) -> str:
        return pg_connection_uri(self.conn)

    def _get_pg_scheme(self) -> str:
        return pg_scheme()

    def _get_database(self) -> str:
        if self.conn.schema:
            return self.conn.schema
        else:
            parsed = urlparse(self.conn.get_uri())
            return f'{parsed.path}'

    def _get_pg_authority(self) -> str:
        return pg_authority(self.conn)

    def _conn_id(self) -> str:
        return self.operator.postgres_conn_id

    def _information_schema_query(self, table_names: str) -> str:
        return f"""
        SELECT table_schema,
        table_name,
        column_name,
        ordinal_position,
        udt_name
        FROM information_schema.columns
        WHERE table_name IN ({table_names});
        """

    def _get_hook(self) -> Any:
        PostgresHook = safe_import_airflow(
            airflow_1_path="airflow.hooks.postgres_hook.PostgresHook",
            airflow_2_path="airflow.providers.postgres.hooks.postgres.PostgresHook"
        )
        return PostgresHook(
            postgres_conn_id=self.operator.postgres_conn_id,
            schema=self.conn.schema
        )

    def _get_table_schemas(
            self, table_names: List[DbTableName]
    ) -> List[DbTableSchema]:
        # Avoid querying postgres by returning an empty array
        # if no table names have been provided.
        if not table_names:
            return []

        # Keeps track of the schema by table.
        schemas_by_table = {}

        hook = self._get_hook()
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                table_names_as_str = ",".join(map(
                    lambda name: f"'{name.name}'", table_names
                ))
                cursor.execute(
                    self._information_schema_query(table_names_as_str)
                )
                for row in cursor.fetchall():
                    table_schema_name: str = row[_TABLE_SCHEMA]
                    table_name: DbTableName = DbTableName(row[_TABLE_NAME])
                    table_column: DbColumn = DbColumn(
                        name=row[_COLUMN_NAME],
                        type=row[_UDT_NAME],
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

    def _extract_annotations(
        self,
        datasets: List[Dataset]
    ) -> List[Dataset]:
        self.log.info("Extracting annotation from postgres")
        for dataset in datasets:
            _, _, table = dataset.name.split('.')
            self.log.info("Getting table comment for table {}".format(table))
            annotation: dict = json.loads(self._get_table_comment(table))

            self.log.info("Table {} has this annotation: ".format(table))
            self.log.info(json.dumps(annotation))

            annotation_facet = Annotation()
            
            # Row annotation
            annotation_facet.row_annotation = annotation['rows'] \
                if 'rows' in annotation else None
            
            # Columns annotation
            annotation_facet.column_annotation = annotation['columns'] \
                if 'columns' in annotation else None
            
            # Dataset annotation
            dataset_annotation = annotation.copy()
            dataset_annotation.pop('rows', None)
            dataset_annotation.pop('columns', None)
            annotation_facet.dataset_annotation = dataset_annotation
            
            dataset.custom_facets.update(annotation_facet)
        
        return datasets

    def _table_comment_query(self, table_name: str) -> str:
        return f"""
        SELECT obj_description('{table_name}'::regclass);
        """

    def _get_table_comment(self, table_name: str) -> str:
        hook = self._get_pg_hook()
        
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(
                    self._table_comment_query(table_name)
                )
                row = cursor.fetchone()

                return row[0]