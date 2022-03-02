from getpass import getuser
import platform
from typing import Type, List

from contextlib import closing
from typing import Optional, List
from urllib.parse import urlparse

from openlineage.airflow.utils import (
    get_normalized_postgres_connection_uri,
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
from openlineage.common.dataset import Source, Dataset

from tokyo_lineage.metadata_extractor.base import BaseMetadataExtractor
from tokyo_lineage.models.airflow_task import AirflowTask

_TABLE_SCHEMA = 0
_TABLE_NAME = 1
_COLUMN_NAME = 2
_ORDINAL_POSITION = 3
# Use 'udt_name' which is the underlying type of column
# (ex: int4, timestamp, varchar, etc)
_UDT_NAME = 4


# TODO: #10 Test PostgresToAvro meta extractor
class PostgresToAvroExtractor(BaseMetadataExtractor):
    default_schema = 'public'

    def __init__(self, task: Type[AirflowTask]):
        super().__init__(task)
    
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["PostgresToAvroOperator"]
    
    @property
    def operator(self):
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

        database = self.operator.database
        if not database:
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

        filesystem_source = Source(
            scheme=self._get_fs_scheme(),
            authority=self._get_fs_authority(),
            connection_url=self._get_fs_connection_uri()
        )
        
        outputs = [
            Dataset(
                name='local_temp_fs', # TODO: #12 Set dataset name specific to the executing DAG
                source=filesystem_source,
                fields=[] # TODO: #13 Extract fields from Avro schema file
            )
        ]

        outputs = [
            Dataset.from_table_schema(
                source=postgres_source,
                table_schema=out_table_schema,
                database_name=database
            ) for out_table_schema in self._get_table_schemas(
                sql_meta.out_tables
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
        return 'files'

    def _get_fs_connection_uri(self) -> str:
        return platform.uname().node
    
    def _get_fs_authority(self) -> str:
        user = getuser()
        node = platform.uname().node

        return ':'.join([user, node])
    
    def _get_pg_connection_uri(self):
        return get_normalized_postgres_connection_uri(self.conn)

    def _get_pg_scheme(self):
        return 'postgres'

    def _get_database(self) -> str:
        if self.conn.schema:
            return self.conn.schema
        else:
            parsed = urlparse(self.conn.get_uri())
            return f'{parsed.path}'

    def _get_pg_authority(self) -> str:
        if self.conn.host and self.conn.port:
            return f'{self.conn.host}:{self.conn.port}'
        else:
            parsed = urlparse(self.conn.get_uri())
            return f'{parsed.hostname}:{parsed.port}'

    def _conn_id(self):
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

    def _get_hook(self):
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