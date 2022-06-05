import json
import platform
import unittest
from datetime import datetime
from unittest.mock import Mock

from airflow.utils.state import State

from openlineage.common.dataset import Field

from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.metadata_extractor.airflow import MySqlToAvroExtractor

class TestMySqlToAvroMetaExtractor(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        task_id = 'test_task'
        operator_name = 'test_operator'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = 'test_dag'
        _task.avro_output_path = '/test/path.avro'

        mysql_conn = Mock()
        mysql_conn.host = 'localhost'
        mysql_conn.port = '3306'
        mysql_conn.get_uri = lambda: 'mysql://root:mysql@localhost:3306/sakila'

        task_instance = Mock()
        task_instance.state = State.SUCCESS
        task_instance.task_id = task_id
        task_instance.operator = operator_name
        task_instance.try_number = 1
        task_instance.execution_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.start_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.end_date = datetime(2022, 2, 10, 8, 0, 0)

        task = AirflowTask(task_id, operator_name, _task, task_instance)

        meta_extractor = MySqlToAvroExtractor(task)

        # Attach mock connection
        meta_extractor.conn = mysql_conn

        self.task = task
        self.task_instance = task_instance
        self.meta_extractor = meta_extractor

    def test_mysql_scheme(self):
        meta_extractor = self.meta_extractor
        
        self.assertEqual(meta_extractor._get_mysql_scheme(), 'mysql')

    def test_mysql_connection_uri(self):
        meta_extractor = self.meta_extractor
        database = 'sakila'
        table = 'test'

        self.assertEqual(meta_extractor._get_mysql_connection_uri(database, table), 'mysql://localhost:3306/sakila.test')

    def test_mysql_authority(self):
        meta_extractor = self.meta_extractor

        self.assertEqual(meta_extractor._get_mysql_authority(), 'localhost:3306')

    def test_fs_scheme(self):
        meta_extractor = self.meta_extractor
        
        self.assertEqual(meta_extractor._get_fs_scheme(), 'file')
    
    def test_fs_connection_uri(self):
        meta_extractor = self.meta_extractor

        scheme = meta_extractor._get_fs_scheme()
        node = platform.uname().node
        path = meta_extractor.operator.avro_output_path

        self.assertEqual(meta_extractor._get_fs_connection_uri(), f"{scheme}://{node}{path}")

    def test_fs_authority(self):
        meta_extractor = self.meta_extractor

        node = platform.uname().node

        self.assertEqual(meta_extractor._get_fs_authority(), node)

    def test_fs_name(self):
        meta_extractor = self.meta_extractor        

        self.assertEqual(meta_extractor._get_output_dataset_name(), '/test/path.avro')
    
    def test_avro_fields_extract(self):
        meta_extractor = self.meta_extractor        

        _temp = meta_extractor._get_avro_schema

        avro_schema_json = json.loads("""{
        "name": "example_schema",
        "namespace": "example.schema",
        "type": "record",
        "fields":[
            {"name": "user_id", "type": "string"},
            {"name": "user_name", "type": ["string", "null"]},
            {"name": "product_id","type": ["double","null"]},
            {"name": "product_price","type": ["long","null"]},
            {"name": "product_serial_number","type": ["int","null"]},
            {"name": "mark_for_delete", "type": ["boolean", "null"]},
            {"name": "created_date", "type":[{"logicalType": "timestamp-micros", "type": "long"},"null"]}
        ]
        }
        """)

        meta_extractor._get_avro_schema = lambda: json.dumps(avro_schema_json)

        fields = [
            Field(
                name='user_id',
                type='string'
            ),
            Field(
                name='user_name',
                type='string'
            ),
            Field(
                name='product_id',
                type='double'
            ),
            Field(
                name='product_price',
                type='long'
            ),
            Field(
                name='product_serial_number',
                type='int'
            ),
            Field(
                name='mark_for_delete',
                type='boolean'
            ),
            Field(
                name='created_date',
                type='timestamp-micros'
            )
        ]

        print(meta_extractor._get_avro_fields())

        self.assertEqual(meta_extractor._get_avro_fields(), fields)