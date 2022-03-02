import json
import getpass
import platform
import unittest
from datetime import datetime
from unittest.mock import Mock

from avro import schema
from airflow.utils.state import State

from openlineage.common.dataset import Source, Field

from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.metadata_extractor.airflow.postgres_to_avro_extractor \
    import PostgresToAvroExtractor

class TestPgToAvroMetaExtractor(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        task_id = 'test_task'
        operator = 'test_operator'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = 'test_dag'

        task_instance = Mock()
        task_instance.state = State.SUCCESS
        task_instance.task_id = task_id
        task_instance.operator = operator
        task_instance.try_number = 1
        task_instance.execution_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.start_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.end_date = datetime(2022, 2, 10, 8, 0, 0)

        task = AirflowTask(task_id, operator, _task, task_instance)

        meta_extractor = PostgresToAvroExtractor(task)

        self.task = task
        self.task_instance = task_instance
        self.meta_extractor = meta_extractor

    def test_fs_scheme(self):
        meta_extractor = self.meta_extractor
        
        self.assertEqual(meta_extractor._get_fs_scheme(), 'files')
    
    def test_fs_connection_uri(self):
        meta_extractor = self.meta_extractor

        self.assertEqual(meta_extractor._get_fs_connection_uri(), platform.uname().node)

    def test_fs_authority(self):
        meta_extractor = self.meta_extractor

        user = getpass.getuser()
        node = platform.uname().node

        self.assertEqual(meta_extractor._get_fs_authority(), ':'.join([user, node]))

    def test_fs_name(self):
        meta_extractor = self.meta_extractor        

        self.assertEqual(meta_extractor._get_fs_name(), 'test_dag_temp_fs')
    
    def test_fs_source(self):
        meta_extractor = self.meta_extractor        
        
        fs_source = Source(
            scheme=meta_extractor._get_fs_scheme(),
            authority=meta_extractor._get_fs_authority(),
            connection_url=meta_extractor._get_fs_connection_uri()
        )

        user = getpass.getuser()
        node = platform.uname().node

        self.assertEqual(fs_source.scheme, 'files')
        self.assertEqual(fs_source.authority, ':'.join([user, node]))
        self.assertEqual(fs_source.connection_url, node)
    
    def test_avro_fields_extract(self):
        meta_extractor = self.meta_extractor        

        _temp = meta_extractor._get_avro_schema

        avro_schema_json = json.loads("""{
        "name": "example_schema",
        "namespace": "example.schema",
        "type": "record",
        "fields":[
            {"name": "user_id", "type": ["string", "null"]},
            {"name": "product_id","type": ["double","null"]},
            {"name": "product_price","type": ["long","null"]},
            {"name": "product_serial_number","type": ["int","null"]},
            {"name": "mark_for_delete", "type": ["boolean", "null"]},
            {"name": "created_date", "type":[{"logicalType": "timestamp-micros", "type": "long"},"null"]}
        ]
        }
        """)

        meta_extractor._get_avro_schema = lambda: schema.parse(json.dumps(avro_schema_json))

        fields = [
            Field(
                name='user_id',
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