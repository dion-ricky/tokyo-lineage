import unittest
from datetime import datetime
from unittest.mock import Mock

from airflow.utils.state import State

from openlineage.common.models import DbTableName

from tokyo_lineage.models.airflow_task import AirflowTask
from tokyo_lineage.metadata_extractor.airflow import BigQueryExtractor


class TestBigQueryMetaExtractor(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        task_id = 'test_task'
        operator_name = 'test_operator'

        _task = Mock()
        _task.task_id = task_id
        _task.dag_id = 'test_dag'
        _task.avro_output_path = '/test/path.avro'
        _task.destination_dataset_table = 'dataset.table'

        bq_conn = Mock()
        bq_conn.host = 'localhost'
        bq_conn.port = '3306'
        bq_conn.get_uri = lambda: 'mysql://root:mysql@localhost:3306/sakila'
        bq_conn.get_extra = lambda: '{"extra__google_cloud_platform__project": "dionricky-personal", "extra__google_cloud_platform__key_path": "/opt/credentials/tokyo-skripsi.json"}'

        task_instance = Mock()
        task_instance.state = State.SUCCESS
        task_instance.task_id = task_id
        task_instance.operator = operator_name
        task_instance.try_number = 1
        task_instance.execution_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.start_date = datetime(2022, 2, 10, 7, 0, 0)
        task_instance.end_date = datetime(2022, 2, 10, 8, 0, 0)

        task = AirflowTask(task_id, operator_name, _task, task_instance)

        meta_extractor = BigQueryExtractor(task)

        # Attach mock connection
        meta_extractor.conn = bq_conn
        meta_extractor._get_bq_connection = lambda: bq_conn

        self.task = task
        self.task_instance = task_instance
        self.meta_extractor = meta_extractor
    
    def test_get_database(self):
        meta_extractor = self.meta_extractor

        self.assertEqual(meta_extractor._get_database(), 'dionricky-personal')

    # def test_get_table_schemas(self):
    #     tables = [
    #         DbTableName('dionricky-personal.alberta.customer'),
    #         DbTableName('dionricky-personal.alberta.address'),
    #         DbTableName('dionricky-personal.queensland.customer'),
    #         DbTableName('dionricky-personal.queensland.address')
    #     ]

    #     compare_to = [
    #         DbTableName('dionricky-personal.alberta.customer'),
    #         DbTableName('dionricky-personal.alberta.address'),
    #         DbTableName('dionricky-personal.queensland.customer'),
    #         DbTableName('dionricky-personal.queensland.address')
    #     ]

    #     print(self.meta_extractor._get_table_schemas(tables))

    #     table_schemas = [i for i in self.meta_extractor._get_table_schemas(tables)]

    #     self.assertEqual(len(table_schemas), 4)
    #     self.assertListEqual(table_schemas, compare_to)
    
    def test_bq_connection_uri(self):
        meta_extractor = self.meta_extractor

        _, dataset, table = 'dionricky-personal.alberta.customer'.split('.')
        conn_uri1 = meta_extractor._get_bq_connection_uri(dataset, table)
        self.assertEqual(conn_uri1, "bigquery:dionricky-personal.alberta.customer")
        
        _, dataset, table = 'dionricky-personal.queensland.customer'.split('.')
        conn_uri1 = meta_extractor._get_bq_connection_uri(dataset, table)
        self.assertEqual(conn_uri1, "bigquery:dionricky-personal.queensland.customer")
    
    def test_output_dataset_name(self):
        meta_extractor = self.meta_extractor
        output_dataset_name = meta_extractor._get_output_dataset_name()
        self.assertEqual(output_dataset_name, 'dionricky-personal.dataset.table')