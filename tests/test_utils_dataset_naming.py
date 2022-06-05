import unittest
import platform
from unittest.mock import Mock

from tokyo_lineage.utils.dataset_naming_helper import (
    fs_scheme,
    fs_authority,
    fs_connection_uri,

    pg_scheme,
    pg_authority,
    pg_connection_uri,

    mysql_scheme,
    mysql_authority,
    mysql_connection_uri,

    mongo_scheme,
    mongo_authority,
    mongo_connection_uri,

    gcs_scheme,
    gcs_authority,
    gcs_connection_uri,

    bq_scheme,
    bq_authority,
    bq_connection_uri
)

class TestUtilsDatasetNaming(unittest.TestCase):
    
    def test_fs_scheme(self):
        self.assertEqual(fs_scheme(), 'file')
    
    def test_fs_authority(self):
        self.assertEqual(fs_authority(), platform.uname().node)
    
    def test_fs_connection_uri(self):
        path = '/opt/tmp'

        scheme = 'file'
        host = platform.uname().node

        self.assertEqual(fs_connection_uri(path), f'{scheme}://{host}{path}')

    def test_pg_scheme(self):
        self.assertEqual(pg_scheme(), 'postgres')
    
    def test_pg_authority(self):
        conn = Mock()
        conn.host = 'localhost'
        conn.port = '5432'
        
        self.assertEqual(pg_authority(conn), f'{conn.host}:{conn.port}')

    def test_pg_connection_uri(self):
        conn = Mock()
        conn.get_uri = lambda: 'postgres://postgres:postgres@localhost:5432/test_db'
        database = 'test_db'
        schema = 'public'
        table = 'test'
        
        self.assertEqual(pg_connection_uri(conn, database, schema, table), 'postgres://localhost:5432/test_db.public.test')
    
    def test_mysql_scheme(self):
        self.assertEqual(mysql_scheme(), 'mysql')
    
    def test_mysql_authority(self):
        conn = Mock()
        conn.host = 'localhost'
        conn.port = '3306'

        self.assertEqual(mysql_authority(conn), f'{conn.host}:{conn.port}')

    def test_mysql_connection_uri(self):
        conn = Mock()
        conn.get_uri = lambda: 'mysql://root:mysql@localhost:3306/test'
        database = 'test_db'
        table = 'test'

        self.assertEqual(mysql_connection_uri(conn, database, table), 'mysql://localhost:3306/test_db.test')

    def test_mongo_scheme(self):
        self.assertEqual(mongo_scheme(), 'mongo')
    
    def test_mongo_authority(self):
        conn = Mock()
        conn.host = 'localhost'
        conn.port = '27017'

        self.assertEqual(mongo_authority(conn), f'{conn.host}:{conn.port}')
    
    def test_mongo_connection_uri(self):
        conn = Mock()
        conn.get_uri = lambda: 'mongodb://mongo:mongo@localhost:27017/'
        database = 'test_db'
        collection = 'test'

        self.assertEqual(mongo_connection_uri(conn, database, collection), 'mongodb://localhost:27017/test_db.test')
    
    def test_gcs_scheme(self):
        self.assertEqual(gcs_scheme(), 'gs')
    
    def test_gcs_authority(self):
        bucket = 'test_bucket'

        self.assertEqual(gcs_authority(bucket), bucket)
    
    def test_gcs_connection_uri(self):
        bucket = 'test_bucket'
        path = '/test/path'

        self.assertEqual(gcs_connection_uri(bucket, path), f'gs://{bucket}{path}')
    
    def test_bq_scheme(self):
        self.assertEqual(bq_scheme(), 'bigquery')
    
    def test_bq_authority(self):
        self.assertEqual(bq_authority(Mock()), "")
    
    def test_bq_connection_uri(self):
        conn = Mock()
        conn.get_extra = lambda: '{"extra__google_cloud_platform__project": "dionricky-personal"}'
        dataset = 'test_dataset'
        table = 'test_table'

        self.assertEqual(bq_connection_uri(conn, dataset, table), f'bigquery:dionricky-personal.{dataset}.{table}')