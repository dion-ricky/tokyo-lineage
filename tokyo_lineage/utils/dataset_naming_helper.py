import json
import platform
from urllib.parse import urlparse

from openlineage.airflow.utils import get_normalized_postgres_connection_uri


def fs_scheme() -> str:
    return 'file'


def fs_authority() -> str:
    return platform.uname().node


def fs_connection_uri() -> str:
    scheme = fs_scheme()
    host = platform.uname().node
    return f'{scheme}://{host}'


def pg_scheme() -> str:
    return 'postgres'


def pg_authority(conn) -> str:
    if conn.host and conn.port:
        return f'{conn.host}:{conn.port}'
    else:
        parsed = urlparse(conn.get_uri())
        return f'{parsed.hostname}:{parsed.port}'


def pg_connection_uri(conn) -> str:
    return get_normalized_postgres_connection_uri(conn)


def gcs_scheme() -> str:
    return 'gs'


def gcs_authority(conn) -> str:
    extras = json.loads(conn.get_extra())
    return f"{extras['extra__google_cloud_platform__project']}"


def gcs_connection_uri(conn, bucket) -> str:
    extras = json.loads(conn.get_extra())
    return f"{gcs_scheme()}://{extras['extra__google_cloud_platform__project']}/{bucket}"


def bq_scheme() -> str:
    return 'bigquery'


def bq_authority(conn) -> str:
    extras = json.loads(conn.get_extra())
    return f"{extras['extra__google_cloud_platform__project']}"


def bq_connection_uri(conn, dataset) -> str:
    scheme = bq_scheme()
    extras = json.loads(conn.get_extra())
    return f"{scheme}://{extras['extra__google_cloud_platform__project']}/{dataset}"