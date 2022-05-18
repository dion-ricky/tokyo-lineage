from tokyo_lineage.metadata_extractor.airflow import (
    PostgresExtractor,
    PostgresToAvroExtractor,
    FileToGcsExtractor,
    GcsToBigQueryExtractor,
    MySqlToAvroExtractor
)

AIRFLOW_METADATA_EXTRACTORS = [
    PostgresExtractor,
    PostgresToAvroExtractor,
    FileToGcsExtractor,
    GcsToBigQueryExtractor,
    MySqlToAvroExtractor
]