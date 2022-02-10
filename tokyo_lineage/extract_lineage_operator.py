import logging
from typing import Optional, Tuple, List

from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults

from tokyo_lineage.extractor.airflow_extractor import AirflowExtractor
from tokyo_lineage.metadata_extractor.base import BaseMetadataExtractor
from tokyo_lineage.utils.airflow import get_dagruns

class ExtractLineageOperator(BaseOperator):
    
    template_fields = ()
    ui_color = '#fee8f4'

    @apply_defaults
    def __init__(
        self,
        dagrun_filters: Optional[Tuple] = (),
        custom_metadata_extractors: Optional[List[BaseMetadataExtractor]] = None,
        *args,
        **kwargs):
        super(ExtractLineageOperator, self).__init__(*args, **kwargs)
        self.dagrun_filters = dagrun_filters 
    
    def execute(self, context):
        logging.info("Start extracting lineage")

        logging.info("Scanning Airflow DagRun")
        dagruns = get_dagruns(self.dagrun_filters)

        logging.info("Instantiating extractor")
        extractor = AirflowExtractor()

        logging.info("Calling JobRun handler")
        extractor.handle_jobs_run(dagruns)

        logging.info("Finished extracting lineage")
        logging.info("INFO: Processed {} jobs".format(len(dagruns)))