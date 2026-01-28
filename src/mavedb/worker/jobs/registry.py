"""Job registry for worker configuration.

This module provides a centralized registry of all available worker jobs
as simple lists for ARQ worker configuration.
"""

from datetime import timedelta
from typing import Callable, List

from arq.cron import CronJob, cron

from mavedb.worker.jobs.data_management import (
    refresh_materialized_views,
    refresh_published_variants_view,
)
from mavedb.worker.jobs.external_services import (
    link_gnomad_variants,
    poll_uniprot_mapping_jobs_for_score_set,
    submit_score_set_mappings_to_car,
    submit_score_set_mappings_to_ldh,
    submit_uniprot_mapping_jobs_for_score_set,
)
from mavedb.worker.jobs.pipeline_management import start_pipeline
from mavedb.worker.jobs.variant_processing import (
    create_variants_for_score_set,
    map_variants_for_score_set,
)

# All job functions for ARQ worker
BACKGROUND_FUNCTIONS: List[Callable] = [
    # Variant processing jobs
    create_variants_for_score_set,
    map_variants_for_score_set,
    # External service jobs
    submit_score_set_mappings_to_car,
    submit_score_set_mappings_to_ldh,
    submit_uniprot_mapping_jobs_for_score_set,
    poll_uniprot_mapping_jobs_for_score_set,
    link_gnomad_variants,
    # Data management jobs
    refresh_materialized_views,
    refresh_published_variants_view,
    # Pipeline management jobs
    start_pipeline,
]

# Cron job definitions for ARQ worker
BACKGROUND_CRONJOBS: List[CronJob] = [
    cron(
        refresh_materialized_views,
        name="refresh_all_materialized_views",
        hour=20,
        minute=0,
        keep_result=timedelta(minutes=2).total_seconds(),
    ),
]


__all__ = [
    "BACKGROUND_FUNCTIONS",
    "BACKGROUND_CRONJOBS",
]
