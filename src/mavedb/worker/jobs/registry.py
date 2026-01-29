"""Job registry for worker configuration.

This module provides a centralized registry of all available worker jobs
as simple lists for ARQ worker configuration.
"""

from datetime import timedelta
from typing import Callable, List

from arq.cron import CronJob, cron

from mavedb.lib.types.workflow import JobDefinition
from mavedb.models.enums.job_pipeline import JobType
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


STANDALONE_JOB_DEFINITIONS: dict[Callable, JobDefinition] = {
    create_variants_for_score_set: {
        "dependencies": [],
        "params": {
            "score_set_id": None,
            "updater_id": None,
            "correlation_id": None,
            "scores_file_key": None,
            "counts_file_key": None,
            "score_columns_metadata": None,
            "count_columns_metadata": None,
        },
        "function": "create_variants_for_score_set",
        "key": "create_variants_for_score_set",
        "type": JobType.VARIANT_CREATION,
    },
    map_variants_for_score_set: {
        "dependencies": [],
        "params": {"score_set_id": None, "updater_id": None, "correlation_id": None},
        "function": "map_variants_for_score_set",
        "key": "map_variants_for_score_set",
        "type": JobType.VARIANT_MAPPING,
    },
    submit_score_set_mappings_to_car: {
        "dependencies": [],
        "params": {"score_set_id": None, "correlation_id": None},
        "function": "submit_score_set_mappings_to_car",
        "key": "submit_score_set_mappings_to_car",
        "type": JobType.MAPPED_VARIANT_ANNOTATION,
    },
    submit_score_set_mappings_to_ldh: {
        "dependencies": [],
        "params": {"score_set_id": None, "correlation_id": None},
        "function": "submit_score_set_mappings_to_ldh",
        "key": "submit_score_set_mappings_to_ldh",
        "type": JobType.MAPPED_VARIANT_ANNOTATION,
    },
    submit_uniprot_mapping_jobs_for_score_set: {
        "dependencies": [],
        "params": {"score_set_id": None, "correlation_id": None},
        "function": "submit_uniprot_mapping_jobs_for_score_set",
        "key": "submit_uniprot_mapping_jobs_for_score_set",
        "type": JobType.MAPPED_VARIANT_ANNOTATION,
    },
    poll_uniprot_mapping_jobs_for_score_set: {
        "dependencies": [],
        "params": {"score_set_id": None, "correlation_id": None},
        "function": "poll_uniprot_mapping_jobs_for_score_set",
        "key": "poll_uniprot_mapping_jobs_for_score_set",
        "type": JobType.MAPPED_VARIANT_ANNOTATION,
    },
    link_gnomad_variants: {
        "dependencies": [],
        "params": {"score_set_id": None, "correlation_id": None},
        "function": "link_gnomad_variants",
        "key": "link_gnomad_variants",
        "type": JobType.MAPPED_VARIANT_ANNOTATION,
    },
    refresh_materialized_views: {
        "dependencies": [],
        "params": {"correlation_id": None},
        "function": "refresh_materialized_views",
        "key": "refresh_materialized_views",
        "type": JobType.DATA_MANAGEMENT,
    },
    refresh_published_variants_view: {
        "dependencies": [],
        "params": {"correlation_id": None},
        "function": "refresh_published_variants_view",
        "key": "refresh_published_variants_view",
        "type": JobType.DATA_MANAGEMENT,
    },
}
"""
Standalone job definitions for direct job submission outside of pipelines. 
All job definitions in this dict must correspond to a job function in BACKGROUND_FUNCTIONS 
and must not have any dependencies on other jobs.
"""


__all__ = [
    "BACKGROUND_FUNCTIONS",
    "BACKGROUND_CRONJOBS",
    "STANDALONE_JOB_DEFINITIONS",
]
