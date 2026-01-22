"""MaveDB Worker Job Functions.

This package contains all worker job functions organized by domain:
- variant_processing: Variant creation and VRS mapping jobs
- external_services: Third-party service integration jobs (ClinGen, UniProt, gnomAD)
- data_management: Database and materialized view management jobs
- utils: Shared utilities for job state, retry logic, and constants

All job functions are exported at the package level for easy import
by the worker settings and other modules. Additionally, a job registry
is provided for ARQ worker configuration.
"""

from mavedb.worker.jobs.data_management.views import (
    refresh_materialized_views,
    refresh_published_variants_view,
)
from mavedb.worker.jobs.external_services.clingen import (
    link_clingen_variants,
    submit_score_set_mappings_to_car,
    submit_score_set_mappings_to_ldh,
)
from mavedb.worker.jobs.external_services.gnomad import link_gnomad_variants
from mavedb.worker.jobs.external_services.uniprot import (
    poll_uniprot_mapping_jobs_for_score_set,
    submit_uniprot_mapping_jobs_for_score_set,
)
from mavedb.worker.jobs.registry import (
    BACKGROUND_CRONJOBS,
    BACKGROUND_FUNCTIONS,
)
from mavedb.worker.jobs.variant_processing.creation import create_variants_for_score_set
from mavedb.worker.jobs.variant_processing.mapping import (
    map_variants_for_score_set,
)

__all__ = [
    # Variant processing jobs
    "create_variants_for_score_set",
    "map_variants_for_score_set",
    # External service integration jobs
    "link_clingen_variants",
    "submit_score_set_mappings_to_car",
    "submit_score_set_mappings_to_ldh",
    "poll_uniprot_mapping_jobs_for_score_set",
    "submit_uniprot_mapping_jobs_for_score_set",
    "link_gnomad_variants",
    # Data management jobs
    "refresh_materialized_views",
    "refresh_published_variants_view",
    # Job registry and utilities
    "BACKGROUND_FUNCTIONS",
    "BACKGROUND_CRONJOBS",
]
