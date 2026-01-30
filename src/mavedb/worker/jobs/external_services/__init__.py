"""External service integration job functions.

This module exports jobs for integrating with third-party services:
- ClinGen (Clinical Genome Resource) for allele registration and data submission
- UniProt for protein sequence annotation and ID mapping
- gnomAD for population frequency and genomic context data
"""

# External services job functions
from .clingen import (
    submit_score_set_mappings_to_car,
    submit_score_set_mappings_to_ldh,
)
from .clinvar import refresh_clinvar_controls
from .gnomad import link_gnomad_variants
from .uniprot import (
    poll_uniprot_mapping_jobs_for_score_set,
    submit_uniprot_mapping_jobs_for_score_set,
)

__all__ = [
    "submit_score_set_mappings_to_car",
    "submit_score_set_mappings_to_ldh",
    "refresh_clinvar_controls",
    "link_gnomad_variants",
    "poll_uniprot_mapping_jobs_for_score_set",
    "submit_uniprot_mapping_jobs_for_score_set",
]
