"""External service integration job functions.

This module exports jobs for integrating with third-party services:
- ClinGen (Clinical Genome Resource) for allele registration and data submission
- UniProt for protein sequence annotation and ID mapping
- gnomAD for population frequency and genomic context data
- HGVS for standardized variant nomenclature population
- Variant Translation for PA<->CA allele relationship mapping
"""

# External services job functions
from .clingen import (
    submit_score_set_mappings_to_car,
    submit_score_set_mappings_to_ldh,
)
from .clinvar import refresh_clinvar_controls
from .gnomad import link_gnomad_variants
from .hgvs import populate_hgvs_for_score_set
from .uniprot import (
    poll_uniprot_mapping_jobs_for_score_set,
    submit_uniprot_mapping_jobs_for_score_set,
)
from .variant_translation import populate_variant_translations_for_score_set

__all__ = [
    "submit_score_set_mappings_to_car",
    "submit_score_set_mappings_to_ldh",
    "refresh_clinvar_controls",
    "link_gnomad_variants",
    "populate_hgvs_for_score_set",
    "populate_variant_translations_for_score_set",
    "poll_uniprot_mapping_jobs_for_score_set",
    "submit_uniprot_mapping_jobs_for_score_set",
]
