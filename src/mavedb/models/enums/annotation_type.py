from enum import Enum


class AnnotationType(str, Enum):
    """A pipeline check whose result ``annotation_status`` records, named after what the check produces."""

    VRS_MAPPING = "vrs_mapping"
    """Did the mapper map this variant to a VRS allele?"""

    CROSS_LEVEL_TRANSLATION = "cross_level_translation"
    """Did reverse translation build this variant's equivalents at the other sequence levels?"""

    CLINGEN_ALLELE_ID = "clingen_allele_id"
    """Does ClinGen's Allele Registry give this allele a CAID?"""

    GNOMAD_ALLELE_FREQUENCY = "gnomad_allele_frequency"
    """Is this allele in gnomAD, at the current gnomAD version?"""

    CLINVAR_ALLELE_ID = "clinvar_allele_id"
    """Does ClinGen link this allele's CAID to a ClinVar AlleleID? One status per allele, whatever the release."""

    CLINVAR_CONTROL = "clinvar_control"
    """Is this allele's ClinVar AlleleID in a given ClinVar release? One status per release, only for alleles whose
    ``CLINVAR_ALLELE_ID`` is present."""

    VEP_FUNCTIONAL_CONSEQUENCE = "vep_functional_consequence"
    """Does VEP give this allele a functional consequence, at the current Ensembl release?"""

    LDH_SUBMISSION = "ldh_submission"
    """Was this variant submitted to ClinGen's Linked Data Hub?"""
