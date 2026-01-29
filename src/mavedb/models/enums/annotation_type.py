import enum


class AnnotationType(enum.Enum):
    VRS_MAPPING = "vrs_mapping"
    CLINGEN_ALLELE_ID = "clingen_allele_id"
    MAPPED_HGVS = "mapped_hgvs"
    VARIANT_TRANSLATION = "variant_translation"
    GNOMAD_ALLELE_FREQUENCY = "gnomad_allele_frequency"
    CLINVAR_CONTROLS = "clinvar_control"
    VEP_FUNCTIONAL_CONSEQUENCE = "vep_functional_consequence"
    LDH_SUBMISSION = "ldh_submission"
