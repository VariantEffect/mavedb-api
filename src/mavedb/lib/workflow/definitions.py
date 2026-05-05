from mavedb.lib.types.workflow import JobDefinition, PipelineDefinition
from mavedb.models.enums.job_pipeline import DependencyType, JobType

# As a general rule, job keys should match function names for clarity. In some cases of
# repeated jobs, a suffix may be added to the key for uniqueness.


def annotation_pipeline_job_definitions() -> list[JobDefinition]:
    return [
        {
            "key": "submit_score_set_mappings_to_car",
            "function": "submit_score_set_mappings_to_car",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "updater_id": None,  # Required param to be filled in at runtime
            },
            "dependencies": [("map_variants_for_score_set", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "warm_clingen_cache",
            "function": "warm_clingen_cache",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "link_gnomad_variants",
            "function": "link_gnomad_variants",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
            },
            "dependencies": [("warm_clingen_cache", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "submit_uniprot_mapping_jobs_for_score_set",
            "function": "submit_uniprot_mapping_jobs_for_score_set",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
            },
            "dependencies": [("map_variants_for_score_set", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "poll_uniprot_mapping_jobs_for_score_set",
            "function": "poll_uniprot_mapping_jobs_for_score_set",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "mapping_jobs": {},  # Required param to be filled in at runtime by previous job
            },
            "dependencies": [("submit_uniprot_mapping_jobs_for_score_set", DependencyType.SUCCESS_REQUIRED)],
            # UniProt ID mapping results are typically ready within seconds to minutes. A 30-second
            # retry delay prevents hammering the API while still polling frequently enough to be timely.
            "retry_delay_seconds": 30,
        },
        # Consolidated ClinVar refresh: a single job iterates all archival versions internally
        {
            "key": "refresh_clinvar_controls",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
            },
            "dependencies": [("warm_clingen_cache", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "populate_hgvs_for_score_set",
            "function": "populate_hgvs_for_score_set",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
            },
            "dependencies": [("warm_clingen_cache", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "populate_vep_for_score_set",
            "function": "populate_vep_for_score_set",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "populate_variant_translations_for_score_set",
            "function": "populate_variant_translations_for_score_set",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
            },
            "dependencies": [("warm_clingen_cache", DependencyType.SUCCESS_REQUIRED)],
        },
    ]


PIPELINE_DEFINITIONS: dict[str, PipelineDefinition] = {
    "validate_map_annotate_score_set": {
        "description": "Pipeline to validate, map, and annotate variants for a score set.",
        "job_definitions": [
            {
                "key": "create_variants_for_score_set",
                "function": "create_variants_for_score_set",
                "type": JobType.VARIANT_CREATION,
                "params": {
                    "correlation_id": None,  # Required param to be filled in at runtime
                    "score_set_id": None,  # Required param to be filled in at runtime
                    "updater_id": None,  # Required param to be filled in at runtime
                    "scores_file_key": None,  # Required param to be filled in at runtime
                    "counts_file_key": None,  # Required param to be filled in at runtime
                    "score_columns_metadata": None,  # Required param to be filled in at runtime
                    "count_columns_metadata": None,  # Required param to be filled in at runtime
                },
                "dependencies": [],
            },
            {
                "key": "map_variants_for_score_set",
                "function": "map_variants_for_score_set",
                "type": JobType.VARIANT_MAPPING,
                "params": {
                    "correlation_id": None,  # Required param to be filled in at runtime
                    "score_set_id": None,  # Required param to be filled in at runtime
                    "updater_id": None,  # Required param to be filled in at runtime
                },
                "dependencies": [("create_variants_for_score_set", DependencyType.SUCCESS_REQUIRED)],
            },
            *annotation_pipeline_job_definitions(),
        ],
    },
    "map_annotate_score_set": {
        "description": "Pipeline to map and annotate variants for a score set (assumes variants are already created).",
        "job_definitions": [
            {
                "key": "map_variants_for_score_set",
                "function": "map_variants_for_score_set",
                "type": JobType.VARIANT_MAPPING,
                "params": {
                    "correlation_id": None,  # Required param to be filled in at runtime
                    "score_set_id": None,  # Required param to be filled in at runtime
                    "updater_id": None,  # Required param to be filled in at runtime
                },
                "dependencies": [],
            },
            *annotation_pipeline_job_definitions(),
        ],
    },
    "annotate_score_set": {
        "description": "Pipeline to annotate variants for a score set.",
        "job_definitions": annotation_pipeline_job_definitions(),
    },
    "publish_score_set": {
        "description": "Pipeline to run post-publication tasks for a score set.",
        "job_definitions": [
            {
                "key": "refresh_published_variants_view",
                "function": "refresh_published_variants_view",
                "type": JobType.DATA_MANAGEMENT,
                "params": {
                    "correlation_id": None,
                    "score_set_id": None,
                },
                "dependencies": [],
            },
            # Future publish work: submit the published score set's mapped variants to ClinGen LDH.
            # {
            #     "key": "submit_score_set_mappings_to_ldh",
            #     "function": "submit_score_set_mappings_to_ldh",
            #     "type": JobType.MAPPED_VARIANT_ANNOTATION,
            #     "params": {
            #         "correlation_id": None,
            #         "score_set_id": None,
            #     },
            #     "dependencies": [],
            # },
        ],
    },
    # Add more pipelines here
}
