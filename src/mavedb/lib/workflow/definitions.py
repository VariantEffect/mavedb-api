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
            "key": "link_gnomad_variants",
            "function": "link_gnomad_variants",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
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
        },
        # TODO#650: Simplify or automate the generation of these repetitive job definitions
        {
            "key": "refresh_clinvar_controls_201502",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2015,
                "month": 2,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_201601",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2016,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_201701",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2017,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_201801",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2018,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_201901",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2019,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_202001",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2020,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_202101",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2021,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_202201",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2022,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_202301",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2023,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_202401",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2024,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_202501",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2025,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
        },
        {
            "key": "refresh_clinvar_controls_202601",
            "function": "refresh_clinvar_controls",
            "type": JobType.MAPPED_VARIANT_ANNOTATION,
            "params": {
                "correlation_id": None,  # Required param to be filled in at runtime
                "score_set_id": None,  # Required param to be filled in at runtime
                "year": 2026,
                "month": 1,
            },
            "dependencies": [("submit_score_set_mappings_to_car", DependencyType.SUCCESS_REQUIRED)],
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
    "annotate_score_set": {
        "description": "Pipeline to annotate variants for a score set.",
        "job_definitions": annotation_pipeline_job_definitions(),
    },
    # Add more pipelines here
}
