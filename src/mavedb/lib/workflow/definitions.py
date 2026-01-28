from mavedb.lib.types.workflow import PipelineDefinition
from mavedb.models.enums.job_pipeline import DependencyType, JobType

# As a general rule, job keys should match function names for clarity. In some cases of
# repeated jobs, a suffix may be added to the key for uniqueness.

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
        ],
    },
    # Add more pipelines here
}
