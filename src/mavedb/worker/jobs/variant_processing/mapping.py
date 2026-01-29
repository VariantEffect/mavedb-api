"""Variant mapping jobs using VRS (Variant Representation Specification).

This module handles the mapping of variants to standardized genomic coordinates
using the VRS mapping service. It includes queue management, retry logic,
and coordination with downstream services like ClinGen and UniProt.
"""

import asyncio
import functools
import logging
from datetime import date
from typing import Any

from sqlalchemy import cast, null, select
from sqlalchemy.dialects.postgresql import JSONB

from mavedb.data_providers.services import vrs_mapper
from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.exceptions import (
    NoMappedVariantsError,
    NonexistentMappingReferenceError,
    NonexistentMappingResultsError,
    NonexistentMappingScoresError,
)
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.mapping import ANNOTATION_LAYERS, EXCLUDED_PREMAPPED_ANNOTATION_KEYS
from mavedb.lib.slack import send_slack_error
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


@with_pipeline_management
async def map_variants_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """Map variants for a given score set using VRS."""
    # Handle everything prior to score set fetch in an outer layer. Any issues prior to
    # fetching the score set should fail the job outright and we will be unable to set
    # a processing state on the score set itself.

    job = job_manager.get_job()

    _job_required_params = [
        "score_set_id",
        "correlation_id",
        "updater_id",
    ]
    validate_job_params(_job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore

    # Handle everything within try/except to persist appropriate mapping state
    try:
        correlation_id = job.job_params["correlation_id"]  # type: ignore
        updater_id = job.job_params["updater_id"]  # type: ignore
        updated_by = job_manager.db.scalars(select(User).where(User.id == updater_id)).one()

        # Setup initial context and progress
        job_manager.save_to_context(
            {
                "application": "mavedb-worker",
                "function": "map_variants_for_score_set",
                "resource": score_set.urn,
                "correlation_id": correlation_id,
            }
        )
        job_manager.update_progress(0, 100, "Starting variant mapping job.")
        logger.info(msg="Started variant mapping job", extra=job_manager.logging_context())

        # TODO#372: non-nullable URNs
        if not score_set.urn:  # pragma: no cover
            raise ValueError("Score set URN is required for variant mapping.")

        # Setup score set state for mapping
        score_set.mapping_state = MappingState.processing
        score_set.mapping_errors = null()
        score_set.modified_by = updated_by
        score_set.modification_date = date.today()

        job_manager.db.add(score_set)
        job_manager.db.flush()

        job_manager.save_to_context({"mapping_state": score_set.mapping_state.name})
        job_manager.update_progress(10, 100, "Score set prepared for variant mapping.")
        logger.debug(msg="Score set prepared for variant mapping.", extra=job_manager.logging_context())

        # Do not block Worker event loop during mapping, see: https://arq-docs.helpmanual.io/#synchronous-jobs.
        vrs = vrs_mapper()
        blocking = functools.partial(vrs.map_score_set, score_set.urn)
        loop = asyncio.get_running_loop()

        mapping_results = None

        logger.debug(msg="Mapping variants using VRS mapping service.", extra=job_manager.logging_context())
        job_manager.update_progress(30, 100, "Mapping variants using VRS mapping service.")
        mapping_results = await loop.run_in_executor(ctx["pool"], blocking)

        logger.debug(msg="Done mapping variants.", extra=job_manager.logging_context())
        job_manager.update_progress(80, 100, "Processing mapped variants.")

        ## Check our assumptions about mapping results and handle errors appropriately.

        # Ensure we have mapping results
        if not mapping_results:
            job_manager.db.rollback()
            score_set.mapping_errors = {"error_message": "Mapping results were not returned from VRS mapping service."}
            job_manager.update_progress(100, 100, "Variant mapping failed due to missing results.")
            logger.error(
                msg="Mapping results were not returned from VRS mapping service.", extra=job_manager.logging_context()
            )
            raise NonexistentMappingResultsError("Mapping results were not returned from VRS mapping service.")

        # Ensure we have mapped scores
        mapped_scores = mapping_results.get("mapped_scores")
        if not mapped_scores:
            job_manager.db.rollback()
            score_set.mapping_errors = {"error_message": mapping_results.get("error_message")}
            job_manager.update_progress(100, 100, "Variant mapping failed; no variants were mapped.")
            logger.error(msg="No variants were mapped for this score set.", extra=job_manager.logging_context())
            raise NonexistentMappingScoresError("No variants were mapped for this score set.")

        # Ensure we have reference metadata
        reference_metadata = mapping_results.get("reference_sequences")
        if not reference_metadata:
            job_manager.db.rollback()
            score_set.mapping_errors = {"error_message": "Reference metadata missing from mapping results."}
            job_manager.update_progress(100, 100, "Variant mapping failed due to missing reference metadata.")
            logger.error(msg="Reference metadata missing from mapping results.", extra=job_manager.logging_context())
            raise NonexistentMappingReferenceError("Reference metadata missing from mapping results.")

        # Process and store mapped variants
        for target_gene_identifier in reference_metadata:
            target_gene = next(
                (target_gene for target_gene in score_set.target_genes if target_gene.name == target_gene_identifier),
                None,
            )

            if not target_gene:
                raise ValueError(
                    f"Target gene {target_gene_identifier} not found in database for score set {score_set.urn}."
                )

            job_manager.save_to_context({"processing_target_gene": target_gene.id})
            logger.debug(f"Processing target gene {target_gene.name}.", extra=job_manager.logging_context())

            # allow for multiple annotation layers
            pre_mapped_metadata: dict[str, Any] = {}
            post_mapped_metadata: dict[str, Any] = {}

            # add gene-level info
            gene_info = reference_metadata[target_gene_identifier].get("gene_info")
            if gene_info:
                target_gene.mapped_hgnc_name = gene_info.get("hgnc_symbol")
                post_mapped_metadata["hgnc_name_selection_method"] = gene_info.get("selection_method")

                job_manager.save_to_context({"mapped_hgnc_name": target_gene.mapped_hgnc_name})
                logger.debug("Added mapped HGNC name to target gene.", extra=job_manager.logging_context())

            # add annotation layer info
            for annotation_layer in reference_metadata[target_gene_identifier]["layers"]:
                layer_premapped = reference_metadata[target_gene_identifier]["layers"][annotation_layer].get(
                    "computed_reference_sequence"
                )
                if layer_premapped:
                    pre_mapped_metadata[ANNOTATION_LAYERS[annotation_layer]] = {
                        k: layer_premapped[k]
                        for k in set(list(layer_premapped.keys())) - EXCLUDED_PREMAPPED_ANNOTATION_KEYS
                    }
                    job_manager.save_to_context({"pre_mapped_layer_exists": True})

                layer_postmapped = reference_metadata[target_gene_identifier]["layers"][annotation_layer].get(
                    "mapped_reference_sequence"
                )
                if layer_postmapped:
                    post_mapped_metadata[ANNOTATION_LAYERS[annotation_layer]] = layer_postmapped
                    job_manager.save_to_context({"post_mapped_layer_exists": True})

                logger.debug(
                    f"Added annotation layer mapping metadata for {annotation_layer}.",
                    extra=job_manager.logging_context(),
                )

            target_gene.pre_mapped_metadata = cast(pre_mapped_metadata, JSONB)
            target_gene.post_mapped_metadata = cast(post_mapped_metadata, JSONB)
            job_manager.db.add(target_gene)
            logger.debug("Added mapping metadata to target gene.", extra=job_manager.logging_context())

        total_variants = len(mapped_scores)
        job_manager.save_to_context({"total_variants_to_process": total_variants})
        job_manager.update_progress(90, 100, "Saving mapped variants.")

        successful_mapped_variants = 0
        annotation_manager = AnnotationStatusManager(job_manager.db)
        for mapped_score in mapped_scores:
            variant_urn = mapped_score.get("mavedb_id")
            variant = job_manager.db.scalars(select(Variant).where(Variant.urn == variant_urn)).one()

            job_manager.save_to_context({"processing_variant": variant.id})
            logger.debug(f"Processing variant {variant.id}.", extra=job_manager.logging_context())

            # there should only be one current mapped variant per variant id, so update old mapped variant to current = false
            existing_mapped_variant = (
                job_manager.db.query(MappedVariant)
                .filter(MappedVariant.variant_id == variant.id, MappedVariant.current.is_(True))
                .one_or_none()
            )

            if existing_mapped_variant:
                job_manager.save_to_context({"existing_mapped_variant": existing_mapped_variant.id})
                existing_mapped_variant.current = False
                job_manager.db.add(existing_mapped_variant)
                logger.debug(msg="Set existing mapped variant to current = false.", extra=job_manager.logging_context())

            annotation_was_successful = mapped_score.get("pre_mapped") and mapped_score.get("post_mapped")
            if annotation_was_successful:
                successful_mapped_variants += 1
                job_manager.save_to_context({"successful_mapped_variants": successful_mapped_variants})

            mapped_variant = MappedVariant(
                pre_mapped=mapped_score.get("pre_mapped", null()),
                post_mapped=mapped_score.get("post_mapped", null()),
                variant_id=variant.id,
                modification_date=date.today(),
                mapped_date=mapping_results["mapped_date_utc"],
                vrs_version=mapped_score.get("vrs_version", null()),
                mapping_api_version=mapping_results["dcd_mapping_version"],
                error_message=mapped_score.get("error_message", null()),
                current=True,
            )

            annotation_manager.add_annotation(
                variant_id=variant.id,  # type: ignore
                annotation_type=AnnotationType.VRS_MAPPING,
                version=mapped_score.get("vrs_version", null()),
                status=AnnotationStatus.SUCCESS if annotation_was_successful else AnnotationStatus.FAILED,
                annotation_data={
                    "error_message": mapped_score.get("error_message", null()),
                    "job_run_id": job.id,
                    "success_data": {
                        "mapped_assay_level_hgvs": get_hgvs_from_post_mapped(mapped_score.get("post_mapped", {})),
                    },
                },
                current=True,
            )

            job_manager.db.add(mapped_variant)
            logger.debug(msg="Added new mapped variant to session.", extra=job_manager.logging_context())

        if successful_mapped_variants == 0:
            score_set.mapping_state = MappingState.failed
            score_set.mapping_errors = {"error_message": "All variants failed to map."}
        elif successful_mapped_variants < total_variants:
            score_set.mapping_state = MappingState.incomplete
        else:
            score_set.mapping_state = MappingState.complete

        job_manager.save_to_context(
            {
                "successful_mapped_variants": successful_mapped_variants,
                "mapping_state": score_set.mapping_state.name,
                "mapping_errors": score_set.mapping_errors,
                "inserted_mapped_variants": len(mapped_scores),
            }
        )
    except (NonexistentMappingResultsError, NonexistentMappingScoresError, NonexistentMappingReferenceError) as e:
        send_slack_error(e)
        logging_context = {**job_manager.logging_context(), **format_raised_exception_info_as_dict(e)}
        logger.error(msg="Known error during variant mapping.", extra=logging_context)

        score_set.mapping_state = MappingState.failed
        # These exceptions have already set mapping_errors appropriately

        return {"status": "exception", "data": {}, "exception": e}

    except Exception as e:
        send_slack_error(e)
        logging_context = {**job_manager.logging_context(), **format_raised_exception_info_as_dict(e)}
        logger.error(msg="Encountered an unexpected error while parsing mapped variants.", extra=logging_context)

        job_manager.db.rollback()

        score_set.mapping_state = MappingState.failed
        if not score_set.mapping_errors:
            score_set.mapping_errors = {
                "error_message": f"Encountered an unexpected error while parsing mapped variants. This job will be retried up to {job.max_retries} times (this was attempt {job.retry_count})."
            }
        job_manager.update_progress(100, 100, "Variant mapping failed due to an unexpected error.")

        return {"status": "exception", "data": {}, "exception": e}

    finally:
        job_manager.db.add(score_set)
        job_manager.db.flush()

    logger.info(msg="Inserted mapped variants into db.", extra=job_manager.logging_context())
    job_manager.update_progress(100, 100, "Finished processing mapped variants.")

    if successful_mapped_variants == 0:
        logger.error(msg="No variants were successfully mapped.", extra=job_manager.logging_context())
        return {
            "status": "failed",
            "data": {},
            "exception": NoMappedVariantsError("No variants were successfully mapped."),
        }

    logger.info(msg="Variant mapping job completed successfully.", extra=job_manager.logging_context())
    return {"status": "ok", "data": {}, "exception": None}
