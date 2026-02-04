"""ClinVar integration jobs for variant annotation

This module contains job definitions and utility functions for integrating ClinVar
variant data into MaveDB. It includes functions to fetch and parse ClinVar variant
summary data, and update MaveDB records with the latest ClinVar annotations.
"""

import asyncio
import functools
import logging

import requests
from sqlalchemy import select

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.allele_registry import get_associated_clinvar_allele_id
from mavedb.lib.clinvar.utils import (
    fetch_clinvar_variant_summary_tsv,
    parse_clinvar_variant_summary,
    validate_clinvar_variant_summary_date,
)
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


# TODO#649: This function is currently called multiple times to fill in controls for each month/year.
#           We should consider caching both fetched TSV data and/or ClinGen API results. This would
#           significantly speed up large jobs annotating many variants.


@with_pipeline_management
async def refresh_clinvar_controls(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """
    Job to refresh ClinVar clinical control data in MaveDB.

    This job fetches the latest ClinVar variant summary data and updates
    the clinical control records in MaveDB accordingly.

    Args:
        ctx (dict): The job context containing necessary information.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): The job manager instance for managing job state.

    Returns:
        JobResultData: The result of the job execution.
    """
    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id", "year", "month"]
    validate_job_params(_job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore
    year = int(job.job_params["year"])  # type: ignore
    month = int(job.job_params["month"])  # type: ignore

    validate_clinvar_variant_summary_date(month, year)
    # Version must be in MM_YYYY format
    clinvar_version = f"{month:02d}_{year}"

    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "refresh_clinvar_controls",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
            "clinvar_year": year,
            "clinvar_month": month,
        }
    )
    job_manager.update_progress(0, 100, f"Starting ClinVar clinical control refresh for version {clinvar_version}.")
    logger.info(msg="Started ClinVar clinical control refresh", extra=job_manager.logging_context())

    job_manager.update_progress(1, 100, "Fetching ClinVar variant summary TSV data.")
    logger.debug("Fetching ClinVar variant summary TSV data.", extra=job_manager.logging_context())

    # Fetch and parse ClinVar variant summary TSV data
    blocking = functools.partial(fetch_clinvar_variant_summary_tsv, month, year)
    loop = asyncio.get_running_loop()
    tsv_content = await loop.run_in_executor(ctx["pool"], blocking)
    tsv_data = parse_clinvar_variant_summary(tsv_content)

    job_manager.update_progress(10, 100, "Fetched and parsed ClinVar variant summary TSV data.")
    logger.debug("Fetched and parsed ClinVar variant summary TSV data.", extra=job_manager.logging_context())

    variants_to_refresh = job_manager.db.scalars(
        select(MappedVariant)
        .join(Variant)
        .where(
            Variant.score_set_id == score_set.id,
            MappedVariant.current.is_(True),
        )
    ).all()
    total_variants_to_refresh = len(variants_to_refresh)
    job_manager.save_to_context({"total_variants_to_refresh": total_variants_to_refresh})

    logger.info(
        f"Refreshing ClinVar data for {total_variants_to_refresh} variants.", extra=job_manager.logging_context()
    )
    annotation_manager = AnnotationStatusManager(job_manager.db)
    for index, mapped_variant in enumerate(variants_to_refresh):
        job_manager.save_to_context({"mapped_variant_id": mapped_variant.id, "progress_index": index})
        if total_variants_to_refresh > 0 and index % (max(total_variants_to_refresh // 100, 1)) == 0:
            job_manager.update_progress(
                10 + int((index / total_variants_to_refresh) * 90),
                100,
                f"Refreshing ClinVar data for {total_variants_to_refresh} variants ({index} completed).",
            )

        clingen_id = mapped_variant.clingen_allele_id
        job_manager.save_to_context({"clingen_allele_id": clingen_id})

        if clingen_id is None:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.CLINVAR_CONTROL,
                version=clinvar_version,
                status=AnnotationStatus.SKIPPED,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": "Mapped variant does not have an associated ClinGen allele ID.",
                    "failure_category": "missing_clingen_allele_id",
                },
            )
            logger.debug(
                "Mapped variant does not have an associated ClinGen allele ID.", extra=job_manager.logging_context()
            )
            continue

        if clingen_id is not None and "," in clingen_id:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.CLINVAR_CONTROL,
                version=clinvar_version,
                status=AnnotationStatus.SKIPPED,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": "Multi-variant ClinGen allele IDs cannot be associated with ClinVar data.",
                    "failure_category": "multi_variant_clingen_allele_id",
                },
            )
            logger.debug("Detected a multi-variant ClinGen allele ID, skipping.", extra=job_manager.logging_context())
            continue

        # Fetch associated ClinVar Allele ID from ClinGen API
        try:
            # Guaranteed based on our query filters.
            clinvar_allele_id = get_associated_clinvar_allele_id(clingen_id)  # type: ignore
        except requests.exceptions.RequestException as exc:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.CLINVAR_CONTROL,
                version=clinvar_version,
                status=AnnotationStatus.FAILED,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": f"Failed to retrieve ClinVar allele ID from ClinGen API: {str(exc)}",
                    "failure_category": "clingen_api_error",
                },
            )
            logger.error(
                f"Failed to retrieve ClinVar allele ID from ClinGen API for ClinGen allele ID {clingen_id}.",
                extra=job_manager.logging_context(),
                exc_info=exc,
            )
            continue

        job_manager.save_to_context({"clinvar_allele_id": clinvar_allele_id})

        if clinvar_allele_id is None:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.CLINVAR_CONTROL,
                version=clinvar_version,
                status=AnnotationStatus.SKIPPED,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": "No ClinVar allele ID found for ClinGen allele ID.",
                    "failure_category": "no_associated_clinvar_allele_id",
                },
                current=True,
            )
            logger.debug("No ClinVar allele ID found for ClinGen allele ID.", extra=job_manager.logging_context())
            continue

        if clinvar_allele_id not in tsv_data:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.CLINVAR_CONTROL,
                version=clinvar_version,
                status=AnnotationStatus.SKIPPED,
                annotation_data={
                    "job_run_id": job_manager.job_id,
                    "error_message": "No ClinVar data found for ClinVar allele ID.",
                    "failure_category": "no_clinvar_variant_data",
                },
            )
            logger.debug("No ClinVar variant data found for ClinGen allele ID.", extra=job_manager.logging_context())
            continue

        variant_data = tsv_data[clinvar_allele_id]
        identifier = str(clinvar_allele_id)

        clinvar_variant = job_manager.db.scalars(
            select(ClinicalControl).where(
                ClinicalControl.db_identifier == identifier,
                ClinicalControl.db_version == clinvar_version,
                ClinicalControl.db_name == "ClinVar",
            )
        ).one_or_none()
        if clinvar_variant is None:
            job_manager.save_to_context({"creating_new_clinvar_variant": True})
            clinvar_variant = ClinicalControl(
                db_identifier=identifier,
                gene_symbol=variant_data.get("GeneSymbol"),
                clinical_significance=variant_data.get("ClinicalSignificance"),
                clinical_review_status=variant_data.get("ReviewStatus"),
                db_version=clinvar_version,
                db_name="ClinVar",
            )
        else:
            job_manager.save_to_context({"creating_new_clinvar_variant": False})
            clinvar_variant.gene_symbol = variant_data.get("GeneSymbol")
            clinvar_variant.clinical_significance = variant_data.get("ClinicalSignificance")
            clinvar_variant.clinical_review_status = variant_data.get("ReviewStatus")

        # Add and flush the updated/new clinical control
        job_manager.db.add(clinvar_variant)
        job_manager.db.flush()

        # Link the clinical control to the mapped variant if not already linked
        if clinvar_variant not in mapped_variant.clinical_controls:
            mapped_variant.clinical_controls.append(clinvar_variant)
            job_manager.db.add(mapped_variant)
            logger.debug("Linked ClinicalControl to MappedVariant.", extra=job_manager.logging_context())

        annotation_manager.add_annotation(
            variant_id=mapped_variant.variant_id,  # type: ignore
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            version=clinvar_version,
            status=AnnotationStatus.SUCCESS,
            annotation_data={
                "job_run_id": job_manager.job_id,
                "success_data": {
                    "clinvar_allele_id": clinvar_allele_id,
                },
            },
            current=True,
        )

        logger.debug("Updated ClinVar data for ClinGen allele ID.", extra=job_manager.logging_context())

    logger.info(
        msg=f"Fetched ClinVar variant summary data version {clinvar_version}", extra=job_manager.logging_context()
    )
    job_manager.update_progress(100, 100, "Completed ClinVar clinical control refresh.")

    return {"status": "ok", "data": {}, "exception": None}
