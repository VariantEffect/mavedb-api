import io
import json
import logging
import time
from datetime import date, datetime
from functools import partial
from typing import Any, List, Literal, Optional, Sequence, TypedDict, Union

import numpy as np
import pandas as pd
import requests
from arq import ArqRedis
from fastapi import APIRouter, Depends, File, Query, Request, Response, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import StreamingResponse
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityStatement
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement
from pydantic import ValidationError
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError, MultipleResultsFound
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.data_providers.services import CSV_UPLOAD_S3_BUCKET_NAME, s3_client
from mavedb.lib.annotation.annotate import (
    variant_functional_impact_statement,
    variant_pathogenicity_statement,
    variant_study_result,
)
from mavedb.lib.annotation.context import variant_annotation_context
from mavedb.lib.annotation.exceptions import EXPECTED_ABSENCE_EXCEPTIONS
from mavedb.lib.authorization import (
    get_current_user,
    get_principal,
    require_current_user,
    require_current_user_with_email,
)
from mavedb.lib.clinical_controls import get_clinical_control_options, get_clinical_controls_with_variant_urns
from mavedb.lib.contributors import find_or_create_contributor
from mavedb.lib.csv.columns import variants_to_csv_rows
from mavedb.lib.csv.deprecated_params import (
    DROP_NA_COLUMNS_DESCRIPTION,
    INCLUDE_CUSTOM_COLUMNS_DESCRIPTION,
    INCLUDE_POST_MAPPED_HGVS_DESCRIPTION,
    resolve_deprecated_csv_params,
)
from mavedb.lib.csv.namespaces import CSV_NAMESPACES_PARAM_DESCRIPTION, CsvNamespaceStr
from mavedb.lib.csv.score_set import available_score_set_csv_namespaces, get_score_set_variants_as_csv
from mavedb.lib.exceptions import MixedTargetError, NonexistentOrcidUserError
from mavedb.lib.experiments import enrich_experiment_with_num_score_sets
from mavedb.lib.gnomad import get_gnomad_variants_with_variant_urns
from mavedb.lib.identifiers import (
    create_external_gene_identifier_offset,
    find_or_create_doi_identifier,
    find_or_create_publication_identifier,
)
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.score_calibrations import create_score_calibration
from mavedb.lib.score_set_variants import get_lean_score_set_variants
from mavedb.lib.score_sets import (
    csv_data_to_df,
    fetch_score_set_search_filter_options,
    find_meta_analyses_for_experiment_sets,
    get_annotatable_variants,
    is_replaces_id_unique_violation,
    refresh_variant_urns,
)
from mavedb.lib.score_sets import (
    search_score_sets as _search_score_sets,
)
from mavedb.lib.slack import send_slack_error
from mavedb.lib.target_genes import find_or_create_target_gene_by_accession, find_or_create_target_gene_by_sequence
from mavedb.lib.taxonomies import find_or_create_taxonomy
from mavedb.lib.types.authentication import UserData
from mavedb.lib.urns import (
    generate_experiment_set_urn,
    generate_experiment_urn,
    generate_score_set_urn,
)
from mavedb.lib.variant_detail import get_variant_detail
from mavedb.lib.workflow.kickoff import enqueue_pipeline_for_score_set
from mavedb.models.contributor import Contributor
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.experiment import Experiment
from mavedb.models.license import License
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.variant import Variant
from mavedb.routers.shared import (
    ACCESS_CONTROL_ERROR_RESPONSES,
    BASE_400_RESPONSE,
    BASE_409_RESPONSE,
    BASE_RESPONSES,
    GATEWAY_ERROR_RESPONSES,
    PUBLIC_ERROR_RESPONSES,
    ROUTER_BASE_PREFIX,
)
from mavedb.view_models import clinical_control, gnomad_variant, score_set
from mavedb.view_models.contributor import ContributorCreate
from mavedb.view_models.csv_namespace import AvailableCsvNamespace
from mavedb.view_models.doi_identifier import DoiIdentifierCreate
from mavedb.view_models.lean_variant import LeanVariant
from mavedb.view_models.publication_identifier import PublicationIdentifierCreate
from mavedb.view_models.score_set_dataset_columns import DatasetColumnMetadata
from mavedb.view_models.search import ScoreSetsSearch, ScoreSetsSearchFilterOptionsResponse, ScoreSetsSearchResponse
from mavedb.view_models.target_gene import TargetGeneCreate
from mavedb.view_models.variant_detail import VariantDetail

TAG_NAME = "Score Sets"
logger = logging.getLogger(__name__)

SCORE_SET_SEARCH_MAX_LIMIT = 100
SCORE_SET_SEARCH_MAX_PUBLICATION_IDENTIFIERS = 40


async def enqueue_variant_creation(
    *,
    item: ScoreSet,
    user_data: UserData,
    new_scores_df: Optional[pd.DataFrame] = None,
    new_counts_df: Optional[pd.DataFrame] = None,
    new_score_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None,
    new_count_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None,
    worker: ArqRedis,
    db: Session,
) -> None:
    assert item.dataset_columns is not None

    # create CSV from existing variants on the score set if no new dataframe provided
    existing_scores_df = None
    if new_scores_df is None and item.dataset_columns.get("score_columns"):
        score_columns = {
            "core": ["hgvs_nt", "hgvs_splice", "hgvs_pro"],
            "scores": item.dataset_columns["score_columns"],
        }
        existing_scores_df = pd.DataFrame(
            variants_to_csv_rows(item.variants, columns=score_columns, namespaced=False)
        ).replace("NA", np.NaN)

    # create CSV from existing variants on the score set if no new dataframe provided
    existing_counts_df = None
    if new_counts_df is None and item.dataset_columns.get("count_columns"):
        count_columns = {
            "core": ["hgvs_nt", "hgvs_splice", "hgvs_pro"],
            "counts": item.dataset_columns["count_columns"],
        }
        existing_counts_df = pd.DataFrame(
            variants_to_csv_rows(item.variants, columns=count_columns, namespaced=False)
        ).replace("NA", np.NaN)

    scores_file_to_upload = existing_scores_df if new_scores_df is None else new_scores_df
    counts_file_to_upload = existing_counts_df if new_counts_df is None else new_counts_df

    scores_file_key = None
    counts_file_key = None
    if scores_file_to_upload is not None or counts_file_to_upload is not None:
        timestamp = date.today().isoformat()
        unique_id = str(int(time.time() * 1000))
        user_id = user_data.user.id
        score_set_id = item.id

        s3 = s3_client()

        if scores_file_to_upload is not None:
            save_to_logging_context({"num_scores": len(scores_file_to_upload)})
            scores_file_key = f"{score_set_id}/{user_id}/{timestamp}-{unique_id}-scores.csv"
            s3.upload_fileobj(
                Fileobj=io.BytesIO(scores_file_to_upload.to_csv(index=False).encode("utf-8")),
                Bucket=CSV_UPLOAD_S3_BUCKET_NAME,
                Key=scores_file_key,
            )

        if counts_file_to_upload is not None:
            save_to_logging_context({"num_counts": len(counts_file_to_upload)})
            counts_file_key = f"{score_set_id}/{user_id}/{timestamp}-{unique_id}-counts.csv"
            s3.upload_fileobj(
                Fileobj=io.BytesIO(counts_file_to_upload.to_csv(index=False).encode("utf-8")),
                Bucket=CSV_UPLOAD_S3_BUCKET_NAME,
                Key=counts_file_key,
            )

    try:
        await enqueue_pipeline_for_score_set(
            db=db,
            redis=worker,
            pipeline_name="validate_map_annotate_score_set",
            score_set=item,
            user=user_data.user,
            extra_params={
                "scores_file_key": scores_file_key,
                "counts_file_key": counts_file_key,
                "score_columns_metadata": item.dataset_columns.get("score_columns_metadata")
                if new_score_columns_metadata is None
                else new_score_columns_metadata,
                "count_columns_metadata": item.dataset_columns.get("count_columns_metadata")
                if new_count_columns_metadata is None
                else new_count_columns_metadata,
            },
        )

    except Exception:
        # Clean up any S3 files uploaded during this call to avoid orphaned objects when the
        # pipeline could not be created or enqueued.
        keys_to_delete = [k for k in [scores_file_key, counts_file_key] if k is not None]
        if keys_to_delete:
            try:
                s3_client().delete_objects(
                    Bucket=CSV_UPLOAD_S3_BUCKET_NAME,
                    Delete={"Objects": [{"Key": k} for k in keys_to_delete]},
                )
            except Exception:
                logger.error(
                    msg="Failed to clean up orphaned S3 files after pipeline enqueue failure.",
                    extra=logging_context(),
                )
        raise


class ScoreSetUpdateResult(TypedDict):
    item: ScoreSet
    should_create_variants: bool


async def score_set_update(
    *,
    db: Session,
    urn: str,
    item_update: score_set.ScoreSetUpdateAllOptional,
    exclude_unset: bool = False,
    user_data: UserData,
    existing_item: Optional[ScoreSet] = None,
) -> ScoreSetUpdateResult:
    logger.info(msg="Updating score set.", extra=logging_context())

    should_create_variants = False
    item_update_dict: dict[str, Any] = item_update.model_dump(exclude_unset=exclude_unset)

    item = existing_item or db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item or item.id is None:
        logger.info(msg="Failed to update score set; The requested score set does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.UPDATE)

    for var, value in item_update_dict.items():
        if var not in [
            "contributors",
            "doi_identifiers",
            "experiment_urn",
            "license_id",
            "secondary_publication_identifiers",
            "primary_publication_identifiers",
            "target_genes",
            "dataset_columns",
        ]:
            setattr(item, var, value)

    item_update_license_id = item_update_dict.get("license_id")
    if item_update_license_id is not None:
        save_to_logging_context({"license": item_update_license_id})
        license_ = db.query(License).filter(License.id == item_update_license_id).one_or_none()

        if not license_:
            logger.info(
                msg="Failed to update score set; The requested license does not exist.", extra=logging_context()
            )
            raise HTTPException(status_code=404, detail="Unknown license")

            # Allow in-active licenses to be retained on update if they already exist on the item.
        elif not license_.active and item.license.id != item_update_license_id:
            logger.info(
                msg="Failed to update score set license; The requested license is no longer active.",
                extra=logging_context(),
            )
            raise HTTPException(status_code=409, detail="Invalid license")

        item.license = license_

    if "doi_identifiers" in item_update_dict:
        doi_identifiers_list = [
            DoiIdentifierCreate(**identifier) for identifier in item_update_dict.get("doi_identifiers") or []
        ]
        item.doi_identifiers = [
            await find_or_create_doi_identifier(db, identifier.identifier) for identifier in doi_identifiers_list
        ]

    if any(key in item_update_dict for key in ["primary_publication_identifiers", "secondary_publication_identifiers"]):
        if "primary_publication_identifiers" in item_update_dict:
            primary_publication_identifiers_list = [
                PublicationIdentifierCreate(**identifier)
                for identifier in item_update_dict.get("primary_publication_identifiers") or []
            ]
            try:
                primary_publication_identifiers = [
                    await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
                    for identifier in primary_publication_identifiers_list
                ]
            except requests.exceptions.ConnectTimeout:
                logger.error(msg="Gateway timed out while creating publication identifiers.", extra=logging_context())
                raise HTTPException(
                    status_code=504,
                    detail="Gateway Timeout while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
                )

            except requests.exceptions.HTTPError:
                logger.error(
                    msg="Encountered bad gateway while creating publication identifiers.", extra=logging_context()
                )
                raise HTTPException(
                    status_code=502,
                    detail="Bad Gateway while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
                )
        else:
            # set to existing primary publication identifiers if not provided in update
            primary_publication_identifiers = [p for p in item.publication_identifiers if getattr(p, "primary", False)]

        if "secondary_publication_identifiers" in item_update_dict:
            secondary_publication_identifiers_list = [
                PublicationIdentifierCreate(**identifier)
                for identifier in item_update_dict.get("secondary_publication_identifiers") or []
            ]
            try:
                secondary_publication_identifiers = [
                    await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
                    for identifier in secondary_publication_identifiers_list
                ]
            except requests.exceptions.ConnectTimeout:
                logger.error(msg="Gateway timed out while creating publication identifiers.", extra=logging_context())
                raise HTTPException(
                    status_code=504,
                    detail="Gateway Timeout while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
                )

            except requests.exceptions.HTTPError:
                logger.error(
                    msg="Encountered bad gateway while creating publication identifiers.", extra=logging_context()
                )
                raise HTTPException(
                    status_code=502,
                    detail="Bad Gateway while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
                )

        else:
            # set to existing secondary publication identifiers if not provided in update
            secondary_publication_identifiers = [
                p for p in item.publication_identifiers if not getattr(p, "primary", False)
            ]

        publication_identifiers = primary_publication_identifiers + secondary_publication_identifiers

        # create a temporary `primary` attribute on each of our publications that indicates
        # to our association proxy whether it is a primary publication or not
        primary_identifiers = [p.identifier for p in primary_publication_identifiers]
        for publication in publication_identifiers:
            setattr(publication, "primary", publication.identifier in primary_identifiers)

        item.publication_identifiers = publication_identifiers

    if "contributors" in item_update_dict:
        try:
            contributors = [
                ContributorCreate(**contributor) for contributor in item_update_dict.get("contributors") or []
            ]
            item.contributors = [
                await find_or_create_contributor(db, contributor.orcid_id) for contributor in contributors
            ]
        except NonexistentOrcidUserError as e:
            logger.error(msg="Could not find ORCID user with the provided user ID.", extra=logging_context())
            raise HTTPException(status_code=404, detail=str(e))

    # Score set has not been published and attributes affecting scores may still be edited.
    if item.private:
        if "target_genes" in item_update_dict:
            # stash existing target gene ids to compare after update, to determine if variants need to be re-created
            assert all(tg.id is not None for tg in item.target_genes)
            existing_target_ids: list[int] = [tg.id for tg in item.target_genes if tg.id is not None]

            targets: List[TargetGene] = []
            accessions = False

            for tg in item_update_dict.get("target_genes", []):
                gene = TargetGeneCreate(**tg)
                if gene.target_sequence:
                    if accessions and len(targets) > 0:
                        logger.info(
                            msg="Failed to update score set; Both a sequence and accession based target were detected.",
                            extra=logging_context(),
                        )

                        raise MixedTargetError(
                            "MaveDB does not support score-sets with both sequence and accession based targets. Please re-submit this scoreset using only one type of target."
                        )

                    upload_taxonomy = gene.target_sequence.taxonomy
                    save_to_logging_context({"requested_taxonomy": gene.target_sequence.taxonomy.code})
                    taxonomy = await find_or_create_taxonomy(db, upload_taxonomy)

                    if not taxonomy:
                        logger.info(
                            msg="Failed to create score set; The requested taxonomy does not exist.",
                            extra=logging_context(),
                        )
                        raise HTTPException(
                            status_code=404,
                            detail=f"Unknown taxonomy {gene.target_sequence.taxonomy.code}",
                        )

                    # If the target sequence has a label, use it. Otherwise, use the name from the target gene as the label.
                    # View model validation rules enforce that sequences must have a label defined if there are more than one
                    # targets defined on a score set.
                    seq_label = gene.target_sequence.label if gene.target_sequence.label is not None else gene.name

                    target_gene = target_gene = find_or_create_target_gene_by_sequence(
                        db,
                        score_set_id=item.id,
                        tg=jsonable_encoder(
                            gene,
                            by_alias=False,
                            exclude={
                                "external_identifiers",
                                "target_sequence",
                                "target_accession",
                            },
                        ),
                        tg_sequence={
                            **jsonable_encoder(gene.target_sequence, by_alias=False, exclude={"taxonomy", "label"}),
                            "taxonomy": taxonomy,
                            "label": seq_label,
                        },
                    )

                elif gene.target_accession:
                    if not accessions and len(targets) > 0:
                        logger.info(
                            msg="Failed to create score set; Both a sequence and accession based target were detected.",
                            extra=logging_context(),
                        )
                        raise MixedTargetError(
                            "MaveDB does not support score-sets with both sequence and accession based targets. Please re-submit this scoreset using only one type of target."
                        )
                    accessions = True

                    target_gene = find_or_create_target_gene_by_accession(
                        db,
                        score_set_id=item.id,
                        tg=jsonable_encoder(
                            gene,
                            by_alias=False,
                            exclude={
                                "external_identifiers",
                                "target_sequence",
                                "target_accession",
                            },
                        ),
                        tg_accession=jsonable_encoder(gene.target_accession, by_alias=False),
                    )
                else:
                    save_to_logging_context({"failing_target": gene})
                    logger.info(msg="Failed to create score set; Could not infer target type.", extra=logging_context())
                    raise ValueError("One of either `target_accession` or `target_gene` should be present")

                for external_gene_identifier_offset_create in gene.external_identifiers:
                    offset = external_gene_identifier_offset_create.offset
                    identifier_create = external_gene_identifier_offset_create.identifier
                    await create_external_gene_identifier_offset(
                        db,
                        target_gene,
                        identifier_create.db_name,
                        identifier_create.identifier,
                        offset,
                    )

                targets.append(target_gene)

            item.target_genes = targets

            assert all(tg.id is not None for tg in item.target_genes)
            current_target_ids: list[int] = [tg.id for tg in item.target_genes if tg.id is not None]

            if sorted(existing_target_ids) != sorted(current_target_ids):
                logger.info(msg=f"Target genes have changed for score set {item.id}", extra=logging_context())
                should_create_variants = True if item.variants else False

    else:
        logger.debug(msg="Skipped score range and target gene update. Score set is published.", extra=logging_context())

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    return {"item": item, "should_create_variants": should_create_variants}


class ParseScoreSetUpdate(TypedDict):
    scores_df: Optional[pd.DataFrame]
    counts_df: Optional[pd.DataFrame]


async def parse_score_set_variants_uploads(
    scores_file: Optional[UploadFile] = File(None),
    counts_file: Optional[UploadFile] = File(None),
) -> ParseScoreSetUpdate:
    if scores_file and scores_file.file:
        try:
            scores_df = csv_data_to_df(scores_file.file)
        # Handle non-utf8 file problem.
        except UnicodeDecodeError as e:
            raise HTTPException(
                status_code=400, detail=f"Error decoding file: {e}. Ensure the file has correct values."
            )
    else:
        scores_df = None

    if counts_file and counts_file.file:
        try:
            counts_df = csv_data_to_df(counts_file.file)
        # Handle non-utf8 file problem.
        except UnicodeDecodeError as e:
            raise HTTPException(
                status_code=400, detail=f"Error decoding file: {e}. Ensure the file has correct values."
            )
    else:
        counts_df = None

    return {
        "scores_df": scores_df,
        "counts_df": counts_df,
    }


async def fetch_score_set_by_urn(
    db, urn: str, user: Optional[UserData], owner_or_contributor: Optional[UserData], only_published: bool
) -> ScoreSet:
    """
    Fetch one score set by URN, ensuring that the user has read permission.

    :param db: An active database session.
    :param urn: The score set URN.
    :param user: The user who has requested the score set. If the user does not have read permission, the score set will
      not be returned. If None, the score set is returned only if publicly visible.
    :param owner_or_contributor: If not None, require that the result be a score set of which this user is owner or
      contributor. This is an ownership requirement, not a visibility one: it does not admit score sets that are
      merely public. Combining it with only_published therefore yields published score sets owned by this user.
    :param only_published: If true, only return the score set if it is published.
    :return: The score set.
    :raises HTTPException: 404 if no score set matches the URN and the supplied filters, or 500 if more than one
        does. Read permission is asserted on the result and raises through assert_permission.
    """
    try:
        query = db.query(ScoreSet).filter(ScoreSet.urn == urn)
        if owner_or_contributor is not None:
            query = query.filter(
                or_(
                    ScoreSet.created_by_id == owner_or_contributor.user.id,
                    ScoreSet.contributors.any(Contributor.orcid_id == owner_or_contributor.user.username),
                )
            )
        if only_published:
            query = query.filter(ScoreSet.private.is_(False))
        item = query.one_or_none()
    except MultipleResultsFound:
        logger.info(
            msg="Could not fetch the requested score set; Multiple such score sets exist.", extra=logging_context()
        )
        raise HTTPException(status_code=500, detail=f"multiple score sets with URN '{urn}' were found")

    if not item:
        logger.info(msg="Could not fetch the requested score set; No such score sets exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user, item, Action.READ)

    # Narrowing what the score set carries belongs to _score_set_response, so that this function's other
    # callers -- supersession lookup, publication -- receive the score set as it actually is.
    return item


def _score_set_response(item: ScoreSet, principal: Principal) -> score_set.ScoreSet:
    """
    Serialize a score set for a response, withholding the sub-resources this caller may not read.

    Every route in this module that returns a ``ScoreSet`` view model builds it here. The two sub-resources
    a score set carries have READ rules stricter than its own, and each was leaked from a different route
    before this was centralized:

      - Calibrations. Publishing a score set does not publish its calibrations, and owning a score set does
        not entitle its owner to a community calibration someone else attached to it.
      - The superseding score set, which is usually still private while the score set it replaces is public.

    The search routes are the deliberate exception: they answer with ``ShortScoreSet``, which carries neither
    sub-resource, so there is nothing for this function to narrow. Any route that widens its response model
    to ``ScoreSet`` must come through here.

    Local to this module by design. A shared response constructor was considered and deferred: the same ORM
    graph is also serialized as CSV, VA-Spec NDJSON and ScoreSetPublicDump, none of which such a constructor
    would cover, so the durable guarantee belongs at the session rather than the response layer.

    Narrowing is applied to the validated view. ``ScoreSet.score_calibrations`` is mapped with
    ``cascade="all, delete-orphan"``, and assigning ``superseding_score_set = None`` nulls the other score
    set's ``replaces_id``; narrowing the ORM objects instead stages both as writes.

    Args:
        item (ScoreSet): The score set to serialize. Asserting READ on the score set itself belongs to the
            caller.
        principal (Principal): The caller being served.

    Returns:
        score_set.ScoreSet: The score set view model, carrying only what this caller may read.
    """
    visible_calibration_ids = {
        calibration.id for calibration in principal.viewer_for(ScoreCalibrationViewer).visible(item.score_calibrations)
    }
    superseding_is_visible = item.superseding_score_set is not None and (
        has_permission(principal.user_data, item.superseding_score_set, Action.READ).permitted
    )

    validated_item = score_set.ScoreSet.model_validate(item)
    return validated_item.model_copy(
        update={
            "experiment": enrich_experiment_with_num_score_sets(item.experiment, principal.user_data),
            "score_calibrations": [
                calibration
                for calibration in (validated_item.score_calibrations or [])
                if calibration.id in visible_calibration_ids
            ],
            "superseding_score_set": validated_item.superseding_score_set if superseding_is_visible else None,
        }
    )


router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Manage and retrieve Score Sets and their associated data.",
    "externalDocs": {
        "description": "Score Sets Documentation",
        "url": "https://mavedb.org/docs/mavedb/record_types.html#score-sets",
    },
}


@router.post(
    "/score-sets/search",
    status_code=200,
    response_model=ScoreSetsSearchResponse,
    summary="Search score sets",
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
)
def search_score_sets(
    search: ScoreSetsSearch,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """
    Search score sets.
    """

    # Disallow searches for unpublished score sets via this endpoint.
    if search.published is False:
        raise HTTPException(
            status_code=422,
            detail="Cannot search for private score sets except in the context of the current user's data.",
        )
    search.published = True

    # Require a limit of at most SCORE_SET_SEARCH_MAX_LIMIT when the search query does not include publication
    # identifiers. We allow unlimited searches with publication identifiers, presuming that such a search will not have
    # excessive results.
    if search.publication_identifiers is None and search.limit is None:
        search.limit = SCORE_SET_SEARCH_MAX_LIMIT
    elif search.publication_identifiers is None and (search.limit is None or search.limit > SCORE_SET_SEARCH_MAX_LIMIT):
        raise HTTPException(
            status_code=422,
            detail=f"Cannot search for more than {SCORE_SET_SEARCH_MAX_LIMIT} score sets at a time. Please use the offset and limit parameters to run a paginated search.",
        )

    # Also limit the search to at most SCORE_SET_SEARCH_MAX_PUBLICATION_IDENTIFIERS publication identifiers, to prevent
    # artificially constructed searches that return very large result sets.
    if (
        search.publication_identifiers is not None
        and len(search.publication_identifiers) > SCORE_SET_SEARCH_MAX_PUBLICATION_IDENTIFIERS
    ):
        raise HTTPException(
            status_code=422,
            detail=f"Cannot search for score sets belonging to more than {SCORE_SET_SEARCH_MAX_PUBLICATION_IDENTIFIERS} publication identifiers at once.",
        )

    score_sets, num_score_sets = _search_score_sets(db, None, search).values()

    # Unconditional, because this enrichment is also what filters the nested experiment's score set URNs by
    # permission. Serializing the ORM experiment directly instead reaches SavedExperiment's score_set_urns
    # validator, which lists every score set on the experiment, disclosing the URNs of private ones.
    enriched_score_sets = []
    for ss in score_sets:
        enriched_experiment = enrich_experiment_with_num_score_sets(ss.experiment, user_data)
        response_item = score_set.ScoreSet.model_validate(ss).copy(update={"experiment": enriched_experiment})
        enriched_score_sets.append(response_item)

    return {"score_sets": enriched_score_sets, "num_score_sets": num_score_sets}


@router.post("/score-sets/search/filter-options", status_code=200, response_model=ScoreSetsSearchFilterOptionsResponse)
def get_filter_options_for_search(
    search: ScoreSetsSearch,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    # Disallow searches for unpublished score sets via this endpoint, consistent with the main search endpoint.
    if search.published is False:
        raise HTTPException(
            status_code=422,
            detail="Cannot search for private score sets options except in the context of the current user's data.",
        )
    search.published = True
    return fetch_score_set_search_filter_options(db, user_data, None, search)


@router.get(
    "/score-sets/mapped-genes",
    status_code=200,
    response_model=dict[str, list[str]],
    summary="Get score set to mapped gene symbol mapping",
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
)
def score_set_mapped_gene_mapping(
    db: Session = Depends(deps.get_db), user_data: UserData = Depends(get_current_user)
) -> Any:
    """
    Get a mapping of score set URNs to mapped gene symbols.
    """
    save_to_logging_context({"requested_resource": "mapped-genes"})

    score_sets_with_mapping_metadata = db.execute(
        select(ScoreSet, TargetGene.post_mapped_metadata)
        .join(ScoreSet)
        .where(TargetGene.post_mapped_metadata.is_not(None))
    ).all()

    mapped_genes: dict[str, list[str]] = {}
    for score_set_item, post_mapped_metadata in score_sets_with_mapping_metadata:
        if not has_permission(user_data, score_set_item, Action.READ).permitted:
            continue

        sequence_genes = [
            *post_mapped_metadata.get("genomic", {}).get("sequence_genes", []),
            *post_mapped_metadata.get("protein", {}).get("sequence_genes", []),
        ]

        if sequence_genes:
            mapped_genes.setdefault(score_set_item.urn, []).extend(sequence_genes)

    return mapped_genes


@router.post(
    "/me/score-sets/search",
    status_code=200,
    summary="Search my score sets",
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    response_model=ScoreSetsSearchResponse,
)
def search_my_score_sets(
    search: ScoreSetsSearch,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Search score sets created by the current user..
    """
    score_sets, num_score_sets = _search_score_sets(db, user_data.user, search).values()
    enriched_score_sets = []
    for ss in score_sets:
        enriched_experiment = enrich_experiment_with_num_score_sets(ss.experiment, user_data)
        response_item = score_set.ScoreSet.model_validate(ss).copy(update={"experiment": enriched_experiment})
        enriched_score_sets.append(response_item)

    return {"score_sets": enriched_score_sets, "num_score_sets": num_score_sets}


RECENTLY_PUBLISHED_SCORE_SETS_MAX_LIMIT = 20


@router.get(
    "/score-sets/recently-published",
    status_code=200,
    response_model=list[score_set.ScoreSet],
    response_model_exclude_none=True,
    summary="List recently published score sets",
)
def list_recently_published_score_sets(
    limit: int = Query(
        default=10,
        ge=1,
        le=RECENTLY_PUBLISHED_SCORE_SETS_MAX_LIMIT,
        description=f"Number of score sets to return (maximum {RECENTLY_PUBLISHED_SCORE_SETS_MAX_LIMIT}).",
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Return the most recently published score sets, ordered by publication date descending.
    """
    save_to_logging_context({"requested_resource": "recently-published", "limit": limit})

    items = (
        db.query(ScoreSet)
        .filter(ScoreSet.published_date.isnot(None), ScoreSet.private.is_(False))
        .order_by(ScoreSet.published_date.desc(), ScoreSet.urn.desc())
        .limit(limit)
        .all()
    )

    return [
        _score_set_response(item, principal) for item in items if has_permission(user_data, item, Action.READ).permitted
    ]


@router.get(
    "/score-sets/",
    status_code=200,
    response_model=list[score_set.ScoreSet],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    response_model_exclude_none=True,
    summary="Fetch score sets by URN list",
)
async def show_score_sets(
    *,
    urns: str = Query(..., description="Comma-separated list of score set URNs"),
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Fetch score sets identified by a list of URNs.
    """
    urn_list = [urn.strip() for urn in urns.split(",") if urn.strip()]
    if not urn_list:
        raise HTTPException(status_code=422, detail="At least one URN is required")

    save_to_logging_context({"requested_resource": urn_list})
    response_items: list[score_set.ScoreSet] = []
    for urn in urn_list:
        item = await fetch_score_set_by_urn(db, urn, user_data, None, False)
        response_items.append(_score_set_response(item, principal))

    return response_items


@router.get(
    "/score-sets/{urn}",
    status_code=200,
    response_model=score_set.ScoreSet,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    response_model_exclude_none=True,
    summary="Fetch score set by URN",
)
async def show_score_set(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Fetch a single score set by URN.
    """
    save_to_logging_context({"requested_resource": urn})
    item = await fetch_score_set_by_urn(db, urn, user_data, None, False)
    return _score_set_response(item, principal)


@router.get(
    "/score-sets/{urn}/csv-namespaces",
    status_code=200,
    response_model=list[AvailableCsvNamespace],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="List the CSV column namespaces this score set has data for",
)
def get_score_set_csv_namespaces(
    *,
    urn: str,
    response: Response,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the offered namespaces as they stood at this instant, so discovery matches an "
            "`as_of` download. ISO 8601, ideally timezone-aware. Defaults to current."
        ),
    ),
) -> Any:
    """
    List the CSV column namespaces this score set has data for, labeled and grouped for a picker.

    Each entry's `namespace` is a value accepted by the `namespaces` parameter of the CSV endpoints.
    Deliberately a separate request rather than a field on the score set: it costs several queries and is
    only needed when a user opens a download dialog, so it should not sit on the score-set page's
    critical path.

    Parameters
    __________
    urn : str
        The URN of the score set to inspect.
    db : Session
        The database session to use.
    user_data : Optional[UserData]
        The user data of the current user. If None, no user-specific permissions are checked.

    Returns
    _______
    list[AvailableCsvNamespace]
        The namespaces with data, each with a human-readable label and group.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "csv-namespaces"})

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(msg="Could not fetch CSV namespaces; No such score set exists.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, score_set, Action.READ)

    # Echoed like the CSV endpoints: the instant discovery answered for is part of the answer.
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"
    return available_score_set_csv_namespaces(
        db,
        score_set,
        viewer=principal.viewer_for(ScoreCalibrationViewer),
        as_of=as_of,
    )


@router.get(
    "/score-sets/{urn}/variants",
    status_code=200,
    response_model=List[LeanVariant],
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Get the lean whole-set variant view for a score set",
)
async def get_score_set_lean_variants(
    *,
    urn: str,
    response: Response,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the annotation layer (mapping, allele links, VEP consequence) as it stood at "
            "this instant, over the score set's fixed scores. ISO 8601, ideally timezone-aware. This is "
            "content valid-time only — it never re-selects a score-set version. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """
    Return the lean whole-set view for a score set: one pre-chewed record per variant carrying the
    selection key (variant URN), score, a representative consequence, the bridge identifiers into the
    annotation dimensions (ClinGen allele id, assay-level digest), and the DNA + protein parsed
    position/ref/alt blocks that drive the heatmap's level toggle.

    The full set is returned in one payload — the score-set page bins/sorts/filters across every
    variant client-side. as_of time-travels the annotation layer only (scores are immutable); the
    resolved value is echoed in the X-As-Of response header so the content-time is a visible fact.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "lean-variants", "as_of": as_of})
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"

    score_set = await fetch_score_set_by_urn(db, urn, user_data, None, False)
    return get_lean_score_set_variants(db, score_set, as_of=as_of)


def _stream_score_set_variant_details(
    db: Session,
    variants: Sequence[Variant],
    superseding_score_set: Optional[ScoreSet],
    visible_calibration_ids: set[int],
    *,
    as_of: Optional[datetime] = None,
):
    """Serialize the whole-set variant-detail export as NDJSON — one :class:`VariantDetail` per line.

    Assembles each variant's detail envelope (the flat assay fields + ``preMapped``/``postMapped`` VRS
    pair + the spec-pure GA4GH CategoricalVariant + the digest-keyed VEP/gnomAD/ClinVar annotation map)
    one at a time, so a large score set streams rather than building every envelope up front.
    ``superseding_score_set`` / ``visible_calibration_ids`` are resolved once for the whole set and
    threaded into every per-variant build.
    """
    for variant in variants:
        detail = get_variant_detail(
            db,
            variant,
            superseding_score_set=superseding_score_set,
            visible_calibration_ids=visible_calibration_ids,
            as_of=as_of,
        )
        # get_variant_detail returns the lib transit dataclass; coerce it through the view model.
        # exclude_none=False keeps a stable key set per line for programmatic consumers.
        yield VariantDetail.model_validate(detail).model_dump_json(by_alias=True, exclude_none=False) + "\n"


@router.get(
    "/score-sets/{urn}/variant-details",
    status_code=200,
    responses={
        200: {
            "content": {"application/x-ndjson": {}},
            "description": (
                "Newline-delimited JSON: one VariantDetail per mapped variant — the same envelope the "
                "single-variant GET /variants/{urn} route serves, carrying the flat preMapped/postMapped "
                "VRS pair, the spec-pure GA4GH CategoricalVariant, and the digest-keyed VEP/gnomAD/ClinVar "
                "annotation map."
            ),
        },
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
    summary="Download a score set's variant details (VRS + Cat-VRS + annotations)",
)
async def get_score_set_variant_details(
    *,
    urn: str,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (VRS, Cat-VRS membership + VEP/gnomAD/ClinVar annotations) "
            "as it stood at this instant, over the score set's fixed scores. ISO 8601, ideally "
            "timezone-aware. Content valid-time only — it never re-selects a score-set version. Defaults "
            "to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """Download the score set's variant details — the whole-set streaming pair of the single-variant
    ``GET /variants/{urn}`` detail endpoint, and the substrate-faithful replacement for the retired
    ``/mapped-variants`` export.

    One record per *mapped* variant (unmapped variants carry no VRS and are omitted): the same
    VariantDetail envelope the single-variant route serves — the flat ``preMapped``/``postMapped`` VRS
    pair for VRS consumers, plus the spec-pure GA4GH CategoricalVariant and the digest-keyed
    VEP/gnomAD/ClinVar annotation map for the full molecular picture.

    Streamed as NDJSON (like the annotated-variant exports) so a large score set downloads without
    materializing every envelope server-side and a client can process it line by line. ``as_of``
    time-travels the molecular layer only (scores/classifications are immutable); the resolved value is
    echoed in ``X-As-Of`` and the variant count in ``X-Total-Count``.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "variant-details", "as_of": as_of})

    score_set = await fetch_score_set_by_urn(db, urn, user_data, None, False)

    # Resolved here rather than inherited: fetch_score_set_by_urn returns the score set as it actually
    # is, leaving narrowing to each response path. Mirrors the single-variant route, which must agree
    # with this one -- they serve the same envelope.
    superseding = score_set.superseding_score_set
    if superseding is not None and not has_permission(user_data, superseding, Action.READ).permitted:
        superseding = None

    # A calibration's READ rule is stricter than its score set's, so reading the score set does not
    # entitle a caller to a private calibration's classifications.
    visible_calibration_ids = {
        sc.id
        for sc in score_set.score_calibrations
        if sc.id is not None and has_permission(user_data, sc, Action.READ).permitted
    }
    variants = get_annotatable_variants(db, score_set, as_of=as_of)
    return StreamingResponse(
        _stream_score_set_variant_details(
            db,
            variants,
            superseding,
            visible_calibration_ids,
            as_of=as_of,
        ),
        media_type="application/x-ndjson",
        headers={
            "X-As-Of": as_of.isoformat() if as_of is not None else "current",
            "X-Total-Count": str(len(variants)),
            "X-Processing-Started": datetime.now().isoformat(),
            "X-Stream-Type": "variant-detail",
            "Access-Control-Expose-Headers": "X-As-Of, X-Total-Count, X-Processing-Started, X-Stream-Type",
        },
    )


@router.get(
    "/score-sets/{urn}/mapped-variants",
    status_code=410,
    deprecated=True,
    responses={410: BASE_RESPONSES[410]},
    summary="Removed; see GET /score-sets/{urn}/variant-details",
)
def get_score_set_mapped_variants_removed(*, urn: str) -> Any:
    """This endpoint has been permanently removed.

    Its JSON-array response has been replaced by a streaming NDJSON payload with a different
    field shape (flat ``preMapped``/``postMapped`` VRS pair rather than a ``MappedVariant``-keyed
    record), so the two are not wire-compatible and this route does not redirect. Use
    ``GET /score-sets/{urn}/variant-details`` instead.
    """
    raise HTTPException(
        status_code=410,
        detail=(
            f"GET /score-sets/{urn}/mapped-variants has been removed. "
            f"Use GET /score-sets/{urn}/variant-details instead."
        ),
    )


@router.get(
    "/score-sets/{urn}/variants/data",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": """Variant data in CSV format, with four fixed columns (accession, hgvs_nt, hgvs_pro,"""
            """ and hgvs_splice), plus score columns defined by the score set.""",
        },
        **BASE_400_RESPONSE,
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
    summary="Get score set variant data in CSV format",
)
def get_score_set_variants_csv(
    *,
    urn: str,
    start: int = Query(default=None, description="Start index for pagination"),
    limit: int = Query(default=None, description="Maximum number of variants to return"),
    namespaces: List[CsvNamespaceStr] = Query(
        default=["scores"],
        description=CSV_NAMESPACES_PARAM_DESCRIPTION,
    ),
    drop_unused_hgvs_columns: Optional[bool] = None,
    drop_na_columns: Optional[bool] = Query(default=None, deprecated=True, description=DROP_NA_COLUMNS_DESCRIPTION),
    include_post_mapped_hgvs: Optional[bool] = Query(
        default=None, deprecated=True, description=INCLUDE_POST_MAPPED_HGVS_DESCRIPTION
    ),
    include_custom_columns: Optional[bool] = Query(
        default=None, deprecated=True, description=INCLUDE_CUSTOM_COLUMNS_DESCRIPTION
    ),
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the annotation layer (post-mapped HGVS, VEP, gnomAD, ClinVar) as it stood at this "
            "instant, over the variant's immutable submitted HGVS/scores/counts. ISO 8601, ideally "
            "timezone-aware. No effect on the scores/counts namespaces. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Return tabular variant data from a score set, identified by URN, in CSV format.

    This differs from get_score_set_scores_csv() in that it returns only the HGVS columns, score column, and mapped HGVS
    string.

    Parameters
    __________
    urn : str
        The URN of the score set to fetch variants from.
    start : Optional[int]
        The index to start from. If None, starts from the beginning.
    limit : Optional[int]
        The maximum number of variants to return. If None, returns all variants.
    namespaces: List[str]
        The namespaces of all columns except for accession, hgvs_nt, hgvs_pro, and hgvs_splice.
        Supported values: "scores" (the required score column), "scores_custom" (the investigator's
        remaining score columns, emitted under the "scores" prefix), "counts", "mavedb", "vep", "gnomad",
        "clingen", "score_set", and ClinVar- and calibration-parameterized namespaces. Multiple ClinVar
        and calibration namespaces may be requested simultaneously.
    drop_unused_hgvs_columns : bool, optional
        Whether to omit the HGVS coordinate columns this score set does not use, e.g. hgvs_nt for a
        protein-only score set. Defaults to False.
    drop_na_columns : bool, optional
        Deprecated spelling of drop_unused_hgvs_columns, accepted for one release.
    include_post_mapped_hgvs : bool, optional
        Deprecated: equivalent to requesting the "mavedb" namespace. Accepted for one release.
    include_custom_columns : bool, optional
        Deprecated: equivalent to requesting the "scores_custom" namespace. Accepted for one release.
    db : Session
        The database session to use.
    user_data : Optional[UserData]
        The user data of the current user. If None, no user-specific permissions are checked.

    Returns
    _______
    str
        The CSV string containing the variant data.
    """
    deprecated = resolve_deprecated_csv_params(
        namespaces=namespaces,
        drop_unused_hgvs_columns=drop_unused_hgvs_columns,
        drop_na_columns=drop_na_columns,
        include_post_mapped_hgvs=include_post_mapped_hgvs,
        include_custom_columns=include_custom_columns,
    )
    namespaces = deprecated.namespaces
    drop_unused_hgvs_columns = deprecated.drop_unused_hgvs_columns

    save_to_logging_context(
        {
            "requested_resource": urn,
            "resource_property": "scores",
            "start": start,
            "limit": limit,
            "drop_na_columns": drop_na_columns,
            "as_of": as_of,
            "drop_unused_hgvs_columns": drop_unused_hgvs_columns,
        }
    )

    if start and start < 0:
        logger.info(msg="Could not fetch scores with negative start index.", extra=logging_context())
        raise HTTPException(status_code=422, detail="Start index must be non-negative")
    if limit is not None and limit <= 0:
        logger.info(msg="Could not fetch scores with non-positive limit.", extra=logging_context())
        raise HTTPException(status_code=422, detail="Limit must be positive")

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(msg="Could not fetch the requested scores; No such score set exists.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, score_set, Action.READ)

    csv_str = get_score_set_variants_as_csv(
        db,
        score_set,
        namespaces,
        namespaced=True,
        start=start,
        limit=limit,
        drop_unused_hgvs_columns_flag=drop_unused_hgvs_columns,
        as_of=as_of,
        # Asked separately from the score set: a private calibration is readable only by its owner,
        # investigator contributors, or an admin, whoever can read the score set.
        viewer=principal.viewer_for(ScoreCalibrationViewer),
    )
    # Both: the deprecation notice (when a legacy parameter was used) and the resolved content-time,
    # so a CSV's as-of instant is a visible fact rather than something the caller has to remember.
    return StreamingResponse(
        iter([csv_str]),
        media_type="text/csv",
        headers={
            **deprecated.response_headers,
            "X-As-Of": as_of.isoformat() if as_of is not None else "current",
            "Access-Control-Expose-Headers": "X-As-Of",
        },
    )


@router.get(
    "/score-sets/{urn}/scores",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": """Variant scores in CSV format, with four fixed columns (accession, hgvs_nt, hgvs_pro,"""
            """ and hgvs_splice), plus score columns defined by the score set.""",
        },
        **BASE_400_RESPONSE,
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
    summary="Get score set scores in CSV format",
)
def get_score_set_scores_csv(
    *,
    urn: str,
    start: int = Query(default=None, description="Start index for pagination"),
    limit: int = Query(default=None, description="Number of variants to return"),
    drop_unused_hgvs_columns: Optional[bool] = None,
    drop_na_columns: Optional[bool] = Query(default=None, deprecated=True, description=DROP_NA_COLUMNS_DESCRIPTION),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """
    Return scores from a score set, identified by URN, in CSV format.
    If no start and limit, all of variants of this score set will be returned.
    Example path:
    /score-sets/{urn}/scores
    /score-sets/{urn}/scores?start=0&limit=100
    /score-sets/{urn}/scores?start=100
    """
    deprecated = resolve_deprecated_csv_params(
        drop_unused_hgvs_columns=drop_unused_hgvs_columns, drop_na_columns=drop_na_columns
    )
    drop_unused_hgvs_columns = deprecated.drop_unused_hgvs_columns

    save_to_logging_context(
        {
            "requested_resource": urn,
            "resource_property": "scores",
            "start": start,
            "limit": limit,
        }
    )

    if start and start < 0:
        logger.info(msg="Could not fetch scores with negative start index.", extra=logging_context())
        raise HTTPException(status_code=400, detail="Start index must be non-negative")
    if limit is not None and limit <= 0:
        logger.info(msg="Could not fetch scores with non-positive limit.", extra=logging_context())
        raise HTTPException(status_code=400, detail="Limit must be positive")

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(msg="Could not fetch the requested scores; No such score set exists.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, score_set, Action.READ)

    # Both score namespaces: this endpoint has always returned every score column the investigator
    # uploaded, and `scores` alone is now just the required one.
    csv_str = get_score_set_variants_as_csv(
        db, score_set, ["scores", "scores_custom"], False, start, limit, drop_unused_hgvs_columns
    )
    return StreamingResponse(iter([csv_str]), media_type="text/csv", headers=deprecated.response_headers)


@router.get(
    "/score-sets/{urn}/counts",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": """Variant counts in CSV format, with four fixed columns (accession, hgvs_nt, hgvs_pro,"""
            """ and hgvs_splice), plus score columns defined by the score set.""",
        },
        **BASE_400_RESPONSE,
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
    summary="Get score set counts in CSV format",
)
async def get_score_set_counts_csv(
    *,
    urn: str,
    start: int = Query(default=None, description="Start index for pagination"),
    limit: int = Query(default=None, description="Number of variants to return"),
    drop_unused_hgvs_columns: Optional[bool] = None,
    drop_na_columns: Optional[bool] = Query(default=None, deprecated=True, description=DROP_NA_COLUMNS_DESCRIPTION),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """
    Return counts from a score set, identified by URN, in CSV format.
    If no start and limit, all of variants of this score set will be returned.
    Example path:
    /score-sets/{urn}/counts
    /score-sets/{urn}/counts?start=0&limit=100
    /score-sets/{urn}/counts?start=100
    """
    deprecated = resolve_deprecated_csv_params(
        drop_unused_hgvs_columns=drop_unused_hgvs_columns, drop_na_columns=drop_na_columns
    )
    drop_unused_hgvs_columns = deprecated.drop_unused_hgvs_columns

    save_to_logging_context(
        {
            "requested_resource": urn,
            "resource_property": "counts",
            "start": start,
            "limit": limit,
        }
    )

    if start and start < 0:
        logger.info(msg="Could not fetch counts with negative start index.", extra=logging_context())
        raise HTTPException(status_code=400, detail="Start index must be non-negative")
    if limit is not None and limit <= 0:
        logger.info(msg="Could not fetch counts with non-positive limit.", extra=logging_context())
        raise HTTPException(status_code=400, detail="Limit must be positive")

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(msg="Could not fetch the requested counts; No such score set exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")

    assert_permission(user_data, score_set, Action.READ)

    csv_str = get_score_set_variants_as_csv(db, score_set, ["counts"], False, start, limit, drop_unused_hgvs_columns)
    return StreamingResponse(iter([csv_str]), media_type="text/csv", headers=deprecated.response_headers)


def _annotation_stream_record(
    db, variant, annotation_function, as_of=None
) -> tuple[dict, Literal["annotated", "unannotated", "errored"]]:
    """
    Build the NDJSON record for one variant, and classify its outcome.

    Returns the record together with one of ``annotated``, ``unannotated``, or ``errored``. Nothing raises:
    a failure past the first record would end the body mid-stream, and since the 200 and its headers went
    out with the first chunk the consumer has no way to be told and simply receives a short file. A failed
    variant is reported in-band instead, as a record carrying an ``error`` object. This includes variants
    failing serialization.

    A variant with nothing to annotate is *not* an error. It is an expected absence, reported as a null
    annotation: either no annotation context could be built from the mapping substrate at ``as_of``, or
    the builder declined the variant. Which exceptions mean that is defined once, in
    ``EXPECTED_ABSENCE_EXCEPTIONS``, because the corpus sweep has to draw the same line and the two must
    not drift apart.
    """
    variant_urn = variant.urn

    try:
        context = variant_annotation_context(db, variant, as_of=as_of)
        annotation = annotation_function(context) if context is not None else None
        annotation_data = annotation.model_dump(exclude_none=True) if annotation else None
    except EXPECTED_ABSENCE_EXCEPTIONS:
        logger.debug(f"Nothing to annotate for variant {variant_urn}.")
        return {"variant_urn": variant_urn, "annotation": None}, "unannotated"
    except Exception as err:
        logger.exception(
            f"Failed to annotate variant {variant_urn}; streaming it as an error record.",
            extra=logging_context(),
        )
        return {
            "variant_urn": variant_urn,
            "annotation": None,
            "error": {"type": type(err).__name__, "detail": str(err)},
        }, "errored"

    if annotation_data is None:
        return {"variant_urn": variant_urn, "annotation": None}, "unannotated"

    return {"variant_urn": variant_urn, "annotation": annotation_data}, "annotated"


def _stream_generated_annotations(db, variants, annotation_function, as_of=None):
    """
    Generator function to stream annotations as pure NDJSON data.

    Builds each variant's :class:`VariantAnnotationContext` from the mapping-record substrate and passes it
    to ``annotation_function``; variants with no live mapping data yield a null annotation.

    Metadata should be provided via HTTP headers:
    - X-Total-Count: Total number of variants
    - X-Processing-Started: ISO timestamp when processing began
    - X-Stream-Type: Type of annotation being streamed

    Emits exactly one record per annotatable variant, so a body holding fewer lines than ``X-Total-Count`` is
    a truncated one. Outcome counts are logged rather than appended to the body, which keeps every line a
    variant record and keeps this format identical to the public dump's ``va/{urn}.va.ndjson``.

    Progress updates are sent as structured log events that can be
    consumed via Server-Sent Events if needed.
    """
    start_time = time.time()
    total_variants = len(variants)
    processed_count = 0
    outcome_counts = {"annotated": 0, "unannotated": 0, "errored": 0}
    logger.info(f"Starting streaming processing of {total_variants} variants")

    for variant in variants:
        result, outcome = _annotation_stream_record(db, variant, annotation_function, as_of=as_of)
        outcome_counts[outcome] += 1

        yield json.dumps(result, default=str) + "\n"

        # Log server-side progress
        processed_count += 1
        if processed_count % (total_variants // 10 + 1) == 0:
            current_time = time.time()
            elapsed = current_time - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            percentage = (processed_count / total_variants) * 100
            eta = (total_variants - processed_count) / rate if rate > 0 else 0

            logger.debug(
                f"Streamed {processed_count}/{total_variants} variants ({rate:.1f}/sec, {percentage:.1f}% complete, ETA: {eta:.1f}s)",
                extra=logging_context(),
            )

    # Log final completion summary
    end_time = time.time()
    total_time = end_time - start_time
    average_time_per_variant = round(total_time / processed_count, 4) if processed_count > 0 else 0
    final_rate = round(processed_count / total_time, 1) if total_time > 0 else 0

    save_to_logging_context(
        {
            "stream_completion": {
                "total_processed": processed_count,
                **outcome_counts,
                "total_time": round(total_time, 2),
                "average_time_per_variant": average_time_per_variant,
                "final_rate": final_rate,
                "timestamp": end_time,
            }
        }
    )
    logger.info(
        f"Completed streaming {processed_count} variants in {total_time:.2f} seconds "
        f"({outcome_counts['errored']} errored, avg: {average_time_per_variant:.4f}s/variant)",
        extra=logging_context(),
    )


@router.get(
    "/score-sets/{urn}/annotated-variants/pathogenicity-statement",
    status_code=200,
    response_model=dict[str, Optional[VariantPathogenicityStatement]],
    response_model_exclude_none=True,
    summary="Get pathogenicity statement annotations for variants within a score set",
    responses={
        200: {
            "content": {"application/x-ndjson": {}},
            "description": "Stream pathogenicity statement annotations for variants.",
        },
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
)
def get_score_set_annotated_variants(
    *,
    urn: str,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (Cat-VRS membership, VEP/gnomAD/ClinVar annotations) as it "
            "stood at this instant, over the variant's fixed score. ISO 8601, ideally timezone-aware. "
            "Content valid-time only — it never re-selects a score-set version, and scores/classifications "
            "are as-of-invariant. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Retrieve annotated variants with pathogenicity statements for a given score set.

    This endpoint streams pathogenicity evidence lines for all current annotated variants
    associated with a specific score set. The response is returned as newline-delimited
    JSON (NDJSON) format for efficient processing of large datasets.

    NDJSON Response Format:
        Each line in the response corresponds to an annotated variant and contains a JSON
        object with the following structure:
        ```
        {
            "variant_urn": "<URN of the annotated variant>",
            "annotation": {
                ... // Pathogenicity evidence line details
            }
        }
        ```

        `annotation` is null where the variant has no mapping data to annotate, or no pathogenicity statements apply
        to it. A variant whose annotation could not be built is reported in-band rather than by
        truncating the stream, and carries an additional `error` object:
        ```
        {
            "variant_urn": "<URN of the annotated variant>",
            "annotation": null,
            "error": {"type": "<exception class>", "detail": "<exception message>"}
        }
        ```

        Every line is a variant record: a response holds exactly `X-Total-Count` lines, so a shorter
        body is a truncated one.

    Args:
        urn (str): The Uniform Resource Name (URN) of the score set to retrieve
            annotated variants for.
        db (Session, optional): Database session dependency. Defaults to Depends(deps.get_db).
        user_data (Optional[UserData], optional): Current user data for permission checking.
            Defaults to Depends(get_current_user).

    Returns:
        Any: StreamingResponse containing newline-delimited JSON with pathogenicity
            evidence lines for each annotated variant. Response includes headers with
            total count, processing start time, and stream type information.

    A score set that exists but has no annotatable variants (never mapped, or none live at ``as_of``)
    streams an empty body with ``X-Total-Count: 0`` — an empty collection, not a 404.

    Raises:
        HTTPException: 404 error if the score set with the given URN is not found.
        HTTPException: 403 error if the user lacks READ permissions for the score set.

    Note:
        This function logs the request context and validates user permissions before
        processing. Use the `as_of` parameter to reconstruct the molecular layer as it stood at a specific
        instant, over the variant's fixed score. The response is streamed to allow for efficient handling
        of large datasets, and progress updates are logged for monitoring purposes.
    """
    save_to_logging_context(
        {
            "requested_resource": urn,
            "resource_property": "annotated-variants/pathogenicity-statement",
            "as_of": as_of,
        }
    )

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(
            msg="Could not fetch the requested pathogenicity evidence lines; No such score set exists.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")

    assert_permission(user_data, score_set, Action.READ)

    variants = get_annotatable_variants(db, score_set, as_of=as_of)

    # An existing score set with no annotatable variants (never mapped, or none live at as_of) streams an
    # empty NDJSON body with X-Total-Count: 0 — an empty collection, not a 404. 404 is reserved for an
    # unresolvable URN or a permission failure, never for a filter (as_of) matching nothing.
    return StreamingResponse(
        _stream_generated_annotations(
            db, variants, partial(variant_pathogenicity_statement, principal=principal), as_of=as_of
        ),
        media_type="application/x-ndjson",
        headers={
            "X-As-Of": as_of.isoformat() if as_of is not None else "current",
            "X-Total-Count": str(len(variants)),
            "X-Processing-Started": datetime.now().isoformat(),
            "X-Stream-Type": "pathogenicity-evidence-line",
            "Access-Control-Expose-Headers": "X-Total-Count, X-Processing-Started, X-Stream-Type",
        },
    )


@router.get(
    "/score-sets/{urn}/annotated-variants/functional-statement",
    status_code=200,
    response_model=dict[str, Optional[Statement]],
    response_model_exclude_none=True,
    summary="Get functional impact statement annotations for annotated variants within a score set",
    responses={
        200: {
            "content": {"application/x-ndjson": {}},
            "description": "Stream functional impact statement annotations for annotated variants.",
        },
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
)
def get_score_set_annotated_variants_functional_statement(
    *,
    urn: str,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (Cat-VRS membership, VEP/gnomAD/ClinVar annotations) as it "
            "stood at this instant, over the variant's fixed score. ISO 8601, ideally timezone-aware. "
            "Content valid-time only — it never re-selects a score-set version, and scores/classifications "
            "are as-of-invariant. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
    principal: Principal = Depends(get_principal),
):
    """
    Retrieve functional impact statements for annotated variants in a score set.

    This endpoint streams functional impact statements for all current annotated variants
    associated with a specific score set. The response is delivered as newline-delimited
    JSON (NDJSON) format.

    NDJSON Response Format:
        Each line in the response corresponds to an annotated variant and contains a JSON
        object with the following structure:
        ```
        {
            "variant_urn": "<URN of the annotated variant>",
            "annotation": {
                ... // Functional impact statement details
            }
        }
        ```

        `annotation` is null where the variant has no mapping data to annotate, or no functional impact statements apply
        to it. A variant whose annotation could not be built is reported in-band rather than by
        truncating the stream, and carries an additional `error` object:
        ```
        {
            "variant_urn": "<URN of the annotated variant>",
            "annotation": null,
            "error": {"type": "<exception class>", "detail": "<exception message>"}
        }
        ```

        Every line is a variant record: a response holds exactly `X-Total-Count` lines, so a shorter
        body is a truncated one.

    Args:
        urn (str): The unique resource name (URN) identifying the score set.
        db (Session): Database session dependency for querying data.
        user_data (Optional[UserData]): Current authenticated user data for permission checks.

    Returns:
        StreamingResponse: NDJSON stream containing functional impact statements for each
            annotated variant. Response includes headers with total count, processing start time,
            and stream type information.

    Raises:
        HTTPException:
            - 404 if the score set with the given URN is not found
            - 404 if no annotated variants are associated with the score set
            - 403 if the user lacks READ permission for the score set

    Note:
        The function requires appropriate read permissions on the score set. Use the `as_of`
        parameter to reconstruct the molecular layer as it stood at a specific instant, over
        the variant's fixed score. The response is streamed to allow for efficient handling of
        large datasets, and progress updates are logged for monitoring purposes.
    """
    save_to_logging_context(
        {
            "requested_resource": urn,
            "resource_property": "annotated-variants/functional-statement",
            "as_of": as_of,
        }
    )

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(
            msg="Could not fetch the requested functional impact statements; No such score set exists.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")

    assert_permission(user_data, score_set, Action.READ)

    variants = get_annotatable_variants(db, score_set, as_of=as_of)

    # An existing score set with no annotatable variants (never mapped, or none live at as_of) streams an
    # empty NDJSON body with X-Total-Count: 0 — an empty collection, not a 404. 404 is reserved for an
    # unresolvable URN or a permission failure, never for a filter (as_of) matching nothing.
    return StreamingResponse(
        _stream_generated_annotations(
            db, variants, partial(variant_functional_impact_statement, principal=principal), as_of=as_of
        ),
        media_type="application/x-ndjson",
        headers={
            "X-As-Of": as_of.isoformat() if as_of is not None else "current",
            "X-Total-Count": str(len(variants)),
            "X-Processing-Started": datetime.now().isoformat(),
            "X-Stream-Type": "functional-impact-statement",
            "Access-Control-Expose-Headers": "X-Total-Count, X-Processing-Started, X-Stream-Type",
        },
    )


@router.get(
    "/score-sets/{urn}/annotated-variants/study-result",
    status_code=200,
    response_model=dict[str, Optional[ExperimentalVariantFunctionalImpactStudyResult]],
    response_model_exclude_none=True,
    summary="Get functional study result annotations for annotated variants within a score set",
    responses={
        200: {
            "content": {"application/x-ndjson": {}},
            "description": "Stream functional study result annotations for annotated variants.",
        },
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
)
def get_score_set_annotated_variants_functional_study_result(
    *,
    urn: str,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (Cat-VRS membership, VEP/gnomAD/ClinVar annotations) as it "
            "stood at this instant, over the variant's fixed score. ISO 8601, ideally timezone-aware. "
            "Content valid-time only — it never re-selects a score-set version, and scores/classifications "
            "are as-of-invariant. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
):
    """
    Retrieve functional study results for annotated variants in a score set.

    This endpoint streams functional study result annotations for all current annotated variants
    associated with a specific score set. The results are returned as newline-delimited JSON
    (NDJSON) format for efficient streaming of large datasets.

    NDJSON Response Format:
        Each line in the response corresponds to a annotated variant and contains a JSON
        object with the following structure:
        ```
        {
            "variant_urn": "<URN of the annotated variant>",
            "annotation": {
                ... // Functional study result details
            }
        }
        ```

        `annotation` is null where the variant has no mapping data to annotate, or no study results apply
        to it. A variant whose annotation could not be built is reported in-band rather than by
        truncating the stream, and carries an additional `error` object:
        ```
        {
            "variant_urn": "<URN of the annotated variant>",
            "annotation": null,
            "error": {"type": "<exception class>", "detail": "<exception message>"}
        }
        ```

        Every line is a variant record: a response holds exactly `X-Total-Count` lines, so a shorter
        body is a truncated one.

    Args:
        urn (str): The URN (Uniform Resource Name) of the score set to retrieve variants for.
        db (Session): Database session dependency for querying the database.
        user_data (Optional[UserData]): Current user data for permission validation.

    Returns:
        StreamingResponse: A streaming response containing functional study results in NDJSON format.
            Headers include:
            - X-Total-Count: Total number of annotated variants being streamed
            - X-Processing-Started: ISO timestamp when processing began
            - X-Stream-Type: Set to "functional-study-result"
            - Access-Control-Expose-Headers: Exposed headers for CORS

    Raises:
        HTTPException:
            - 404 if the score set with the given URN is not found
            - 404 if no annotated variants are associated with the score set
            - 403 if the user lacks READ permission for the score set

    Notes:
        - The `as_of` parameter allows reconstruction of the molecular layer as it stood at a specific
          instant, over the variant's fixed score. It is ISO 8601 formatted and ideally timezone-aware.
        - The response is streamed to allow for efficient handling of large datasets, and progress updates
          are logged for monitoring purposes.
    """
    save_to_logging_context(
        {
            "requested_resource": urn,
            "resource_property": "annotated-variants/study-result",
            "as_of": as_of,
        }
    )

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(
            msg="Could not fetch the requested functional study results; No such score set exists.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")

    assert_permission(user_data, score_set, Action.READ)

    variants = get_annotatable_variants(db, score_set, as_of=as_of)

    # An existing score set with no annotatable variants (never mapped, or none live at as_of) streams an
    # empty NDJSON body with X-Total-Count: 0 — an empty collection, not a 404. 404 is reserved for an
    # unresolvable URN or a permission failure, never for a filter (as_of) matching nothing.
    return StreamingResponse(
        _stream_generated_annotations(db, variants, variant_study_result, as_of=as_of),
        media_type="application/x-ndjson",
        headers={
            "X-As-Of": as_of.isoformat() if as_of is not None else "current",
            "X-Total-Count": str(len(variants)),
            "X-Processing-Started": datetime.now().isoformat(),
            "X-Stream-Type": "functional-study-result",
            "Access-Control-Expose-Headers": "X-Total-Count, X-Processing-Started, X-Stream-Type",
        },
    )


@router.post(
    "/score-sets/",
    response_model=score_set.ScoreSet,
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES, **BASE_409_RESPONSE, **GATEWAY_ERROR_RESPONSES},
    summary="Create a score set",
)
async def create_score_set(
    *,
    item_create: score_set.ScoreSetCreate,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Create a score set.
    """
    logger.debug(msg="Began score set creation.", extra=logging_context())

    experiment: Optional[Experiment] = None
    if item_create.experiment_urn is not None:
        experiment = db.query(Experiment).filter(Experiment.urn == item_create.experiment_urn).one_or_none()
        if not experiment:
            logger.info(
                msg="Failed to create score set; The requested experiment does not exist.", extra=logging_context()
            )
            raise HTTPException(status_code=404, detail="The requested experiment does not exist")
        # Not allow add score set in meta-analysis experiments.
        if any(s.meta_analyzes_score_sets for s in experiment.score_sets):
            raise HTTPException(
                status_code=409,
                detail="Score sets may not be added to a meta-analysis experiment.",
            )

        save_to_logging_context({"experiment": experiment.urn})
        assert_permission(user_data, experiment, Action.ADD_SCORE_SET)

    license_ = db.query(License).filter(License.id == item_create.license_id).one_or_none()
    save_to_logging_context({"requested_license": item_create.license_id})

    if not license_:
        logger.info(msg="Failed to create score set; The requested license does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail="The requested license does not exist")
    elif not license_.active:
        logger.info(
            msg="Failed to create score set; The requested license is no longer active.", extra=logging_context()
        )
        raise HTTPException(
            status_code=409,
            detail="Invalid license. The requested license is not active and may no longer be attached to score sets.",
        )

    save_to_logging_context({"requested_superseded_score_set": item_create.superseded_score_set_urn})
    if item_create.superseded_score_set_urn is not None:
        # Passing user_data as owner_or_contributor is what authorizes the supersession: the fetch returns
        # only published score sets this user owns or contributes to. There is no Action for supersession yet.
        superseded_score_set = await fetch_score_set_by_urn(
            db, item_create.superseded_score_set_urn, user_data, user_data, True
        )

        if superseded_score_set is None:
            logger.info(
                msg="Failed to create score set; The requested superseded score set does not exist.",
                extra=logging_context(),
            )
            raise HTTPException(
                status_code=404,
                detail="The requested superseded score set does not exist",
            )

        if superseded_score_set.superseding_score_set:
            logger.info(
                msg=f"Failed to create score set. This score set has been superseded by score set: {superseded_score_set.superseding_score_set.urn}.",
                extra=logging_context(),
            )
            raise HTTPException(
                status_code=409,
                detail=f"This score set has been superseded by score set: {superseded_score_set.superseding_score_set.urn}.",
            )
    else:
        superseded_score_set = None

    distinct_meta_analyzes_score_set_urns = list(set(item_create.meta_analyzes_score_set_urns or []))
    meta_analyzes_score_sets = [
        ss
        for ss in [
            await fetch_score_set_by_urn(db, urn, user_data, None, True)
            for urn in distinct_meta_analyzes_score_set_urns
        ]
        if ss is not None
    ]

    save_to_logging_context({"requested_meta_analyzes_score_sets": distinct_meta_analyzes_score_set_urns})
    for i, meta_analyzes_score_set in enumerate(meta_analyzes_score_sets):
        if meta_analyzes_score_set is None:
            logger.info(
                msg=f"Failed to create score set; The requested meta analyzed score set ({distinct_meta_analyzes_score_set_urns[i]}) does not exist.",
                extra=logging_context(),
            )
            raise HTTPException(
                status_code=404,
                detail=f"Unknown meta-analyzed score set {distinct_meta_analyzes_score_set_urns[i]}",
            )

    if len(meta_analyzes_score_sets) > 0:
        # If any existing score set is a meta-analysis for score sets in the same collection of experiment sets, use its
        # experiment as the parent of our new meta-analysis. Otherwise, create a new experiment.
        meta_analyzes_experiment_sets = list(
            set(
                (
                    ss.experiment.experiment_set
                    for ss in meta_analyzes_score_sets
                    if ss.experiment.experiment_set is not None
                )
            )
        )
        meta_analyzes_experiment_set_urns = [es.urn for es in meta_analyzes_experiment_sets if es.urn is not None]
        existing_meta_analyses = find_meta_analyses_for_experiment_sets(db, meta_analyzes_experiment_set_urns)

        if len(existing_meta_analyses) > 0:
            experiment = existing_meta_analyses[0].experiment
        elif len(meta_analyzes_experiment_sets) == 1:
            # The analyzed score sets all belong to one experiment set, so the meta-analysis should go in that
            # experiment set's meta-analysis experiment. But there is no meta-analysis experiment (or else we would
            # have found it by looking at existing_meta_analyses[0].experiment), so we will create one.
            meta_analyzes_experiment_set = meta_analyzes_experiment_sets[0]
            experiment = Experiment(
                experiment_set=meta_analyzes_experiment_set,
                title=item_create.title,
                short_description=item_create.short_description,
                abstract_text=item_create.abstract_text,
                method_text=item_create.method_text,
                extra_metadata={},
                created_by=user_data.user,
                modified_by=user_data.user,
            )
        else:
            experiment = Experiment(
                title=item_create.title,
                short_description=item_create.short_description,
                abstract_text=item_create.abstract_text,
                method_text=item_create.method_text,
                extra_metadata={},
                created_by=user_data.user,
                modified_by=user_data.user,
            )

        save_to_logging_context({"meta_analysis_experiment": experiment.urn})
        logger.debug(msg="Creating experiment within meta analysis experiment.", extra=logging_context())

    contributors: list[Contributor] = []
    try:
        contributors = [
            await find_or_create_contributor(db, contributor.orcid_id) for contributor in item_create.contributors or []
        ]
    except NonexistentOrcidUserError as e:
        logger.error(msg="Could not find ORCID user with the provided user ID.", extra=logging_context())
        raise HTTPException(status_code=404, detail=str(e))

    try:
        doi_identifiers = [
            await find_or_create_doi_identifier(db, identifier.identifier)
            for identifier in item_create.doi_identifiers or []
        ]
        primary_publication_identifiers = [
            await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
            for identifier in item_create.primary_publication_identifiers or []
        ]
        publication_identifiers = [
            await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
            for identifier in item_create.secondary_publication_identifiers or []
        ] + primary_publication_identifiers

    except requests.exceptions.ConnectTimeout:
        logger.error(msg="Gateway timed out while creating experiment identifiers.", extra=logging_context())
        raise HTTPException(
            status_code=504,
            detail="Gateway Timeout while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
        )

    except requests.exceptions.HTTPError:
        logger.error(msg="Encountered bad gateway while creating experiment identifiers.", extra=logging_context())
        raise HTTPException(
            status_code=502,
            detail="Bad Gateway while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
        )

    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    primary_identifiers = [pub.identifier for pub in primary_publication_identifiers]
    for publication in publication_identifiers:
        setattr(publication, "primary", publication.identifier in primary_identifiers)

    score_calibrations: list[ScoreCalibration] = []
    if item_create.score_calibrations:
        for calibration_create in item_create.score_calibrations:
            # TODO#592: Support for class-based calibrations on score set creation
            if calibration_create.class_based:
                logger.info(
                    msg="Failed to create score set; Class-based calibrations are not supported on score set creation.",
                    extra=logging_context(),
                )
                raise HTTPException(
                    status_code=409,
                    detail="Class-based calibrations are not supported on score set creation. Please create class-based calibrations after creating the score set.",
                )

            created_calibration_item = await create_score_calibration(
                db, calibration_create, user_data.user, variant_classes=None
            )
            created_calibration_item.investigator_provided = True  # necessarily true on score set creation
            score_calibrations.append(created_calibration_item)

    targets: list[TargetGene] = []
    accessions = False
    for gene in item_create.target_genes:
        if gene.target_sequence:
            if accessions and len(targets) > 0:
                logger.info(
                    msg="Failed to create score set; Both a sequence and accession based target were detected.",
                    extra=logging_context(),
                )
                raise MixedTargetError(
                    "MaveDB does not support score-sets with both sequence and accession based targets. Please re-submit this scoreset using only one type of target."
                )
            upload_taxonomy = gene.target_sequence.taxonomy
            save_to_logging_context({"requested_taxonomy": gene.target_sequence.taxonomy.code})
            taxonomy = await find_or_create_taxonomy(db, upload_taxonomy)

            if not taxonomy:
                logger.info(
                    msg="Failed to create score set; The requested taxonomy does not exist.", extra=logging_context()
                )
                raise HTTPException(status_code=404, detail="The requested taxonomy does not exist")

            # If the target sequence has a label, use it. Otherwise, use the name from the target gene as the label.
            # View model validation rules enforce that sequences must have a label defined if there are more than one
            # targets defined on a score set.
            seq_label = gene.target_sequence.label if gene.target_sequence.label is not None else gene.name

            target_sequence = TargetSequence(
                **jsonable_encoder(gene.target_sequence, by_alias=False, exclude={"taxonomy", "label"}),
                taxonomy=taxonomy,
                label=seq_label,
            )
            target_gene = TargetGene(
                **jsonable_encoder(
                    gene,
                    by_alias=False,
                    exclude={
                        "external_identifiers",
                        "target_sequence",
                        "target_accession",
                    },
                ),
                target_sequence=target_sequence,
            )

        elif gene.target_accession:
            if not accessions and len(targets) > 0:
                logger.info(
                    msg="Failed to create score set; Both a sequence and accession based target were detected.",
                    extra=logging_context(),
                )

                raise MixedTargetError(
                    "MaveDB does not support score-sets with both sequence and accession based targets. Please re-submit this scoreset using only one type of target."
                )
            accessions = True
            target_accession = TargetAccession(**jsonable_encoder(gene.target_accession, by_alias=False))
            target_gene = TargetGene(
                **jsonable_encoder(
                    gene,
                    by_alias=False,
                    exclude={
                        "external_identifiers",
                        "target_sequence",
                        "target_accession",
                    },
                ),
                target_accession=target_accession,
            )
        else:
            save_to_logging_context({"failing_target": gene})
            logger.info(msg="Failed to create score set; Could not infer target type.", extra=logging_context())
            raise ValueError("One of either `target_accession` or `target_gene` should be present")

        for external_gene_identifier_offset_create in gene.external_identifiers:
            offset = external_gene_identifier_offset_create.offset
            identifier_create = external_gene_identifier_offset_create.identifier
            await create_external_gene_identifier_offset(
                db,
                target_gene,
                identifier_create.db_name,
                identifier_create.identifier,
                offset,
            )

        targets.append(target_gene)

    assert experiment is not None

    item = ScoreSet(
        **jsonable_encoder(
            item_create,
            by_alias=False,
            exclude={
                "contributors",
                "doi_identifiers",
                "experiment_urn",
                "license_id",
                "meta_analyzes_score_set_urns",
                "primary_publication_identifiers",
                "secondary_publication_identifiers",
                "superseded_score_set_urn",
                "target_genes",
                "score_calibrations",
            },
        ),
        experiment=experiment,
        license=license_,
        superseded_score_set=superseded_score_set,
        meta_analyzes_score_sets=meta_analyzes_score_sets,
        target_genes=targets,
        contributors=contributors,
        doi_identifiers=doi_identifiers,
        publication_identifiers=publication_identifiers,
        processing_state=ProcessingState.incomplete,
        created_by=user_data.user,
        modified_by=user_data.user,
        score_calibrations=score_calibrations,
    )  # type: ignore[call-arg]

    try:
        db.add(item)
        db.commit()
    except IntegrityError as e:
        db.rollback()
        if is_replaces_id_unique_violation(e):
            raise HTTPException(
                status_code=409,
                detail="The requested score set has already been superseded.",
            )
        raise
    db.refresh(item)

    save_to_logging_context({"created_resource": item.urn})

    return _score_set_response(item, principal)


@router.post(
    "/score-sets/{urn}/variants/data",
    response_model=score_set.ScoreSet,
    response_model_exclude_none=True,
    responses={**BASE_400_RESPONSE, **ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Upload score and variant count files for a score set",
    openapi_extra={
        "requestBody": {
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "scores_file": {
                                "type": "string",
                                "format": "binary",
                                "description": "CSV file containing variant scores. This file is required, and should have at least one score column.",
                            },
                            "counts_file": {
                                "type": "string",
                                "format": "binary",
                                "description": "CSV file containing variant counts. If provided, this file should have the same index and variant columns as the scores file.",
                            },
                            "score_columns_metadata": {
                                "type": "string",
                                "format": "binary",
                                "description": "JSON file containing metadata for score columns. If provided, this file should have metadata for one or more score columns in the scores file. This JSON file should provide a dictionary mapping column names to metadata objects. Metadata objects should follow the DatasetColumnMetadata schema: `{'description': string, 'details': string}`.",
                            },
                            "count_columns_metadata": {
                                "type": "string",
                                "format": "binary",
                                "description": "JSON file containing metadata for count columns. If provided, this file should have metadata for one or more count columns in the counts file. This JSON file should provide a dictionary mapping column names to metadata objects. Metadata objects should follow the DatasetColumnMetadata schema: `{'description': string, 'details': string}`.",
                            },
                        },
                        "required": ["scores_file"],
                    }
                },
            },
            "description": "Score files, to be uploaded as multipart form data. The `scores_file` is required, while the `counts_file`, `score_columns_metadata`, and `count_columns_metadata` are optional.",
        }
    },
)
async def upload_score_set_variant_data(
    *,
    urn: str,
    data: Request,
    counts_file: Optional[UploadFile] = File(None),
    scores_file: Optional[UploadFile] = File(None),
    # count_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None,
    # score_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
    worker: ArqRedis = Depends(deps.get_worker),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Upload scores and variant count files for a score set, and initiate processing these files to
    create variants.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "variants"})

    try:
        score_set_variants_data = await parse_score_set_variants_uploads(scores_file, counts_file)

        form_data = await data.form()
        # Parse variants dataset column metadata JSON strings
        dataset_column_metadata = {
            key: json.loads(str(value))
            for key, value in form_data.items()
            if key in ["count_columns_metadata", "score_columns_metadata"]
        }
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

    # item = db.query(ScoreSet).filter(ScoreSet.urn == urn).filter(ScoreSet.private.is_(False)).one_or_none()
    item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item or not item.urn:
        logger.info(msg="Failed to create variants; The requested score set does not exist.", extra=logging_context())
        return None

    assert_permission(user_data, item, Action.UPDATE)
    assert_permission(user_data, item, Action.SET_SCORES)

    # Although this is also updated within the variant creation job, update it here
    # as well so that we can display the proper UI components (queue invocation delay
    # races the score set GET request).
    item.processing_state = ProcessingState.processing

    logger.info(msg="Enqueuing variant creation job.", extra=logging_context())

    try:
        await enqueue_variant_creation(
            item=item,
            user_data=user_data,
            new_scores_df=score_set_variants_data["scores_df"],
            new_counts_df=score_set_variants_data["counts_df"],
            new_score_columns_metadata=dataset_column_metadata.get("score_columns_metadata", {}),
            new_count_columns_metadata=dataset_column_metadata.get("count_columns_metadata", {}),
            worker=worker,
            db=db,
        )
    except Exception as e:
        logger.error(
            msg="Failed to enqueue variant creation pipeline; resetting score set processing state.",
            extra=logging_context(),
            exc_info=e,
        )
        try:
            db.rollback()
            item.processing_state = ProcessingState.failed
            item.processing_errors = {
                "exception": "Failed to create variant processing pipeline. Please try uploading the variant data again",
                "detail": None,
            }
            db.add(item)
            db.commit()
        except Exception:
            logger.error(
                msg="Failed to reset score set processing state after pipeline enqueue failure.",
                extra=logging_context(),
            )
        raise HTTPException(
            status_code=500,
            detail="Could not update variants for this score set at this time. Failed to create variant processing pipeline.",
        )

    db.add(item)
    db.commit()
    db.refresh(item)

    return _score_set_response(item, principal)


@router.patch(
    "/score-sets-with-variants/{urn}",
    response_model=score_set.ScoreSet,
    response_model_exclude_none=True,
    responses={**BASE_400_RESPONSE, **ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Update score ranges / calibrations for a score set",
    openapi_extra={
        "requestBody": {
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            **score_set.ScoreSetUpdateAllOptional.model_json_schema(by_alias=False)["properties"],
                            "scores_file": {
                                "type": "string",
                                "format": "binary",
                                "description": "CSV file containing variant scores. If provided, this file should have at least one score column.",
                            },
                            "counts_file": {
                                "type": "string",
                                "format": "binary",
                                "description": "CSV file containing variant counts. If provided, this file should have the same index and variant columns as the scores file.",
                            },
                            "score_columns_metadata": {
                                "type": "string",
                                "format": "binary",
                                "description": "JSON file containing metadata for score columns. If provided, this file should have metadata for one or more score columns in the scores file. This JSON file should provide a dictionary mapping column names to metadata objects. Metadata objects should follow the DatasetColumnMetadata schema: `{'description': string, 'details': string}`.",
                            },
                            "count_columns_metadata": {
                                "type": "string",
                                "format": "binary",
                                "description": "JSON file containing metadata for count columns. If provided, this file should have metadata for one or more count columns in the counts file. This JSON file should provide a dictionary mapping column names to metadata objects. Metadata objects should follow the DatasetColumnMetadata schema: `{'description': string, 'details': string}`.",
                            },
                        },
                    }
                },
            },
            "description": "Score set properties and score files, to be uploaded as multipart form data. All fields here are optional, and only those provided will be updated.",
        }
    },
)
async def update_score_set_with_variants(
    *,
    urn: str,
    request: Request,
    # Variants data files
    counts_file: Optional[UploadFile] = File(None),
    scores_file: Optional[UploadFile] = File(None),
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
    worker: ArqRedis = Depends(deps.get_worker),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Update a score set and variants.
    """
    logger.info(msg="Began score set with variants update.", extra=logging_context())

    # TODO#629: Use `flexible_model_loader` utility here to support both form data and JSON body.
    #           See: https://github.com/VariantEffect/mavedb-api/pull/589/changes/d1641de7e4bee43e8a0c9f9283e022c5b56830ff
    #           Currently, only form data is supported but this would allow us to also support JSON bodies
    #           in cases where no files are being uploaded. My view is accepting score set calibration
    #           information via a single form field is also more straightforward than handling all the score
    #           set update fields as separate form fields and parsing them into an object. Doing so will also
    #           simplify the OpenAPI schema for this endpoint.
    try:
        # Get all form data from the request
        form_data = await request.form()

        # Convert form data to dictionary, excluding file and associated column metadata fields
        form_dict = {
            key: value
            for key, value in form_data.items()
            if key not in ["counts_file", "scores_file", "count_columns_metadata", "score_columns_metadata"]
        }
        # Create the update object using **kwargs in as_form
        item_update_partial = score_set.ScoreSetUpdateAllOptional.as_form(**form_dict)

        # parse uploaded CSV files
        score_set_variants_data = await parse_score_set_variants_uploads(
            scores_file,
            counts_file,
        )

        # Parse variants dataset column metadata JSON strings
        dataset_column_metadata = {
            key: json.loads(str(value))
            for key, value in form_data.items()
            if key in ["count_columns_metadata", "score_columns_metadata"]
        }
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

    # get existing item from db
    existing_item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()

    # merge existing item data with item_update data to validate against ScoreSetUpdate

    if existing_item:
        existing_item_data = score_set.ScoreSet.model_validate(existing_item).model_dump()
        updated_data = {**existing_item_data, **item_update_partial.model_dump(exclude_unset=True)}
        try:
            score_set.ScoreSetUpdate.model_validate(updated_data)
        except ValidationError as e:
            # format as fastapi request validation error
            raise RequestValidationError(errors=e.errors())
    else:
        logger.info(msg="Failed to update score set; The requested score set does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    itemUpdateResult = await score_set_update(
        db=db,
        urn=urn,
        item_update=item_update_partial,
        exclude_unset=True,
        user_data=user_data,
        existing_item=existing_item,
    )
    updatedItem = itemUpdateResult["item"]
    should_create_variants = itemUpdateResult.get("should_create_variants", False)

    existing_score_columns_metadata = (existing_item.dataset_columns or {}).get("score_columns_metadata", {})
    existing_count_columns_metadata = (existing_item.dataset_columns or {}).get("count_columns_metadata", {})

    did_score_columns_metadata_change = (
        dataset_column_metadata.get("score_columns_metadata", {}) != existing_score_columns_metadata
    )
    did_count_columns_metadata_change = (
        dataset_column_metadata.get("count_columns_metadata", {}) != existing_count_columns_metadata
    )

    # run variant creation job only if targets have changed (indicated by "should_create_variants"), new score
    # or count files were uploaded, or dataset column metadata has changed
    if (
        should_create_variants
        or did_score_columns_metadata_change
        or did_count_columns_metadata_change
        or any([val is not None for val in score_set_variants_data.values()])
    ):
        assert_permission(user_data, updatedItem, Action.SET_SCORES)

        updatedItem.processing_state = ProcessingState.processing
        logger.info(msg="Enqueuing variant creation job.", extra=logging_context())

        try:
            await enqueue_variant_creation(
                item=updatedItem,
                user_data=user_data,
                worker=worker,
                new_scores_df=score_set_variants_data["scores_df"],
                new_counts_df=score_set_variants_data["counts_df"],
                new_score_columns_metadata=dataset_column_metadata.get("score_columns_metadata")
                if did_score_columns_metadata_change
                else existing_score_columns_metadata,
                new_count_columns_metadata=dataset_column_metadata.get("count_columns_metadata")
                if did_count_columns_metadata_change
                else existing_count_columns_metadata,
                db=db,
            )
        except Exception as e:
            logger.error(
                msg="Failed to enqueue variant creation pipeline; resetting score set processing state.",
                extra=logging_context(),
                exc_info=e,
            )
            try:
                db.rollback()
                updatedItem.processing_state = ProcessingState.failed
                updatedItem.processing_errors = {
                    "exception": "Failed to create variant processing pipeline. Please try uploading the variant data again",
                    "detail": None,
                }
                db.add(updatedItem)
                db.commit()
            except Exception:
                logger.error(
                    msg="Failed to reset score set processing state after pipeline enqueue failure.",
                    extra=logging_context(),
                )
            raise HTTPException(
                status_code=500,
                detail="Could not update variants for this score set at this time. Failed to create variant processing pipeline.",
            )

    db.add(updatedItem)
    db.commit()
    db.refresh(updatedItem)

    return _score_set_response(updatedItem, principal)


@router.put(
    "/score-sets/{urn}",
    response_model=score_set.ScoreSet,
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES, **BASE_409_RESPONSE, **GATEWAY_ERROR_RESPONSES},
    summary="Update a score set",
)
async def update_score_set(
    *,
    urn: str,
    item_update: score_set.ScoreSetUpdate,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
    worker: ArqRedis = Depends(deps.get_worker),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Update a score set.
    """
    save_to_logging_context({"requested_resource": urn})
    logger.debug(msg="Began score set update.", extra=logging_context())

    # this object will contain all required fields because item_update type is ScoreSetUpdate, but
    # is converted to instance of ScoreSetUpdateAllOptional to match expected input of score_set_update function
    score_set_update_item = score_set.ScoreSetUpdateAllOptional.model_validate(item_update.model_dump())
    itemUpdateResult = await score_set_update(
        db=db, urn=urn, item_update=score_set_update_item, exclude_unset=False, user_data=user_data
    )
    updatedItem = itemUpdateResult["item"]
    should_create_variants = itemUpdateResult["should_create_variants"]

    if should_create_variants:
        # Although this is also updated within the variant creation job, update it here
        # as well so that we can display the proper UI components (queue invocation delay
        # races the score set GET request).
        updatedItem.processing_state = ProcessingState.processing

        logger.info(msg="Enqueuing variant creation job.", extra=logging_context())
        try:
            await enqueue_variant_creation(
                item=updatedItem,
                user_data=user_data,
                worker=worker,
                db=db,
            )
        except Exception as e:
            logger.error(
                msg="Failed to enqueue variant creation pipeline; resetting score set processing state.",
                extra=logging_context(),
                exc_info=e,
            )
            try:
                db.rollback()
                updatedItem.processing_state = ProcessingState.failed
                updatedItem.processing_errors = {
                    "exception": "Failed to create variant processing pipeline. Please try uploading the variant data again",
                    "detail": None,
                }
                db.add(updatedItem)
                db.commit()
            except Exception:
                logger.error(
                    msg="Failed to reset score set processing state after pipeline enqueue failure.",
                    extra=logging_context(),
                )
            raise HTTPException(
                status_code=500,
                detail="Could not update this score set at this time. Failed to create variant processing pipeline.",
            )

        db.add(updatedItem)
        db.commit()
        db.refresh(updatedItem)

    return _score_set_response(updatedItem, principal)


@router.delete(
    "/score-sets/{urn}",
    status_code=200,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Delete a score set",
)
async def delete_score_set(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Delete a score set.

    Raises

    Returns
    _______
    Does not return anything
    string : HTTP code 200 successful but returning content
    or
    communitcate to client whether the operation succeeded
    204 if successful but not returning content - likely going with this
    """
    save_to_logging_context({"requested_resource": urn})

    item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item:
        logger.info(msg="Failed to delete score set; The requested score set does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.DELETE)

    db.delete(item)
    db.commit()


@router.post(
    "/score-sets/{urn}/publish",
    status_code=200,
    response_model=score_set.ScoreSet,
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES, **BASE_409_RESPONSE},
)
async def publish_score_set(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
    worker: ArqRedis = Depends(deps.get_worker),
    principal: Principal = Depends(get_principal),
) -> Any:
    """
    Publish a score set.
    """
    save_to_logging_context({"requested_resource": urn})

    item: Optional[ScoreSet] = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item:
        logger.info(msg="Failed to publish score set; The requested score set does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.PUBLISH)

    if not item.experiment:
        logger.info(
            msg="Failed to publish score set; The requested score set does not belong to an experiment.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=409,
            detail="This score set does not belong to an experiment and cannot be published.",
        )
    if not item.experiment.experiment_set:
        logger.info(
            msg="Failed to publish score set; The requested score set's experiment does not belong to an experiment set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=409,
            detail="This score set's experiment does not belong to an experiment set and cannot be published.",
        )
    # TODO This can probably be done more efficiently; at least, it's worth checking the SQL query that SQLAlchemy
    # generates when all we want is len(score_set.variants).
    if len(item.variants) == 0:
        logger.info(
            msg="Failed to publish score set; The requested score set does not contain any variant scores.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=409,
            detail="cannot publish score set without variant scores",
        )

    published_date = date.today()

    if item.experiment.experiment_set.private or not item.experiment.experiment_set.published_date:
        item.experiment.experiment_set.urn = generate_experiment_set_urn(db)
        item.experiment.experiment_set.private = False
        item.experiment.experiment_set.published_date = published_date
        db.add(item.experiment.experiment_set)

    save_to_logging_context({"experiment_set": item.experiment.experiment_set.urn})

    if item.experiment.private or not item.experiment.published_date:
        item.experiment.urn = generate_experiment_urn(
            db,
            item.experiment.experiment_set,
            experiment_is_meta_analysis=len(item.meta_analyzes_score_sets) > 0,
        )
        item.experiment.private = False
        item.experiment.published_date = published_date
        db.add(item.experiment)

    save_to_logging_context({"experiment": item.experiment.urn})

    item.urn = generate_score_set_urn(db, item.experiment)
    item.private = False
    item.published_date = published_date
    refresh_variant_urns(db, item)

    save_to_logging_context({"score_set": item.urn})

    db.add(item)
    db.commit()
    db.refresh(item)

    try:
        await enqueue_pipeline_for_score_set(
            db=db,
            redis=worker,
            pipeline_name="publish_score_set",
            score_set=item,
            user=user_data.user,
        )
    except Exception as exc:
        logger.warning(
            msg="Failed to enqueue publish_score_set pipeline.",
            extra=logging_context(),
        )
        send_slack_error(err=exc)

    return _score_set_response(item, principal)


@router.get(
    "/score-sets/{urn}/clinical-controls",
    status_code=200,
    response_model=list[clinical_control.ClinicalControlWithClinvarLinks],
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Get clinical controls for a score set",
)
async def get_clinical_controls_for_score_set(
    *,
    urn: str,
    response: Response,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the allele → ClinVar link state as it stood at this instant. "
            "ISO 8601, ideally timezone-aware. Defaults to current."
        ),
    ),
    # We'd prefer to reserve `db` as a query parameter.
    _db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
    db: Optional[str] = None,
    version: Optional[str] = None,
) -> list[clinical_control.ClinicalControlWithClinvarLinks]:
    """
    Fetch relevant clinical controls for a given score set.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "clinical_controls", "as_of": as_of})
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"

    # Rename user facing kwargs for consistency with code base naming conventions.
    db_name = db
    db_version = version
    if db_name is not None:
        save_to_logging_context({"db_name": db_name})
    if db_version is not None:
        save_to_logging_context({"db_version": db_version})

    item: Optional[ScoreSet] = _db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
    if not item:
        logger.info(
            msg="Failed to fetch clinical controls for score set; The requested score set does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.READ)

    controls = get_clinical_controls_with_variant_urns(
        _db, item.id, as_of=as_of, db_name=db_name, db_version=db_version
    )

    if not controls:
        # Can legitimately fire even for a (db_name, db_version) sourced from `.../options`: liveness
        # is re-evaluated per call, see that endpoint's docstring.
        logger.info(
            msg="No clinical control variants matching the provided filters are associated with the requested score set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"No clinical control variants matching the provided filters associated with score set URN {urn} were found",
        )

    save_to_logging_context({"resource_count": len(controls)})

    return [
        clinical_control.ClinicalControlWithClinvarLinks.model_validate(
            {
                "id": ctrl.id,
                "db_identifier": ctrl.db_identifier,
                "gene_symbol": ctrl.gene_symbol,
                "clinical_significance": ctrl.clinical_significance,
                "clinical_review_status": ctrl.clinical_review_status,
                "db_version": ctrl.db_version,
                "db_name": ctrl.db_name,
                "modification_date": ctrl.modification_date,
                "creation_date": ctrl.creation_date,
                "clinvar_links": [
                    {"variant_urn": link.variant_urn, "allele_digest": link.allele_digest} for link in links
                ],
            }
        )
        for ctrl, links in controls
    ]


@router.get(
    "/score-sets/{urn}/clinical-controls/options",
    status_code=200,
    response_model=list[clinical_control.ClinicalControlOptions],
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Get clinical control options for a score set",
)
async def get_clinical_controls_options_for_score_set(
    *,
    urn: str,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the allele → ClinVar link state as it stood at this instant. "
            "ISO 8601, ideally timezone-aware. Defaults to current."
        ),
    ),
    # We'd prefer to reserve `db` as a query parameter.
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
) -> list[dict[str, Union[str, list[str]]]]:
    """
    Fetch clinical control options for a given score set.

    Each ``(db_name, db_version)`` pair returned here was live at the moment of this call, but
    liveness is re-evaluated independently per request. A pair fetched here can have its backing
    ``ClinvarAlleleLink`` retired before a later call to ``GET /score-sets/{urn}/clinical-controls``
    filters on it, in which case that call 404s. Pin an explicit ``as_of`` on both calls to avoid this
    possibility.
    """
    save_to_logging_context(
        {
            "requested_resource": urn,
            "resource_property": "clinical_control_options",
            "as_of": as_of,
        }
    )

    item: Optional[ScoreSet] = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
    if not item:
        logger.info(
            msg="Failed to fetch clinical control options for score set; The requested score set does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.READ)

    options = get_clinical_control_options(db, item.id, as_of=as_of)

    if not options:
        logger.info(
            msg="Failed to fetch clinical control options for score set; No clinical control variants are associated with this score set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"no clinical control variants associated with score set URN {urn} were found",
        )

    return [{"db_name": db_name, "available_versions": available_versions} for db_name, available_versions in options]


@router.get(
    "/score-sets/{urn}/gnomad-variants",
    status_code=200,
    response_model=list[gnomad_variant.GnomADVariantWithVariantLinks],
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Get gnomad variants for a score set",
)
async def get_gnomad_variants_for_score_set(
    *,
    urn: str,
    response: Response,
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the allele → gnomAD link state as it stood at this instant. "
            "ISO 8601, ideally timezone-aware. Defaults to current."
        ),
    ),
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
    version: Optional[str] = None,
) -> list[gnomad_variant.GnomADVariantWithVariantLinks]:
    """
    Fetch relevant gnomad variants for a given score set, each paired with the score-set variants (and
    annotated allele digests) it links to over the allele substrate.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "gnomad_variants", "as_of": as_of})
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"

    # Rename user facing kwargs for consistency with code base naming conventions.
    db_version = version

    item: Optional[ScoreSet] = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
    if not item:
        logger.info(
            msg="Failed to fetch gnomad variants for score set; The requested score set does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.READ)

    if db_version is not None:
        save_to_logging_context({"db_version": db_version})

    gnomad_variants = get_gnomad_variants_with_variant_urns(db, item.id, as_of=as_of, db_version=db_version)

    if not gnomad_variants:
        logger.info(
            msg="No gnomad variants matching the provided filters are associated with the requested score set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"No gnomad variants matching the provided filters associated with score set URN {urn} were found",
        )

    save_to_logging_context({"resource_count": len(gnomad_variants)})

    return [
        gnomad_variant.GnomADVariantWithVariantLinks.model_validate(
            {
                "id": gv.id,
                "db_name": gv.db_name,
                "db_identifier": gv.db_identifier,
                "db_version": gv.db_version,
                "allele_count": gv.allele_count,
                "allele_number": gv.allele_number,
                "allele_frequency": gv.allele_frequency,
                "faf95_max": gv.faf95_max,
                "faf95_max_ancestry": gv.faf95_max_ancestry,
                "creation_date": gv.creation_date,
                "modification_date": gv.modification_date,
                "variant_links": [
                    {"variant_urn": link.variant_urn, "allele_digest": link.allele_digest} for link in links
                ],
            }
        )
        for gv, links in gnomad_variants
    ]
