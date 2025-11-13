import json
import logging
import time
from datetime import date, datetime
from typing import Any, List, Literal, Optional, Sequence, TypedDict, Union

import numpy as np
import pandas as pd
import requests
from arq import ArqRedis
from fastapi import APIRouter, Depends, File, Query, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import StreamingResponse
from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement
from pydantic import ValidationError
from sqlalchemy import or_, select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session, contains_eager

from mavedb import deps
from mavedb.lib.annotation.annotate import (
    variant_functional_impact_statement,
    variant_pathogenicity_evidence,
    variant_study_result,
)
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.authentication import UserData
from mavedb.lib.authorization import (
    get_current_user,
    require_current_user,
    require_current_user_with_email,
)
from mavedb.lib.contributors import find_or_create_contributor
from mavedb.lib.exceptions import MixedTargetError, NonexistentOrcidUserError
from mavedb.lib.experiments import enrich_experiment_with_num_score_sets
from mavedb.lib.identifiers import (
    create_external_gene_identifier_offset,
    find_or_create_doi_identifier,
    find_or_create_publication_identifier,
)
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import (
    correlation_id_for_context,
    logging_context,
    save_to_logging_context,
)
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.score_calibrations import create_score_calibration
from mavedb.lib.score_sets import (
    csv_data_to_df,
    fetch_score_set_search_filter_options,
    find_meta_analyses_for_experiment_sets,
    get_score_set_variants_as_csv,
    refresh_variant_urns,
    variants_to_csv_rows,
)
from mavedb.lib.score_sets import (
    search_score_sets as _search_score_sets,
)
from mavedb.lib.target_genes import find_or_create_target_gene_by_accession, find_or_create_target_gene_by_sequence
from mavedb.lib.taxonomies import find_or_create_taxonomy
from mavedb.lib.urns import (
    generate_experiment_set_urn,
    generate_experiment_urn,
    generate_score_set_urn,
)
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.contributor import Contributor
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.experiment import Experiment
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.license import License
from mavedb.models.mapped_variant import MappedVariant
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
    GATEWAY_ERROR_RESPONSES,
    PUBLIC_ERROR_RESPONSES,
    ROUTER_BASE_PREFIX,
)
from mavedb.view_models import clinical_control, gnomad_variant, mapped_variant, score_set
from mavedb.view_models.contributor import ContributorCreate
from mavedb.view_models.doi_identifier import DoiIdentifierCreate
from mavedb.view_models.publication_identifier import PublicationIdentifierCreate
from mavedb.view_models.score_set_dataset_columns import DatasetColumnMetadata
from mavedb.view_models.search import ScoreSetsSearch, ScoreSetsSearchFilterOptionsResponse, ScoreSetsSearchResponse
from mavedb.view_models.target_gene import TargetGeneCreate

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
) -> None:
    assert item.dataset_columns is not None

    # create CSV from existing variants on the score set if no new dataframe provided
    existing_scores_df = None
    if new_scores_df is None:
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

    # Await the insertion of this job into the worker queue, not the job itself.
    # Uses provided score and counts dataframes and metadata files, or falls back to existing data on the score set if not provided.
    job = await worker.enqueue_job(
        "create_variants_for_score_set",
        correlation_id_for_context(),
        item.id,
        user_data.user.id,
        existing_scores_df if new_scores_df is None else new_scores_df,
        existing_counts_df if new_counts_df is None else new_counts_df,
        item.dataset_columns.get("score_columns_metadata")
        if new_score_columns_metadata is None
        else new_score_columns_metadata,
        item.dataset_columns.get("count_columns_metadata")
        if new_count_columns_metadata is None
        else new_count_columns_metadata,
    )
    if job is not None:
        save_to_logging_context({"worker_job_id": job.job_id})
        logger.info(msg="Enqueued variant creation job.", extra=logging_context())


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
      contributor.
    :param only_published: If true, only return the score set if it is published.
    :return: The score set, or None if the URL was not found or refers to a private score set not owned by the specified
        user.
    """
    try:
        query = db.query(ScoreSet).filter(ScoreSet.urn == urn)
        if owner_or_contributor is not None:
            query.filter(
                or_(
                    ScoreSet.private.is_(False),
                    ScoreSet.created_by_id == owner_or_contributor.user.id,
                    ScoreSet.contributors.any(Contributor.orcid_id == owner_or_contributor.user.username),
                )
            )
        if only_published:
            query.filter(ScoreSet.private.is_(False))
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

    if item.superseding_score_set and not has_permission(user, item.superseding_score_set, Action.READ).permitted:
        item.superseding_score_set = None

    item.score_calibrations = [sc for sc in item.score_calibrations if has_permission(user, sc, Action.READ).permitted]

    return item


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
    enriched_score_sets = []
    if search.include_experiment_score_set_urns_and_count:
        for ss in score_sets:
            enriched_experiment = enrich_experiment_with_num_score_sets(ss.experiment, user_data)
            response_item = score_set.ScoreSet.model_validate(ss).copy(update={"experiment": enriched_experiment})
            enriched_score_sets.append(response_item)
        score_sets = enriched_score_sets

    return {"score_sets": score_sets, "num_score_sets": num_score_sets}


@router.post("/score-sets/search/filter-options", status_code=200, response_model=ScoreSetsSearchFilterOptionsResponse)
def get_filter_options_for_search(
    search: ScoreSetsSearch,
    db: Session = Depends(deps.get_db),
) -> Any:
    return fetch_score_set_search_filter_options(db, None, search)


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
) -> Any:
    """
    Fetch a single score set by URN.
    """
    save_to_logging_context({"requested_resource": urn})
    item = await fetch_score_set_by_urn(db, urn, user_data, None, False)
    enriched_experiment = enrich_experiment_with_num_score_sets(item.experiment, user_data)
    return score_set.ScoreSet.model_validate(item).copy(update={"experiment": enriched_experiment})


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
    namespaces: List[Literal["scores", "counts"]] = Query(
        default=["scores"], description="One or more data types to include: scores, counts, clinVar, gnomAD"
    ),
    drop_na_columns: Optional[bool] = None,
    include_custom_columns: Optional[bool] = None,
    include_post_mapped_hgvs: Optional[bool] = None,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """
    Return tabular variant data from a score set, identified by URN, in CSV format.

    This differs from get_score_set_scores_csv() in that it returns only the HGVS columns, score column, and mapped HGVS
    string.

    TODO (https://github.com/VariantEffect/mavedb-api/issues/446) We may add another function for ClinVar and gnomAD.
    export endpoint, with options governing which columns to include.

    Parameters
    __________
    urn : str
        The URN of the score set to fetch variants from.
    start : Optional[int]
        The index to start from. If None, starts from the beginning.
    limit : Optional[int]
        The maximum number of variants to return. If None, returns all variants.
    namespaces: List[Literal["scores", "counts"]]
        The namespaces of all columns except for accession, hgvs_nt, hgvs_pro, and hgvs_splice.
        We may add ClinVar and gnomAD in the future.
    drop_na_columns : bool, optional
        Whether to drop columns that contain only NA values. Defaults to False.
    db : Session
        The database session to use.
    user_data : Optional[UserData]
        The user data of the current user. If None, no user-specific permissions are checked.

    Returns
    _______
    str
        The CSV string containing the variant data.
    """
    save_to_logging_context(
        {
            "requested_resource": urn,
            "resource_property": "scores",
            "start": start,
            "limit": limit,
            "drop_na_columns": drop_na_columns,
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
        True,
        start,
        limit,
        drop_na_columns,
        include_custom_columns,
        include_post_mapped_hgvs,
    )
    return StreamingResponse(iter([csv_str]), media_type="text/csv")


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
    drop_na_columns: Optional[bool] = None,
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

    csv_str = get_score_set_variants_as_csv(db, score_set, ["scores"], False, start, limit, drop_na_columns)
    return StreamingResponse(iter([csv_str]), media_type="text/csv")


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
    drop_na_columns: Optional[bool] = None,
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

    csv_str = get_score_set_variants_as_csv(db, score_set, ["counts"], False, start, limit, drop_na_columns)
    return StreamingResponse(iter([csv_str]), media_type="text/csv")


@router.get(
    "/score-sets/{urn}/mapped-variants",
    status_code=200,
    response_model=list[mapped_variant.MappedVariant],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Get mapped variants from score set by URN",
)
def get_score_set_mapped_variants(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> list[MappedVariant]:
    """
    Return mapped variants from a score set, identified by URN.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "mapped-variants"})

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(
            msg="Could not fetch the requested mapped variants; No such score set exist.", extra=logging_context()
        )
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")

    assert_permission(user_data, score_set, Action.READ)

    mapped_variants = (
        db.query(MappedVariant)
        .filter(ScoreSet.urn == urn)
        .filter(ScoreSet.id == Variant.score_set_id)
        .filter(Variant.id == MappedVariant.variant_id)
        .all()
    )

    if not mapped_variants:
        logger.info(msg="No mapped variants are associated with the requested score set.", extra=logging_context())
        raise HTTPException(
            status_code=404,
            detail=f"No mapped variant associated with score set URN {urn} was found",
        )

    return mapped_variants


def _stream_generated_annotations(mapped_variants, annotation_function):
    """
    Generator function to stream annotations as pure NDJSON data.

    Metadata should be provided via HTTP headers:
    - X-Total-Count: Total number of variants
    - X-Processing-Started: ISO timestamp when processing began
    - X-Stream-Type: Type of annotation being streamed

    Progress updates are sent as structured log events that can be
    consumed via Server-Sent Events if needed.
    """
    start_time = time.time()
    total_variants = len(mapped_variants)
    processed_count = 0
    logger.info(f"Starting streaming processing of {total_variants} mapped variants")

    for i, mv in enumerate(mapped_variants):
        try:
            annotation = annotation_function(mv)
        except MappingDataDoesntExistException:
            logger.debug(f"Mapping data does not exist for variant {mv.variant.urn}.")
            annotation = None

        # Send pure result data (no wrapper)
        result = {
            "variant_urn": mv.variant.urn,
            "annotation": annotation.model_dump(exclude_none=True) if annotation else None,
        }
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
                "total_time": round(total_time, 2),
                "average_time_per_variant": average_time_per_variant,
                "final_rate": final_rate,
                "timestamp": end_time,
            }
        }
    )
    logger.info(
        f"Completed streaming {processed_count} variants in {total_time:.2f} seconds (avg: {average_time_per_variant:.4f}s/variant)",
        extra=logging_context(),
    )


class VariantPathogenicityEvidenceLineResponseType(TypedDict):
    variant_urn: str
    annotation: Optional[VariantPathogenicityEvidenceLine]


@router.get(
    "/score-sets/{urn}/annotated-variants/pathogenicity-evidence-line",
    status_code=200,
    response_model=dict[str, Optional[VariantPathogenicityEvidenceLine]],
    response_model_exclude_none=True,
    summary="Get pathogenicity evidence line annotations for mapped variants within a score set",
    responses={
        200: {
            "content": {"application/x-ndjson": {}},
            "description": "Stream pathogenicity evidence line annotations for mapped variants.",
        },
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
)
def get_score_set_annotated_variants(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """
    Retrieve annotated variants with pathogenicity evidence for a given score set.

    This endpoint streams pathogenicity evidence lines for all current mapped variants
    associated with a specific score set. The response is returned as newline-delimited
    JSON (NDJSON) format for efficient processing of large datasets.

    NDJSON Response Format:
        Each line in the response corresponds to a mapped variant and contains a JSON
        object with the following structure:
        ```
        {
            "variant_urn": "<URN of the mapped variant>",
            "annotation": {
                ... // Pathogenicity evidence line details
            }
        }
        ```

    Args:
        urn (str): The Uniform Resource Name (URN) of the score set to retrieve
            annotated variants for.
        db (Session, optional): Database session dependency. Defaults to Depends(deps.get_db).
        user_data (Optional[UserData], optional): Current user data for permission checking.
            Defaults to Depends(get_current_user).

    Returns:
        Any: StreamingResponse containing newline-delimited JSON with pathogenicity
            evidence lines for each mapped variant. Response includes headers with
            total count, processing start time, and stream type information.

    Raises:
        HTTPException: 404 error if the score set with the given URN is not found.
        HTTPException: 404 error if no mapped variants are associated with the score set.
        HTTPException: 403 error if the user lacks READ permissions for the score set.

    Note:
        This function logs the request context and validates user permissions before
        processing. Only current (non-historical) mapped variants are included in
        the response.
    """
    save_to_logging_context(
        {"requested_resource": urn, "resource_property": "annotated-variants/pathogenicity-evidence-line"}
    )

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(
            msg="Could not fetch the requested pathogenicity evidence lines; No such score set exists.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")

    assert_permission(user_data, score_set, Action.READ)

    mapped_variants = (
        db.query(MappedVariant)
        .join(MappedVariant.variant)
        .join(Variant.score_set)
        .filter(ScoreSet.urn == urn)
        .filter(MappedVariant.current.is_(True))
        .options(
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set),
            contains_eager(MappedVariant.variant)
            .contains_eager(Variant.score_set)
            .selectinload(ScoreSet.publication_identifier_associations),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.created_by),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.modified_by),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.license),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.experiment),
        )
        .all()
    )

    if not mapped_variants:
        logger.info(msg="No mapped variants are associated with the requested score set.", extra=logging_context())
        raise HTTPException(
            status_code=404,
            detail=f"No mapped variants associated with score set URN {urn} were found. Could not construct evidence lines.",
        )

    return StreamingResponse(
        _stream_generated_annotations(mapped_variants, variant_pathogenicity_evidence),
        media_type="application/x-ndjson",
        headers={
            "X-Total-Count": str(len(mapped_variants)),
            "X-Processing-Started": datetime.now().isoformat(),
            "X-Stream-Type": "pathogenicity-evidence-line",
            "Access-Control-Expose-Headers": "X-Total-Count, X-Processing-Started, X-Stream-Type",
        },
    )


class FunctionalImpactStatementResponseType(TypedDict):
    variant_urn: str
    annotation: Optional[Statement]


@router.get(
    "/score-sets/{urn}/annotated-variants/functional-impact-statement",
    status_code=200,
    response_model=dict[str, Optional[Statement]],
    response_model_exclude_none=True,
    summary="Get functional impact statement annotations for mapped variants within a score set",
    responses={
        200: {
            "content": {"application/x-ndjson": {}},
            "description": "Stream functional impact statement annotations for mapped variants.",
        },
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
)
def get_score_set_annotated_variants_functional_statement(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
):
    """
    Retrieve functional impact statements for annotated variants in a score set.

    This endpoint streams functional impact statements for all current mapped variants
    associated with a specific score set. The response is delivered as newline-delimited
    JSON (NDJSON) format.

    NDJSON Response Format:
        Each line in the response corresponds to a mapped variant and contains a JSON
        object with the following structure:
        ```
        {
            "variant_urn": "<URN of the mapped variant>",
            "annotation": {
                ... // Functional impact statement details
            }
        }
        ```

    Args:
        urn (str): The unique resource name (URN) identifying the score set.
        db (Session): Database session dependency for querying data.
        user_data (Optional[UserData]): Current authenticated user data for permission checks.

    Returns:
        StreamingResponse: NDJSON stream containing functional impact statements for each
            mapped variant. Response includes headers with total count, processing start time,
            and stream type information.

    Raises:
        HTTPException:
            - 404 if the score set with the given URN is not found
            - 404 if no mapped variants are associated with the score set
            - 403 if the user lacks READ permission for the score set

    Note:
        Only current (non-historical) mapped variants are included in the response.
        The function requires appropriate read permissions on the score set.
    """
    save_to_logging_context(
        {"requested_resource": urn, "resource_property": "annotated-variants/functional-impact-statement"}
    )

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(
            msg="Could not fetch the requested functional impact statements; No such score set exists.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")

    assert_permission(user_data, score_set, Action.READ)

    mapped_variants = (
        db.query(MappedVariant)
        .join(MappedVariant.variant)
        .join(Variant.score_set)
        .filter(ScoreSet.urn == urn)
        .filter(MappedVariant.current.is_(True))
        .options(
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set),
            contains_eager(MappedVariant.variant)
            .contains_eager(Variant.score_set)
            .selectinload(ScoreSet.publication_identifier_associations),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.created_by),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.modified_by),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.license),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.experiment),
        )
        .all()
    )

    if not mapped_variants:
        logger.info(msg="No mapped variants are associated with the requested score set.", extra=logging_context())
        raise HTTPException(
            status_code=404,
            detail=f"No mapped variants associated with score set URN {urn} were found. Could not construct functional impact statements.",
        )

    return StreamingResponse(
        _stream_generated_annotations(mapped_variants, variant_functional_impact_statement),
        media_type="application/x-ndjson",
        headers={
            "X-Total-Count": str(len(mapped_variants)),
            "X-Processing-Started": datetime.now().isoformat(),
            "X-Stream-Type": "functional-impact-statement",
            "Access-Control-Expose-Headers": "X-Total-Count, X-Processing-Started, X-Stream-Type",
        },
    )


class FunctionalStudyResultResponseType(TypedDict):
    variant_urn: str
    annotation: Optional[ExperimentalVariantFunctionalImpactStudyResult]


@router.get(
    "/score-sets/{urn}/annotated-variants/functional-study-result",
    status_code=200,
    response_model=dict[str, Optional[ExperimentalVariantFunctionalImpactStudyResult]],
    response_model_exclude_none=True,
    summary="Get functional study result annotations for mapped variants within a score set",
    responses={
        200: {
            "content": {"application/x-ndjson": {}},
            "description": "Stream functional study result annotations for mapped variants.",
        },
        **ACCESS_CONTROL_ERROR_RESPONSES,
    },
)
def get_score_set_annotated_variants_functional_study_result(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
):
    """
    Retrieve functional study results for annotated variants in a score set.

    This endpoint streams functional study result annotations for all current mapped variants
    associated with a specific score set. The results are returned as newline-delimited JSON
    (NDJSON) format for efficient streaming of large datasets.

    NDJSON Response Format:
        Each line in the response corresponds to a mapped variant and contains a JSON
        object with the following structure:
        ```
        {
            "variant_urn": "<URN of the mapped variant>",
            "annotation": {
                ... // Functional study result details
            }
        }
        ```

    Args:
        urn (str): The URN (Uniform Resource Name) of the score set to retrieve variants for.
        db (Session): Database session dependency for querying the database.
        user_data (Optional[UserData]): Current user data for permission validation.

    Returns:
        StreamingResponse: A streaming response containing functional study results in NDJSON format.
            Headers include:
            - X-Total-Count: Total number of mapped variants being streamed
            - X-Processing-Started: ISO timestamp when processing began
            - X-Stream-Type: Set to "functional-study-result"
            - Access-Control-Expose-Headers: Exposed headers for CORS

    Raises:
        HTTPException:
            - 404 if the score set with the given URN is not found
            - 404 if no mapped variants are associated with the score set
            - 403 if the user lacks READ permission for the score set

    Notes:
        - Only returns current mapped variants (MappedVariant.current == True)
        - Eagerly loads related ScoreSet data including publications, users, license, and experiment
        - Logs requests and errors for monitoring and debugging purposes
    """
    save_to_logging_context(
        {"requested_resource": urn, "resource_property": "annotated-variants/functional-study-result"}
    )

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        logger.info(
            msg="Could not fetch the requested functional study results; No such score set exists.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")

    assert_permission(user_data, score_set, Action.READ)

    mapped_variants = (
        db.query(MappedVariant)
        .join(MappedVariant.variant)
        .join(Variant.score_set)
        .filter(ScoreSet.urn == urn)
        .filter(MappedVariant.current.is_(True))
        .options(
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set),
            contains_eager(MappedVariant.variant)
            .contains_eager(Variant.score_set)
            .selectinload(ScoreSet.publication_identifier_associations),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.created_by),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.modified_by),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.license),
            contains_eager(MappedVariant.variant).contains_eager(Variant.score_set).selectinload(ScoreSet.experiment),
        )
        .all()
    )

    if not mapped_variants:
        logger.info(msg="No mapped variants are associated with the requested score set.", extra=logging_context())
        raise HTTPException(
            status_code=404,
            detail=f"No mapped variants associated with score set URN {urn} were found. Could not construct study results.",
        )

    return StreamingResponse(
        _stream_generated_annotations(mapped_variants, variant_study_result),
        media_type="application/x-ndjson",
        headers={
            "X-Total-Count": str(len(mapped_variants)),
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
            created_calibration_item = await create_score_calibration(db, calibration_create, user_data.user)
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

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"created_resource": item.urn})

    enriched_experiment = enrich_experiment_with_num_score_sets(item.experiment, user_data)
    return score_set.ScoreSet.model_validate(item).copy(update={"experiment": enriched_experiment})


@router.post(
    "/score-sets/{urn}/variants/data",
    response_model=score_set.ScoreSet,
    response_model_exclude_none=True,
    responses={**BASE_400_RESPONSE, **ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Upload score and variant count files for a score set",
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

    await enqueue_variant_creation(
        item=item,
        user_data=user_data,
        new_scores_df=score_set_variants_data["scores_df"],
        new_counts_df=score_set_variants_data["counts_df"],
        new_score_columns_metadata=dataset_column_metadata.get("score_columns_metadata", {}),
        new_count_columns_metadata=dataset_column_metadata.get("count_columns_metadata", {}),
        worker=worker,
    )

    db.add(item)
    db.commit()
    db.refresh(item)

    enriched_experiment = enrich_experiment_with_num_score_sets(item.experiment, user_data)
    return score_set.ScoreSet.model_validate(item).copy(update={"experiment": enriched_experiment})


@router.patch(
    "/score-sets-with-variants/{urn}",
    response_model=score_set.ScoreSet,
    response_model_exclude_none=True,
    responses={**BASE_400_RESPONSE, **ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Update score ranges / calibrations for a score set",
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
) -> Any:
    """
    Update a score set and variants.
    """
    logger.info(msg="Began score set with variants update.", extra=logging_context())

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
        )

    db.add(updatedItem)
    db.commit()
    db.refresh(updatedItem)

    enriched_experiment = enrich_experiment_with_num_score_sets(updatedItem.experiment, user_data)
    return score_set.ScoreSet.model_validate(updatedItem).copy(update={"experiment": enriched_experiment})


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
        await enqueue_variant_creation(item=updatedItem, user_data=user_data, worker=worker)

        db.add(updatedItem)
        db.commit()
        db.refresh(updatedItem)

    enriched_experiment = enrich_experiment_with_num_score_sets(updatedItem.experiment, user_data)
    return score_set.ScoreSet.model_validate(updatedItem).copy(update={"experiment": enriched_experiment})


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

    # await the insertion of this job into the worker queue, not the job itself.
    job = await worker.enqueue_job("refresh_published_variants_view", correlation_id_for_context())
    if job is not None:
        save_to_logging_context({"worker_job_id": job.job_id})
        logger.info(msg="Enqueud published variant materialized view refresh job.", extra=logging_context())
    else:
        logger.warning(
            msg="Failed to enqueue published variant materialized view refresh job.", extra=logging_context()
        )

    enriched_experiment = enrich_experiment_with_num_score_sets(item.experiment, user_data)
    return score_set.ScoreSet.model_validate(item).copy(update={"experiment": enriched_experiment})


@router.get(
    "/score-sets/{urn}/clinical-controls",
    status_code=200,
    response_model=list[clinical_control.ClinicalControlWithMappedVariants],
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Get clinical controls for a score set",
)
async def get_clinical_controls_for_score_set(
    *,
    urn: str,
    # We'd prefer to reserve `db` as a query parameter.
    _db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
    db: Optional[str] = None,
    version: Optional[str] = None,
) -> Sequence[ClinicalControl]:
    """
    Fetch relevant clinical controls for a given score set.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "clinical_controls"})

    # Rename user facing kwargs for consistency with code base naming conventions. My-py doesn't care for us redefining db.
    db_name = db
    db_version = version

    item: Optional[ScoreSet] = _db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
    if not item:
        logger.info(
            msg="Failed to fetch clinical controls for score set; The requested score set does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.READ)

    clinical_controls_query = (
        select(ClinicalControl)
        .join(ClinicalControl.mapped_variants)
        .join(MappedVariant.variant)
        .options(contains_eager(ClinicalControl.mapped_variants).contains_eager(MappedVariant.variant))
        .filter(MappedVariant.current.is_(True))
        .filter(Variant.score_set_id == item.id)
    )

    if db_name is not None:
        save_to_logging_context({"db_name": db_name})
        clinical_controls_query = clinical_controls_query.filter(ClinicalControl.db_name == db_name)

    if db_version is not None:
        save_to_logging_context({"db_version": db_version})
        clinical_controls_query = clinical_controls_query.filter(ClinicalControl.db_version == db_version)

    clinical_controls: Sequence[ClinicalControl] = _db.scalars(clinical_controls_query).unique().all()

    if not clinical_controls:
        logger.info(
            msg="No clinical control variants matching the provided filters are associated with the requested score set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"No clinical control variants matching the provided filters associated with score set URN {urn} were found",
        )

    save_to_logging_context({"resource_count": len(clinical_controls)})

    return clinical_controls


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
    # We'd prefer to reserve `db` as a query parameter.
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
) -> list[dict[str, Union[str, list[str]]]]:
    """
    Fetch clinical control options for a given score set.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "clinical_control_options"})

    item: Optional[ScoreSet] = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
    if not item:
        logger.info(
            msg="Failed to fetch clinical control options for score set; The requested score set does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.READ)

    clinical_controls_query = (
        select(ClinicalControl.db_name, ClinicalControl.db_version)
        .join(MappedVariant, ClinicalControl.mapped_variants)
        .join(Variant)
        .where(Variant.score_set_id == item.id)
    )

    clinical_controls_for_item = db.execute(clinical_controls_query).unique()

    # NOTE: We return options even for pairwise groupings which may have no associated mapped variants
    #       and 404 when ultimately requested together.
    clinical_control_options: dict[str, list[str]] = {}
    for db_name, db_version in clinical_controls_for_item:
        clinical_control_options.setdefault(db_name, []).append(db_version)

    if not clinical_control_options:
        logger.info(
            msg="Failed to fetch clinical control options for score set; No clinical control variants are associated with this score set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"no clinical control variants associated with score set URN {urn} were found",
        )

    return [
        dict(zip(("db_name", "available_versions"), (db_name, db_versions)))
        for db_name, db_versions in clinical_control_options.items()
    ]


@router.get(
    "/score-sets/{urn}/gnomad-variants",
    status_code=200,
    response_model=list[gnomad_variant.GnomADVariantWithMappedVariants],
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Get gnomad variants for a score set",
)
async def get_gnomad_variants_for_score_set(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(get_current_user),
    version: Optional[str] = None,
) -> Sequence[GnomADVariant]:
    """
    Fetch relevant gnomad variants for a given score set.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "gnomad_variants"})

    # Rename user facing kwargs for consistency with code base naming conventions. My-py doesn't care for us redefining db.
    db_version = version

    item: Optional[ScoreSet] = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one_or_none()
    if not item:
        logger.info(
            msg="Failed to fetch gnomad variants for score set; The requested score set does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.READ)

    gnomad_variants_query = (
        select(GnomADVariant)
        .join(MappedVariant, GnomADVariant.mapped_variants)
        .join(Variant)
        .where(Variant.score_set_id == item.id)
    )

    if db_version is not None:
        save_to_logging_context({"db_version": db_version})
        gnomad_variants_query = gnomad_variants_query.where(GnomADVariant.db_version == db_version)

    gnomad_variants_for_item: Sequence[GnomADVariant] = db.scalars(gnomad_variants_query).all()
    gnomad_variants_with_mapped_variant = []
    for gnomad_variant_in_item in gnomad_variants_for_item:
        gnomad_variant_in_item.mapped_variants = [
            mv for mv in gnomad_variant_in_item.mapped_variants if mv.current and mv.variant.score_set_id == item.id
        ]

        if gnomad_variant_in_item.mapped_variants:
            gnomad_variants_with_mapped_variant.append(gnomad_variant_in_item)

    if not gnomad_variants_with_mapped_variant:
        logger.info(
            msg="No gnomad variants matching the provided filters are associated with the requested score set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"No gnomad variants matching the provided filters associated with score set URN {urn} were found",
        )

    save_to_logging_context({"resource_count": len(gnomad_variants_for_item)})

    return gnomad_variants_for_item
