import logging
from datetime import date
from typing import Any, List, Optional

import pandas as pd
import pydantic
from arq import ArqRedis
from fastapi import APIRouter, Depends, File, Query, UploadFile, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import null, or_, select
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import UserData
from mavedb.lib.authorization import (
    get_current_user,
    require_current_user,
    require_current_user_with_email,
    RoleRequirer,
)
from mavedb.lib.contributors import find_or_create_contributor
from mavedb.lib.exceptions import MixedTargetError, NonexistentOrcidUserError, ValidationError
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
from mavedb.lib.score_sets import (
    csv_data_to_df,
    find_meta_analyses_for_experiment_sets,
    get_score_set_counts_as_csv,
    get_score_set_scores_as_csv,
    variants_to_csv_rows,
)
from mavedb.lib.score_sets import (
    fetch_superseding_score_set_in_search_result,
    search_score_sets as _search_score_sets,
    refresh_variant_urns,
)
from mavedb.lib.taxonomies import find_or_create_taxonomy
from mavedb.lib.urns import (
    generate_experiment_set_urn,
    generate_experiment_urn,
    generate_score_set_urn,
)
from mavedb.models.contributor import Contributor
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment import Experiment
from mavedb.models.license import License
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_sequence import TargetSequence
from mavedb.models.variant import Variant
from mavedb.view_models import mapped_variant, score_set, calibration
from mavedb.view_models.search import ScoreSetsSearch

logger = logging.getLogger(__name__)


async def fetch_score_set_by_urn(
    db, urn: str, user: Optional[UserData], owner_or_contributor: Optional[UserData], only_published: bool
) -> Optional[ScoreSet]:
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

    return item


router = APIRouter(
    prefix="/api/v1",
    tags=["score sets"],
    responses={404: {"description": "not found"}},
    route_class=LoggedRoute,
)


@router.post("/score-sets/search", status_code=200, response_model=list[score_set.ShortScoreSet])
def search_score_sets(
    search: ScoreSetsSearch,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:  # = Body(..., embed=True),
    """
    Search score sets.
    """
    score_sets = _search_score_sets(db, None, search)
    return fetch_superseding_score_set_in_search_result(score_sets, user_data, search)


@router.get("/score-sets/mapped-genes", status_code=200, response_model=dict[str, list[str]])
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
    response_model=list[score_set.ShortScoreSet],
)
def search_my_score_sets(
    search: ScoreSetsSearch,  # = Body(..., embed=True),
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Search score sets created by the current user..
    """
    score_sets = _search_score_sets(db, user_data.user, search)
    return fetch_superseding_score_set_in_search_result(score_sets, user_data, search)


@router.get(
    "/score-sets/{urn}",
    status_code=200,
    response_model=score_set.ScoreSet,
    responses={404: {}, 500: {}},
    response_model_exclude_none=True,
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
    return await fetch_score_set_by_urn(db, urn, user_data, None, False)


@router.get(
    "/score-sets/{urn}/scores",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": """Variant scores in CSV format, with four fixed columns (accession, hgvs_nt, hgvs_pro,"""
            """ and hgvs_splice), plus score columns defined by the score set.""",
        }
    },
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

    csv_str = get_score_set_scores_as_csv(db, score_set, start, limit, drop_na_columns)
    return StreamingResponse(iter([csv_str]), media_type="text/csv")


@router.get(
    "/score-sets/{urn}/counts",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": """Variant counts in CSV format, with four fixed columns (accession, hgvs_nt, hgvs_pro,"""
            """ and hgvs_splice), plus score columns defined by the score set.""",
        }
    },
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

    csv_str = get_score_set_counts_as_csv(db, score_set, start, limit, drop_na_columns)
    return StreamingResponse(iter([csv_str]), media_type="text/csv")


@router.get(
    "/score-sets/{urn}/mapped-variants",
    status_code=200,
    response_model=list[mapped_variant.MappedVariant],
)
def get_score_set_mapped_variants(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
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


@router.post(
    "/score-sets/",
    response_model=score_set.ScoreSet,
    responses={422: {}},
    response_model_exclude_none=True,
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
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown experiment")
        # Not allow add score set in meta-analysis experiments.
        if any(s.meta_analyzes_score_sets for s in experiment.score_sets):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Score sets may not be added to a meta-analysis experiment.",
            )

        save_to_logging_context({"experiment": experiment.urn})
        assert_permission(user_data, experiment, Action.ADD_SCORE_SET)

    license_ = db.query(License).filter(License.id == item_create.license_id).one_or_none()
    save_to_logging_context({"requested_license": item_create.license_id})

    if not license_:
        logger.info(msg="Failed to create score set; The requested license does not exist.", extra=logging_context())
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown license")
    elif not license_.active:
        logger.info(
            msg="Failed to create score set; The requested license is no longer active.", extra=logging_context()
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid license")

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
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unknown superseded score set",
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
                status_code=status.HTTP_400_BAD_REQUEST,
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
        raise pydantic.ValidationError(
            [pydantic.error_wrappers.ErrorWrapper(ValidationError(str(e)), loc="contributors")],
            model=score_set.ScoreSetCreate,
        )

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

    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    primary_identifiers = [pub.identifier for pub in primary_publication_identifiers]
    for publication in publication_identifiers:
        setattr(publication, "primary", publication.identifier in primary_identifiers)

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
            save_to_logging_context({"requested_taxonomy": gene.target_sequence.taxonomy.tax_id})
            taxonomy = await find_or_create_taxonomy(db, upload_taxonomy)

            if not taxonomy:
                logger.info(
                    msg="Failed to create score set; The requested taxonomy does not exist.", extra=logging_context()
                )
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown taxonomy")

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
                "score_ranges",
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
        score_ranges=item_create.score_ranges.dict() if item_create.score_ranges else null(),
    )  # type: ignore

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"created_resource": item.urn})
    return item


@router.post(
    "/score-sets/{urn}/variants/data",
    response_model=score_set.ScoreSet,
    responses={422: {}},
    response_model_exclude_none=True,
)
async def upload_score_set_variant_data(
    *,
    urn: str,
    counts_file: Optional[UploadFile] = File(None),
    scores_file: UploadFile = File(...),
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
    worker: ArqRedis = Depends(deps.get_worker),
) -> Any:
    """
    Upload scores and variant count files for a score set, and initiate processing these files to
    create variants.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "variants"})

    # item = db.query(ScoreSet).filter(ScoreSet.urn == urn).filter(ScoreSet.private.is_(False)).one_or_none()
    item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item or not item.urn:
        logger.info(msg="Failed to create variants; The requested score set does not exist.", extra=logging_context())
        return None

    assert_permission(user_data, item, Action.UPDATE)
    assert_permission(user_data, item, Action.SET_SCORES)

    try:
        scores_df = csv_data_to_df(scores_file.file)
        counts_df = None
        if counts_file and counts_file.filename:
            counts_df = csv_data_to_df(counts_file.file)
    # Handle non-utf8 file problem.
    except UnicodeDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Error decoding file: {e}. Ensure the file has correct values.")

    if scores_file:
        # Although this is also updated within the variant creation job, update it here
        # as well so that we can display the proper UI components (queue invocation delay
        # races the score set GET request).
        item.processing_state = ProcessingState.processing

        # await the insertion of this job into the worker queue, not the job itself.
        job = await worker.enqueue_job(
            "create_variants_for_score_set",
            correlation_id_for_context(),
            item.id,
            user_data.user.id,
            scores_df,
            counts_df,
        )
        if job is not None:
            save_to_logging_context({"worker_job_id": job.job_id})
        logger.info(msg="Enqueud variant creation job.", extra=logging_context())

    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.post(
    "/score-sets/{urn}/calibration/data",
    response_model=score_set.ScoreSet,
    responses={422: {}},
    response_model_exclude_none=True,
)
async def update_score_set_calibration_data(
    *,
    urn: str,
    calibration_update: dict[str, calibration.Calibration],
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(RoleRequirer([UserRole.admin])),
):
    """
    Update thresholds / score calibrations for a score set.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "score_thresholds"})

    try:
        item = db.scalars(select(ScoreSet).where(ScoreSet.urn == urn)).one()
    except NoResultFound:
        logger.info(
            msg="Failed to add score thresholds; The requested score set does not exist.", extra=logging_context()
        )
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.UPDATE)

    item.score_calibrations = {k: v.dict() for k, v in calibration_update.items()}
    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    return item


@router.put(
    "/score-sets/{urn}", response_model=score_set.ScoreSet, responses={422: {}}, response_model_exclude_none=True
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

    item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item:
        logger.info(msg="Failed to update score set; The requested score set does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")

    assert_permission(user_data, item, Action.UPDATE)

    for var, value in vars(item_update).items():
        if var not in [
            "contributors",
            "score_ranges",
            "doi_identifiers",
            "experiment_urn",
            "license_id",
            "secondary_publication_identifiers",
            "primary_publication_identifiers",
            "target_genes",
        ]:
            setattr(item, var, value) if value else None

    if item_update.license_id is not None:
        save_to_logging_context({"license": item_update.license_id})
        license_ = db.query(License).filter(License.id == item_update.license_id).one_or_none()

        if not license_:
            logger.info(
                msg="Failed to update score set; The requested license does not exist.", extra=logging_context()
            )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown license")

            # Allow in-active licenses to be retained on update if they already exist on the item.
        elif not license_.active and item.licence_id != item_update.license_id:
            logger.info(
                msg="Failed to update score set license; The requested license is no longer active.",
                extra=logging_context(),
            )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid license")

        item.license = license_

    item.doi_identifiers = [
        await find_or_create_doi_identifier(db, identifier.identifier)
        for identifier in item_update.doi_identifiers or []
    ]
    primary_publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
        for identifier in item_update.primary_publication_identifiers or []
    ]
    publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
        for identifier in item_update.secondary_publication_identifiers or []
    ] + primary_publication_identifiers

    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    primary_identifiers = [p.identifier for p in primary_publication_identifiers]
    for publication in publication_identifiers:
        setattr(publication, "primary", publication.identifier in primary_identifiers)

    item.publication_identifiers = publication_identifiers

    try:
        item.contributors = [
            await find_or_create_contributor(db, contributor.orcid_id) for contributor in item_update.contributors or []
        ]
    except NonexistentOrcidUserError as e:
        logger.error(msg="Could not find ORCID user with the provided user ID.", extra=logging_context())
        raise pydantic.ValidationError(
            [pydantic.error_wrappers.ErrorWrapper(ValidationError(str(e)), loc="contributors")],
            model=score_set.ScoreSetUpdate,
        )

    # Score set has not been published and attributes affecting scores may still be edited.
    if item.private:
        if item_update.score_ranges:
            item.score_ranges = item_update.score_ranges.dict()
        else:
            item.score_ranges = null()

        # Delete the old target gene, WT sequence, and reference map. These will be deleted when we set the score set's
        # target_gene to None, because we have set cascade='all,delete-orphan' on ScoreSet.target_gene. (Since the
        # relationship is defined with the target gene as owner, this is actually set up in the backref attribute of
        # TargetGene.score_set.)
        #
        # We must flush our database queries now so that the old target gene will be deleted before inserting a new one
        # with the same score_set_id.
        item.target_genes = []
        db.flush()

        targets: List[TargetGene] = []
        accessions = False
        for gene in item_update.target_genes:
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
                save_to_logging_context({"requested_taxonomy": gene.target_sequence.taxonomy.tax_id})
                taxonomy = await find_or_create_taxonomy(db, upload_taxonomy)

                if not taxonomy:
                    logger.info(
                        msg="Failed to create score set; The requested taxonomy does not exist.",
                        extra=logging_context(),
                    )
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Unknown taxonomy {gene.target_sequence.taxonomy.tax_id}",
                    )

                # If the target sequence has a label, use it. Otherwise, use the name from the target gene as the label.
                # View model validation rules enforce that sequences must have a label defined if there are more than one
                # targets defined on a score set.
                seq_label = gene.target_sequence.label if gene.target_sequence.label is not None else gene.name
                target_sequence = TargetSequence(
                    **jsonable_encoder(
                        gene.target_sequence,
                        by_alias=False,
                        exclude={"taxonomy", "label"},
                    ),
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

        item.target_genes = targets

        # re-validate existing variants and clear them if they do not pass validation
        if item.variants:
            assert item.dataset_columns is not None
            score_columns = [
                "hgvs_nt",
                "hgvs_splice",
                "hgvs_pro",
            ] + item.dataset_columns["score_columns"]
            count_columns = [
                "hgvs_nt",
                "hgvs_splice",
                "hgvs_pro",
            ] + item.dataset_columns["count_columns"]

            scores_data = pd.DataFrame(
                variants_to_csv_rows(item.variants, columns=score_columns, dtype="score_data")
            ).replace("NA", pd.NA)

            if item.dataset_columns["count_columns"]:
                count_data = pd.DataFrame(
                    variants_to_csv_rows(item.variants, columns=count_columns, dtype="count_data")
                ).replace("NA", pd.NA)
            else:
                count_data = None

            # Although this is also updated within the variant creation job, update it here
            # as well so that we can display the proper UI components (queue invocation delay
            # races the score set GET request).
            item.processing_state = ProcessingState.processing

            # await the insertion of this job into the worker queue, not the job itself.
            job = await worker.enqueue_job(
                "create_variants_for_score_set",
                correlation_id_for_context(),
                item.id,
                user_data.user.id,
                scores_data,
                count_data,
            )
            if job is not None:
                save_to_logging_context({"worker_job_id": job.job_id})
            logger.info(msg="Enqueud variant creation job.", extra=logging_context())
    else:
        logger.debug(msg="Skipped score range and target gene update. Score set is published.", extra=logging_context())

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    return item


@router.delete("/score-sets/{urn}", responses={422: {}})
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
            status_code=500,
            detail="This score set does not belong to an experiment and cannot be published.",
        )
    if not item.experiment.experiment_set:
        logger.info(
            msg="Failed to publish score set; The requested score set's experiment does not belong to an experiment set.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=500,
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
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
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
    job = await worker.enqueue_job(
        "refresh_published_variants_view",
        correlation_id_for_context(),
        user_data.user.id,
    )
    if job is not None:
        save_to_logging_context({"worker_job_id": job.job_id})
        logger.info(msg="Enqueud published variant materialized view refresh job.", extra=logging_context())
    else:
        logger.warning(
            msg="Failed to enqueue published variant materialized view refresh job.", extra=logging_context()
        )

    return item
