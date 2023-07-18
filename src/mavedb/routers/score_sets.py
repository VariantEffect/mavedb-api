import csv
import io
import logging
import re
from datetime import date
from typing import Any, List, Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, File, status, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import MultipleResultsFound

from mavedb import deps
from mavedb.lib.authorization import get_current_user, require_current_user
from mavedb.lib.identifiers import (
    create_external_gene_identifier_offset,
    find_or_create_doi_identifier,
    find_or_create_publication_identifier,
)
from mavedb.lib.permissions import Action, has_permission
from mavedb.lib.score_sets import (
    create_variants_data,
    find_meta_analyses_for_experiment_sets,
    search_score_sets as _search_score_sets,
    VariantData,
)
from mavedb.lib.urns import generate_experiment_set_urn, generate_experiment_urn, generate_score_set_urn
from mavedb.lib.validation import exceptions
from mavedb.lib.validation.constants.general import null_values_list
from mavedb.lib.validation.dataframe import validate_and_standardize_dataframe_pair
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.experiment import Experiment
from mavedb.models.license import License
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.reference_map import ReferenceMap
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.models.wild_type_sequence import WildTypeSequence
from mavedb.view_models import mapped_variant
from mavedb.view_models import score_set
from mavedb.view_models.search import ScoreSetsSearch

logger = logging.getLogger(__name__)

null_values_re = re.compile(r"\s+|none|nan|na|undefined|n/a|null|nil", flags=re.IGNORECASE)


async def fetch_score_set_by_urn(db, urn: str, owner: Optional[User]) -> Optional[ScoreSet]:
    """
    Fetch one score set by URN, ensuring that it is either published or owned by a specified user.

    :param db: An active database session.
    :param urn: The score set URN.
    :param owner: A user whose private score sets may be included in the search. If None, then the score set will only
        be returned if it is public.
    :return: The score set, or None if the URL was not found or refers to a private score set not owned by the specified
        user.
    """
    try:
        permission_filter = ScoreSet.private.is_(False)
        if owner is not None:
            permission_filter = or_(
                permission_filter,
                ScoreSet.created_by_id == owner.id,
            )
        item = db.query(ScoreSet).filter(ScoreSet.urn == urn).filter(permission_filter).one_or_none()
    except MultipleResultsFound:
        raise HTTPException(status_code=500, detail=f"multiple score sets with URN '{urn}' were found")
    if not item:
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")
    return item


def is_null(value):
    """Return True if a string represents a null value."""
    value = str(value).strip().lower()
    return null_values_re.fullmatch(value) or not value


router = APIRouter(prefix="/api/v1", tags=["score sets"], responses={404: {"description": "not found"}})


@router.post("/score-sets/search", status_code=200, response_model=list[score_set.ShortScoreSet])
def search_score_sets(search: ScoreSetsSearch, db: Session = Depends(deps.get_db)) -> Any:  # = Body(..., embed=True),
    """
    Search score sets.
    """
    return _search_score_sets(db, None, search)


@router.post("/me/score-sets/search", status_code=200, response_model=list[score_set.ShortScoreSet])
def search_my_score_sets(
    search: ScoreSetsSearch,  # = Body(..., embed=True),
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Search score sets created by the current user..
    """
    return _search_score_sets(db, user, search)


@router.get(
    "/score-sets/{urn}",
    status_code=200,
    response_model=score_set.ScoreSet,
    responses={404: {}, 500: {}},
    response_model_exclude_none=True,
)
async def show_score_set(
    *, urn: str, db: Session = Depends(deps.get_db), user: User = Depends(get_current_user)
) -> Any:
    """
    Fetch a single score set by URN.
    """

    return await fetch_score_set_by_urn(db, urn, user)


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
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Any:
    """
    Return scores from a score set, identified by URN, in CSV format.
    """
    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")
    permission = has_permission(user, score_set, Action.READ)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)

    columns = ["accession", "hgvs_nt", "hgvs_splice", "hgvs_pro"] + score_set.dataset_columns["score_columns"]
    type_column = "score_data"
    rows_data = get_csv_rows_data(score_set.variants, columns=columns, dtype=type_column)
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=columns, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(rows_data)
    return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")


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
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Any:
    """
    Return counts from a score set, identified by URN, in CSV format.
    """
    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")
    permission = has_permission(user, score_set, Action.READ)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)

    columns = ["accession", "hgvs_nt", "hgvs_splice", "hgvs_pro"] + score_set.dataset_columns["count_columns"]
    type_column = "count_data"
    rows_data = get_csv_rows_data(score_set.variants, columns=columns, dtype=type_column)
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=columns, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(rows_data)
    return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")


@router.get("/score-sets/{urn}/mapped-variants", status_code=200, response_model=list[mapped_variant.MappedVariant])
def get_score_set_mapped_variants(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Any:
    """
    Return mapped variants from a score set, identified by URN.
    """
    score_set = db.query(ScoreSet).filter(ScoreSet.urn == urn).first()
    if not score_set:
        raise HTTPException(status_code=404, detail=f"score set with URN {urn} not found")
    permission = has_permission(user, score_set, Action.READ)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)

    mapped_variants = (
        db.query(MappedVariant)
        .filter(ScoreSet.urn == urn)
        .filter(ScoreSet.id == Variant.score_set_id)
        .filter(Variant.id == MappedVariant.variant_id)
        .all()
    )

    if not mapped_variants:
        raise HTTPException(status_code=404, detail=f"No mapped variant associated with score set URN {urn} was found")

    return mapped_variants


class HGVSColumns:
    NUCLEOTIDE: str = "hgvs_nt"  # dataset.constants.hgvs_nt_column
    TRANSCRIPT: str = "hgvs_splice"  # dataset.constants.hgvs_splice_column
    PROTEIN: str = "hgvs_pro"  # dataset.constants.hgvs_pro_column

    @classmethod
    def options(cls) -> List[str]:
        return [cls.NUCLEOTIDE, cls.TRANSCRIPT, cls.PROTEIN]


@router.post("/score-sets/", response_model=score_set.ScoreSet, responses={422: {}}, response_model_exclude_none=True)
async def create_score_set(
    *,
    item_create: score_set.ScoreSetCreate,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Create a score set.
    """

    # TODO Confirm that the experiment is editable by this user.

    if item_create is None:
        return None

    experiment: Experiment = None
    if item_create.experiment_urn is not None:
        experiment = db.query(Experiment).filter(Experiment.urn == item_create.experiment_urn).one_or_none()
        if not experiment:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown experiment")
        permission = has_permission(user, experiment, Action.ADD_SCORE_SET)
        if not permission.permitted:
            raise HTTPException(status_code=permission.http_code, detail=permission.message)

    license_ = db.query(License).filter(License.id == item_create.license_id).one_or_none()
    if not license_:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown license")

    if item_create.superseded_score_set_urn is not None:
        superseded_score_set = await fetch_score_set_by_urn(db, item_create.superseded_score_set_urn, user)
        if superseded_score_set is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown superseded score set")
    else:
        superseded_score_set = None

    distinct_meta_analyzes_score_set_urns = list(set(item_create.meta_analyzes_score_set_urns or []))
    meta_analyzes_score_sets = [
        await fetch_score_set_by_urn(db, urn, None) for urn in distinct_meta_analyzes_score_set_urns
    ]
    for i, meta_analyzes_score_set in enumerate(meta_analyzes_score_sets):
        if meta_analyzes_score_set is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown meta-analyzed score set {distinct_meta_analyzes_score_set_urns[i]}",
            )

    if len(meta_analyzes_score_sets) > 0:
        # If any existing score set is a meta-analysis for score sets in the same collection of exepriment sets, use its
        # experiment as the parent of our new meta-analysis. Otherwise, create a new experiment.
        meta_analyzes_experiment_sets = list(
            set(map(lambda ss: ss.experiment.experiment_set, meta_analyzes_score_sets))
        )
        meta_analyzes_experiment_set_urns = list(
            set(map(lambda ss: ss.experiment.experiment_set.urn, meta_analyzes_score_sets))
        )
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
                created_by=user,
                modified_by=user,
            )
        else:
            experiment = Experiment(
                title=item_create.title,
                short_description=item_create.short_description,
                abstract_text=item_create.abstract_text,
                method_text=item_create.method_text,
                extra_metadata={},
                created_by=user,
                modified_by=user,
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

    wt_sequence = WildTypeSequence(**jsonable_encoder(item_create.target_gene.wt_sequence, by_alias=False))
    target_gene = TargetGene(
        **jsonable_encoder(
            item_create.target_gene,
            by_alias=False,
            exclude={"external_identifiers", "reference_maps", "wt_sequence"},
        ),
        wt_sequence=wt_sequence,
    )
    for external_gene_identifier_offset_create in item_create.target_gene.external_identifiers:
        offset = external_gene_identifier_offset_create.offset
        identifier_create = external_gene_identifier_offset_create.identifier
        await create_external_gene_identifier_offset(
            db, target_gene, identifier_create.db_name, identifier_create.identifier, offset
        )
    reference_map = ReferenceMap(genome_id=item_create.target_gene.reference_maps[0].genome_id, target=target_gene)
    item = ScoreSet(
        **jsonable_encoder(
            item_create,
            by_alias=False,
            exclude={
                "doi_identifiers",
                "experiment_urn",
                "keywords",
                "license_id",
                "meta_analyzes_score_set_urns",
                "primary_publication_identifiers",
                "secondary_publication_identifiers",
                "superseded_score_set_urn",
                "target_gene",
            },
        ),
        experiment=experiment,
        license=license_,
        superseded_score_set=superseded_score_set,
        meta_analyzes_score_sets=meta_analyzes_score_sets,
        target_gene=target_gene,
        doi_identifiers=doi_identifiers,
        publication_identifiers=publication_identifiers,
        processing_state=ProcessingState.incomplete,
        created_by=user,
        modified_by=user,
    )
    await item.set_keywords(db, item_create.keywords)
    db.add(item)
    db.commit()
    db.refresh(item)
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
    user: User = Depends(require_current_user),
) -> Any:
    """
    Upload scores and variant count files for a score set, and initiate processing these files to
    create variants.
    """

    # TODO Confirm access.

    # item = db.query(ScoreSet).filter(ScoreSet.urn == urn).filter(ScoreSet.private.is_(False)).one_or_none()
    item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item.urn or not scores_file:
        return None
    permission = has_permission(user, item, Action.SET_SCORES)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)

    # Delete the old variants so that uploading new scores and counts won't accumulate the old ones.
    db.query(Variant).filter(Variant.score_set_id == item.id).delete()

    extra_na_values = set(
        list(null_values_list)
        + [str(x).lower() for x in null_values_list]
        + [str(x).upper() for x in null_values_list]
        + [str(x).capitalize() for x in null_values_list]
    )
    scores_df = pd.read_csv(
        filepath_or_buffer=scores_file.file,
        sep=",",
        encoding="utf-8",
        quotechar="'",
        comment="#",
        na_values=extra_na_values,
        keep_default_na=True,
        dtype={**{col: str for col in HGVSColumns.options()}, "scores": float},
    )  # .replace(null_values_re, np.NaN) String will be replaced to NaN value
    for c in HGVSColumns.options():
        if c not in scores_df.columns:
            scores_df[c] = np.NaN
    score_columns = [col for col in scores_df.columns if col not in HGVSColumns.options()]
    counts_df = None
    count_columns = []
    if counts_file and counts_file.filename:
        counts_df = pd.read_csv(
            filepath_or_buffer=counts_file.file,
            sep=",",
            encoding="utf-8",
            quotechar="'",
            comment="#",
            na_values=extra_na_values,
            keep_default_na=True,
            dtype={**{col: str for col in HGVSColumns.options()}, "scores": float},
        ).replace(null_values_re, np.NaN)
        for c in HGVSColumns.options():
            if c not in counts_df.columns:
                counts_df[c] = np.NaN

        count_columns = [col for col in counts_df.columns if col not in HGVSColumns.options()]
    """
    if counts_file:
        try:
            validate_column_names(counts_df)
        except exceptions.ValidationError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    """
    if scores_file:
        try:
            validate_and_standardize_dataframe_pair(
                scores_df, counts_df, item.target_gene.wt_sequence.sequence, item.target_gene.wt_sequence.sequence_type
            )
        except exceptions.ValidationError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    variants_data = create_variants_data(scores_df, counts_df, None)  # , index_col)

    item.num_variants = create_variants(db, item, variants_data)
    logger.info(f"saving variants for {item.urn}")

    item.dataset_columns = {"count_columns": list(count_columns), "score_columns": list(score_columns)}
    item.modified_by = user
    db.add(item)

    # create_variants_task.submit_task(kwargs={
    #    'user_id': None,
    #    'score_set_urn': item.urn,
    #    'scores': None
    #    # 'counts',
    #    # 'index_col',
    #    # 'dataset_columns'
    # })

    db.commit()
    db.refresh(item)
    return item


# @classmethod
# @transaction.atomic
def create_variants(db, score_set: ScoreSet, variants_data: list[VariantData], batch_size=None) -> int:
    num_variants = len(variants_data)
    variant_urns = bulk_create_urns(num_variants, score_set, True)
    variants = (
        Variant(urn=urn, score_set_id=score_set.id, **kwargs) for urn, kwargs in zip(variant_urns, variants_data)
    )
    db.bulk_save_objects(variants)
    db.add(score_set)
    return len(score_set.variants)


# @staticmethod
def bulk_create_urns(n, score_set, reset_counter=False) -> List[str]:
    start_value = 0 if reset_counter else score_set.num_variants
    parent_urn = score_set.urn
    child_urns = ["{}#{}".format(parent_urn, start_value + (i + 1)) for i in range(n)]
    current_value = start_value + n
    score_set.num_variants = current_value
    return child_urns


@router.put("/score-sets/{urn}", response_model=score_set.ScoreSet, responses={422: {}})
async def update_score_set(
    *,
    urn: str,
    item_update: score_set.ScoreSetUpdate,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Update a score set.
    """

    if not item_update:
        raise HTTPException(status_code=400, detail="The request contained no updated item.")

    item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")
    permission = has_permission(user, item, Action.UPDATE)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)

    # Editing unpublished score set
    if item.private is True:
        license_ = None
        if item_update.license_id is not None:
            license_ = db.query(License).filter(License.id == item_update.license_id).one_or_none()
            if not license_:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown license")
            item.license = license_

        for var, value in vars(item_update).items():
            if var not in [
                "keywords",
                "doi_identifiers",
                "experiment_urn",
                "license_id",
                "secondary_publication_identifiers",
                "primary_publication_identifiers",
                "target_gene",
            ]:
                setattr(item, var, value) if value else None

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
        ]
        # create a temporary `primary` attribute on each of our publications that indicates
        # to our association proxy whether it is a primary publication or not
        primary_identifiers = [pub.identifier for pub in primary_publication_identifiers]
        for publication in publication_identifiers:
            setattr(publication, "primary", publication.identifier in primary_identifiers)

        item.publication_identifiers = publication_identifiers
        await item.set_keywords(db, item_update.keywords)

        # Delete the old target gene, WT sequence, and reference map. These will be deleted when we set the score set's
        # target_gene to None, because we have set cascade='all,delete-orphan' on ScoreSet.target_gene. (Since the
        # relationship is defined with the target gene as owner, this is actually set up in the backref attribute of
        # TargetGene.score_set.)
        #
        # We must flush our database queries now so that the old target gene will be deleted before inserting a new one
        # with the same score_set_id.
        item.target_gene = None
        db.flush()

        wt_sequence = WildTypeSequence(**jsonable_encoder(item_update.target_gene.wt_sequence, by_alias=False))

        target_gene = TargetGene(
            **jsonable_encoder(
                item_update.target_gene,
                by_alias=False,
                exclude={"external_identifiers", "reference_maps", "wt_sequence"},
            ),
            wt_sequence=wt_sequence,
        )
        for external_gene_identifier_offset_create in item_update.target_gene.external_identifiers:
            offset = external_gene_identifier_offset_create.offset
            identifier_create = external_gene_identifier_offset_create.identifier
            external_gene_identifier = await create_external_gene_identifier_offset(
                db, target_gene, identifier_create.db_name, identifier_create.identifier, offset
            )
        item.target_gene = target_gene

        reference_map = ReferenceMap(genome_id=item_update.target_gene.reference_maps[0].genome_id, target=target_gene)
        for var, value in vars(item_update).items():
            if var not in [
                "keywords",
                "doi_identifiers",
                "experiment_urn",
                "primary_publication_identifiers",
                "secondary_publication_identifiers",
                "target_gene",
            ]:
                setattr(item, var, value) if value else None

    # Editing published score set
    else:
        for var, value in vars(item_update).items():
            if var in ["title", "method_text", "abstract_text", "short_description"]:
                setattr(item, var, value) if value else None
    db.add(item)

    db.commit()
    db.refresh(item)
    return item


@router.delete("/score-sets/{urn}", responses={422: {}})
async def delete_score_set(
    *, urn: str, db: Session = Depends(deps.get_db), user: User = Depends(require_current_user)
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
    item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")
    permission = has_permission(user, item, Action.DELETE)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)
    db.delete(item)
    db.commit()


@router.post(
    "/score-sets/{urn}/publish", status_code=200, response_model=score_set.ScoreSet, response_model_exclude_none=True
)
def publish_score_set(
    *, urn: str, db: Session = Depends(deps.get_db), user: User = Depends(require_current_user)
) -> Any:
    """
    Publish a score set.
    """
    item: ScoreSet = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail=f"score set with URN '{urn}' not found")
    permission = has_permission(user, item, Action.UPDATE)
    if not permission.permitted:
        raise HTTPException(status_code=permission.http_code, detail=permission.message)

    if not item.experiment:
        raise HTTPException(
            status_code=500, detail="This score set does not belong to an experiment and cannot be published."
        )
    if not item.experiment.experiment_set:
        raise HTTPException(
            status_code=500,
            detail="This score set's experiment does not belong to an experiment set and cannot be published.",
        )
    # TODO This can probably be done more efficiently; at least, it's worth checking the SQL query that SQLAlchemy
    # generates when all we want is len(score_set.variants).
    if len(item.variants) == 0:
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

    if item.experiment.private or not item.experiment.published_date:
        item.experiment.urn = generate_experiment_urn(
            db, item.experiment.experiment_set, experiment_is_meta_analysis=len(item.meta_analyzes_score_sets) > 0
        )
        item.experiment.private = False
        item.experiment.published_date = published_date
        db.add(item.experiment)

    old_urn = item.urn
    item.urn = generate_score_set_urn(db, item.experiment)
    logger.info(f"publishing {old_urn} as {item.urn}")
    item.private = False
    item.published_date = published_date
    db.add(item)

    db.commit()
    db.refresh(item)
    return item


def get_csv_rows_data(variants, columns, dtype, na_rep="NA"):
    """
    Format each variant into a dictionary row containing the keys specified
    in `columns`.

    Parameters
    ----------
    variants : list[variant.models.Variant`]
        List of variants.
    columns : list[str]
        Columns to serialize.
    dtype : str, {'scores', 'counts'}
        The type of data requested. Either the 'score_data' or 'count_data'.
    na_rep : str
        String to represent null values.

    Returns
    -------
    list[dict]
    """
    row_dicts = []
    for variant in variants:
        data = {}
        for column_key in columns:
            if column_key == "hgvs_nt":
                value = str(variant.hgvs_nt)
            elif column_key == "hgvs_pro":
                value = str(variant.hgvs_pro)
            elif column_key == "hgvs_splice":
                value = str(variant.hgvs_splice)
            elif column_key == "accession":
                value = str(variant.urn)
            else:
                value = str(variant.data[dtype][column_key])
            if is_null(value):
                value = na_rep
            data[column_key] = value
        row_dicts.append(data)
    return row_dicts
