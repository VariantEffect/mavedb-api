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
from mavedb.lib.scoresets import create_variants_data, search_scoresets as _search_scoresets, VariantData
from mavedb.lib.urns import generate_experiment_set_urn, generate_experiment_urn, generate_scoreset_urn
from mavedb.lib.validation import exceptions
from mavedb.lib.validation.constants.general import null_values_list
from mavedb.lib.validation.dataframe import validate_and_standardize_dataframe_pair
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.experiment import Experiment
from mavedb.models.license import License
from mavedb.models.reference_map import ReferenceMap
from mavedb.models.scoreset import Scoreset
from mavedb.models.target_gene import TargetGene
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.models.wild_type_sequence import WildTypeSequence
from mavedb.view_models import scoreset
from mavedb.view_models.search import ScoresetsSearch

logger = logging.getLogger(__name__)

null_values_re = re.compile(r"\s+|none|nan|na|undefined|n/a|null|nil", flags=re.IGNORECASE)


async def fetch_scoreset_by_urn(db, urn: str, owner: Optional[User]) -> Optional[Scoreset]:
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
        permission_filter = Scoreset.private.is_(False)
        if owner is not None:
            permission_filter = or_(
                permission_filter,
                Scoreset.created_by_id == owner.id,
            )
        item = db.query(Scoreset).filter(Scoreset.urn == urn).filter(permission_filter).one_or_none()
    except MultipleResultsFound:
        raise HTTPException(status_code=500, detail=f"Multiple scoresets with URN {urn} were found.")
    if not item:
        raise HTTPException(status_code=404, detail=f"Scoreset with URN {urn} not found")
    return item


def is_null(value):
    """Return True if a string represents a null value."""
    value = str(value).strip().lower()
    return null_values_re.fullmatch(value) or not value


router = APIRouter(prefix="/api/v1", tags=["scoresets"], responses={404: {"description": "Not found"}})


@router.post("/scoresets/search", status_code=200, response_model=list[scoreset.ShortScoreset])
def search_scoresets(search: ScoresetsSearch, db: Session = Depends(deps.get_db)) -> Any:  # = Body(..., embed=True),
    """
    Search scoresets.
    """
    return _search_scoresets(db, None, search)


@router.post("/me/scoresets/search", status_code=200, response_model=list[scoreset.ShortScoreset])
def search_my_scoresets(
    search: ScoresetsSearch,  # = Body(..., embed=True),
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Search scoresets created by the current user..
    """
    return _search_scoresets(db, user, search)


@router.get("/scoresets/{urn}", status_code=200, response_model=scoreset.Scoreset, responses={404: {}, 500: {}})
async def show_scoreset(*, urn: str, db: Session = Depends(deps.get_db), user: User = Depends(get_current_user)) -> Any:
    """
    Fetch a single scoreset by URN.
    """

    return await fetch_scoreset_by_urn(db, urn, user)


@router.get(
    "/scoresets/{urn}/scores",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": """Variant scores in CSV format, with four fixed columns (accession, hgvs_nt, hgvs_pro,"""
            """ and hgvs_splice), plus score columns defined by the scoreset.""",
        }
    },
)
def get_scoreset_scores_csv(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Return scores from a scoreset, identified by URN, in CSV format.
    """
    scoreset = db.query(Scoreset).filter(Scoreset.urn == urn).first()
    if not scoreset:
        raise HTTPException(status_code=404, detail=f"Scoreset with URN {urn} not found")
    columns = ["accession", "hgvs_nt", "hgvs_splice", "hgvs_pro"] + scoreset.dataset_columns["score_columns"]
    type_column = "score_data"
    rows_data = get_csv_rows_data(scoreset.variants, columns=columns, dtype=type_column)
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=columns, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(rows_data)
    return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")


@router.get(
    "/scoresets/{urn}/counts",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": """Variant counts in CSV format, with four fixed columns (accession, hgvs_nt, hgvs_pro,"""
            """ and hgvs_splice), plus score columns defined by the scoreset.""",
        }
    },
)
async def get_scoreset_counts_csv(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Return counts from a scoreset, identified by URN, in CSV format.
    """
    scoreset = db.query(Scoreset).filter(Scoreset.urn == urn).first()
    if not scoreset:
        raise HTTPException(status_code=404, detail=f"Recipe with ID {id} not found")
    columns = ["accession", "hgvs_nt", "hgvs_splice", "hgvs_pro"] + scoreset.dataset_columns["count_columns"]
    type_column = "count_data"
    rows_data = get_csv_rows_data(scoreset.variants, columns=columns, dtype=type_column)
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=columns, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(rows_data)
    return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")


class HGVSColumns:
    NUCLEOTIDE: str = "hgvs_nt"  # dataset.constants.hgvs_nt_column
    TRANSCRIPT: str = "hgvs_splice"  # dataset.constants.hgvs_splice_column
    PROTEIN: str = "hgvs_pro"  # dataset.constants.hgvs_pro_column

    @classmethod
    def options(cls) -> List[str]:
        return [cls.NUCLEOTIDE, cls.TRANSCRIPT, cls.PROTEIN]


@router.post("/scoresets/", response_model=scoreset.Scoreset, responses={422: {}})
async def create_scoreset(
    *,
    item_create: scoreset.ScoresetCreate,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Create a scoreset.
    """

    # TODO Confirm that the experiment is editable by this user.

    if item_create is None:
        return None

    experiment = db.query(Experiment).filter(Experiment.urn == item_create.experiment_urn).one_or_none()
    if not experiment:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown experiment")

    license_ = db.query(License).filter(License.id == item_create.license_id).one_or_none()
    if not license_:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown license")

    if item_create.superseded_scoreset_urn is not None:
        superseded_scoreset = await fetch_scoreset_by_urn(db, item_create.superseded_scoreset_urn, user)
        if superseded_scoreset is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown superseded scoreset")
    else:
        superseded_scoreset = None
    meta_analysis_source_scoresets = [
        await fetch_scoreset_by_urn(db, urn) for urn in item_create.meta_analysis_source_scoreset_urns or []
    ]

    doi_identifiers = [
        await find_or_create_doi_identifier(db, identifier.identifier)
        for identifier in item_create.doi_identifiers or []
    ]
    primary_publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier)
        for identifier in item_create.primary_publication_identifiers or []
    ]
    publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier)
        for identifier in item_create.publication_identifiers or []
    ]
    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    for publication in publication_identifiers:
        setattr(publication, "primary", publication in primary_publication_identifiers)

    wt_sequence = WildTypeSequence(**jsonable_encoder(item_create.target_gene.wt_sequence, by_alias=False))
    target_gene = TargetGene(
        **jsonable_encoder(
            item_create.target_gene,
            by_alias=False,
            exclude=["external_identifiers", "reference_maps", "wt_sequence"],
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
    item = Scoreset(
        **jsonable_encoder(
            item_create,
            by_alias=False,
            exclude=[
                "doi_identifiers",
                "experiment_urn",
                "keywords",
                "license_id",
                "meta_analysis_source_scoreset_urns",
                "primary_publication_identifiers",
                "publication_identifiers",
                "superseded_scoreset_urn",
                "target_gene",
            ],
        ),
        experiment=experiment,
        license=license_,
        superseded_scoreset=superseded_scoreset,
        meta_analysis_source_scoresets=meta_analysis_source_scoresets,
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


@router.post("/scoresets/{urn}/variants/data", response_model=scoreset.Scoreset, responses={422: {}})
async def upload_scoreset_variant_data(
    *,
    urn: str,
    counts_file: Optional[UploadFile] = File(None),
    scores_file: UploadFile = File(...),
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Upload scores and variant count files for a scoreset, and initiate processing these files to
    create variants.
    """

    # TODO Confirm access.

    # item = db.query(Scoreset).filter(Scoreset.urn == urn).filter(Scoreset.private.is_(False)).one_or_none()
    item = db.query(Scoreset).filter(Scoreset.urn == urn).one_or_none()
    if not item.urn or not scores_file:
        return None

    # Delete the old variants so that uploading new scores and counts won't accumulate the old ones.
    db.query(Variant).filter(Variant.scoreset_id == item.id).delete()

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
    if variants_data:
        logger.error(f"{item.urn}:{variants_data[-1]}")

    item.num_variants = create_variants(db, item, variants_data)
    logger.error(f"Saving {item.urn}")

    item.dataset_columns = {"count_columns": list(count_columns), "score_columns": list(score_columns)}
    item.modified_by = user
    db.add(item)

    # create_variants_task.submit_task(kwargs={
    #    'user_id': None,
    #    'scoreset_urn': item.urn,
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
def create_variants(db, scoreset: Scoreset, variants_data: list[VariantData], batch_size=None) -> int:
    num_variants = len(variants_data)
    variant_urns = bulk_create_urns(num_variants, scoreset, True)
    variants = (Variant(urn=urn, scoreset_id=scoreset.id, **kwargs) for urn, kwargs in zip(variant_urns, variants_data))
    db.bulk_save_objects(variants)
    db.add(scoreset)
    return len(scoreset.variants)


# @staticmethod
def bulk_create_urns(n, scoreset, reset_counter=False) -> List[str]:
    start_value = 0 if reset_counter else scoreset.num_variants
    parent_urn = scoreset.urn
    child_urns = ["{}#{}".format(parent_urn, start_value + (i + 1)) for i in range(n)]
    current_value = start_value + n
    scoreset.num_variants = current_value
    return child_urns


@router.put("/scoresets/{urn}", response_model=scoreset.Scoreset, responses={422: {}})
async def update_scoreset(
    *,
    urn: str,
    item_update: scoreset.ScoresetUpdate,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Update a scoreset .
    """

    if not item_update:
        raise HTTPException(status_code=400, detail="The request contained no updated item.")

    # item = db.query(Scoreset).filter(Scoreset.urn == urn).filter(Scoreset.private.is_(False)).one_or_none()
    item = db.query(Scoreset).filter(Scoreset.urn == urn).one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail=f"Scoreset with URN {urn} not found.")
    # TODO Ensure that the current user has edit rights for this scoreset.

    # Editing unpublished scoreset
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
                "publication_identifiers",
                "primary_publication_identifiers",
                "target_gene",
            ]:
                setattr(item, var, value) if value else None

        item.doi_identifiers = [
            await find_or_create_doi_identifier(db, identifier.identifier)
            for identifier in item_update.doi_identifiers or []
        ]
        primary_publication_identifiers = [
            await find_or_create_publication_identifier(db, identifier.identifier)
            for identifier in item_update.primary_publication_identifiers or []
        ]
        publication_identifiers = [
            await find_or_create_publication_identifier(db, identifier.identifier)
            for identifier in item_update.publication_identifiers or []
        ]
        # create a temporary `primary` attribute on each of our publications that indicates
        # to our association proxy whether it is a primary publication or not
        for publication in publication_identifiers:
            setattr(publication, "primary", publication in primary_publication_identifiers)

        item.publication_identifiers = publication_identifiers
        await item.set_keywords(db, item_update.keywords)

        # Delete the old target gene, WT sequence, and reference map. These will be deleted when we set the scoreset's
        # target_gene to None, because we have set cascade='all,delete-orphan' on Scoreset.target_gene. (Since the
        # relationship is defined with the target gene as owner, this is actually set up in the backref attribute of
        # TargetGene.scoreset.)
        #
        # We must flush our database queries now so that the old target gene will be deleted before inserting a new one
        # with the same scoreset_id.
        item.target_gene = None
        db.flush()

        wt_sequence = WildTypeSequence(**jsonable_encoder(item_update.target_gene.wt_sequence, by_alias=False))

        target_gene = TargetGene(
            **jsonable_encoder(
                item_update.target_gene,
                by_alias=False,
                exclude=["external_identifiers", "reference_maps", "wt_sequence"],
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
                "publication_identifiers",
                "target_gene",
            ]:
                setattr(item, var, value) if value else None

    # Editing published scoreset
    else:
        for var, value in vars(item_update).items():
            if var in ["title", "method_text", "abstract_text", "short_description"]:
                setattr(item, var, value) if value else None
    db.add(item)

    db.commit()
    db.refresh(item)
    return item


@router.delete("/scoresets/{urn}", responses={422: {}})
async def delete_scoreset(
    *, urn: str, db: Session = Depends(deps.get_db), user: User = Depends(require_current_user)
) -> Any:
    """
    Delete a scoreset .

    Raises

    Returns
    _______
    Does not return anything
    string : HTTP code 200 successful but returning content
    or
    communitcate to client whether the operation succeeded
    204 if successful but not returning content - likely going with this
    """
    item = db.query(Scoreset).filter(Scoreset.urn == urn).one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail=f"Scoreset with URN {urn} not found.")
    # TODO Ensure that the current user has edit rights for this scoreset.
    db.delete(item)
    db.commit()


@router.post("/scoresets/{urn}/publish", status_code=200, response_model=scoreset.Scoreset)
def publish_scoreset(
    *, urn: str, db: Session = Depends(deps.get_db), user: User = Depends(require_current_user)
) -> Any:
    """
    Publish a scoreset.
    """
    item: Scoreset = db.query(Scoreset).filter(Scoreset.urn == urn).one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail=f"Scoreset with URN {urn} not found")
    # TODO Ensure that the current user has edit rights for this scoreset.
    if not item.experiment:
        raise HTTPException(
            status_code=500, detail="This scoreset does not belong to an experiment and cannot be published."
        )
    if not item.experiment.experiment_set:
        raise HTTPException(
            status_code=500,
            detail="This scoreset's experiment does not belong to an experiment set and cannot be published.",
        )

    published_date = date.today()

    if item.experiment.experiment_set.private or not item.experiment.experiment_set.published_date:
        item.experiment.experiment_set.urn = generate_experiment_set_urn(db)
        item.experiment.experiment_set.private = False
        item.experiment.experiment_set.published_date = published_date
        db.add(item.experiment.experiment_set)

    if item.experiment.private or not item.experiment.published_date:
        # TODO Pass experiment.is_meta_analysis instead of False
        item.experiment.urn = generate_experiment_urn(db, item.experiment.experiment_set, False)
        item.experiment.private = False
        item.experiment.published_date = published_date
        db.add(item.experiment)

    item.urn = generate_scoreset_urn(db, item.experiment)
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
