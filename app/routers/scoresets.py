import csv
import logging

import numpy as np
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
import io
import pandas as pd
import re

from mavecore.validation.constants import null_values_list
from sqlalchemy.orm import Session
from typing import Any
import uvicorn
from fastapi import Form, File, UploadFile, Request, FastAPI, Depends
from typing import List
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional
from fastapi.templating import Jinja2Templates
from fastapi import status
import pydantic
from fastapi.exceptions import HTTPException
from fastapi.encoders import jsonable_encoder


from app import deps
from app.lib.scoresets import create_variants
from app.models.enums.processing_state import ProcessingState
from app.models.experiment import Experiment
from app.models.scoreset import Scoreset
from app.tasks.scoreset_tasks import create_variants_task
from app.view_models import scoreset


logger = logging.getLogger(__name__)


null_values_re = re.compile(
    r'\s+|none|nan|na|undefined|n/a|null|nil',
    flags=re.IGNORECASE
)


def is_null(value):
    '''Return True if a string represents a null value.'''
    value = str(value).strip().lower()
    return null_values_re.fullmatch(value) or not value


router = APIRouter(
    prefix='/api/v1/scoresets',
    tags=['scoresets'],
    responses={404: {'description': 'Not found'}}
)


@router.get('/{urn}', status_code=200, response_model=scoreset.Scoreset, responses={404: {}})
def fetch_scoreset(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
) -> Any:
    '''
    Fetch a single scoreset by URN.
    '''
    item = db.query(Scoreset).filter(Scoreset.urn == urn).filter(Scoreset.private.is_(False)).first()
    if not item:
        raise HTTPException(
            status_code=404, detail=f'Scoreset with URN {urn} not found'
        )
    return item


@router.get(
    "/{urn}/scores",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": '''Variant scores in CSV format, with four fixed columns (accession, hgvs_nt, hgvs_pro,'''
                ''' and hgvs_splice), plus score columns defined by the scoreset.'''
        }
    }
)
def fetch_scoreset_scores(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Fetch scores from a scoreset, by scoreset URN.
    """
    scoreset = db.query(Scoreset).filter(Scoreset.urn==urn).first()
    if not scoreset:
        raise HTTPException(
            status_code=404, detail=f"Recipe with ID {id} not found"
        )
    columns = ["accession", "hgvs_nt", "hgvs_pro", "hgvs_splice"] + scoreset.dataset_columns["score_columns"]
    type_column = "score_data"
    rows = format_csv_rows(scoreset.variants, columns=columns, dtype=type_column)
    stream = io.StringIO()
    writer = csv.DictWriter(
        stream, fieldnames=columns, quoting=csv.QUOTE_MINIMAL
    )
    writer.writeheader()
    writer.writerows(rows)
    return StreamingResponse(iter([stream.getvalue()]),
                                 media_type="text/csv"
    )


class HGVSColumns:
    NUCLEOTIDE: str = 'hgvs_nt'  # dataset.constants.hgvs_nt_column
    TRANSCRIPT: str = 'hgvs_splice'  # dataset.constants.hgvs_splice_column
    PROTEIN: str = 'hgvs_pro'  # dataset.constants.hgvs_pro_column

    @classmethod
    def options(cls) -> List[str]:
        return [cls.NUCLEOTIDE, cls.TRANSCRIPT, cls.PROTEIN]


@router.post("/", response_model=scoreset.Scoreset, responses={422: {}})
async def create_scoreset(
    *,
    item_create: scoreset.ScoresetCreate,
    db: Session = Depends(deps.get_db)
) -> Any:
    '''
    Create a scoreset.
    '''
    if item_create is None:
        return None
    experiment = db.query(Experiment).filter(Experiment.urn == item_create.experiment_urn).one_or_none()
    if not experiment:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Unknown experiment')

    item = Scoreset(**jsonable_encoder(item_create, by_alias=False, exclude=['experiment_urn', 'keywords']))
    item.experiment = experiment
    item.processing_state = ProcessingState.incomplete
    item.set_keywords(db, item_create.keywords)
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.post("/{urn}/variants/data", response_model=scoreset.Scoreset, responses={422: {}})
async def upload_scoreset_variant_data(
    *,
    urn: str,
    counts_file: UploadFile = File(...),
    scores_file: UploadFile = File(...),
    db: Session = Depends(deps.get_db)
) -> Any:
    '''
    Upload scores and variant count files for a scoreset, and initiate processing these files to
    create variants.
    '''
    item = db.query(Scoreset).filter(Scoreset.urn == urn).filter(Scoreset.private.is_(False)).one_or_none()
    if not item.urn or not counts_file or not scores_file:
        return None

    extra_na_values = set(
        list(null_values_list)
        + [str(x).lower() for x in null_values_list]
        + [str(x).upper() for x in null_values_list]
        + [str(x).capitalize() for x in null_values_list]
    )

    scores_df = pd.read_csv(
        filepath_or_buffer=scores_file.file,
        sep=',',
        encoding='utf-8',
        quotechar='"',
        comment='#',
        na_values=extra_na_values,
        keep_default_na=True,
        dtype={
            **{col: str for col in HGVSColumns.options()},
            # **{c: str for c in cls.HGVSColumns.options()},
            'scores': float
            # MaveScoresDataset.AdditionalColumns.SCORES: float,
        }
    ).replace(null_values_re, np.NaN)

    counts_df = None
    if counts_file.filename:
        counts_df = pd.read_csv(
            filepath_or_buffer=counts_file.file,
            sep=',',
            encoding='utf-8',
            quotechar='"',
            comment='#',
            na_values=extra_na_values,
            keep_default_na=True,
            dtype={
                **{col: str for col in HGVSColumns.options()},
                # **{c: str for c in cls.HGVSColumns.options()},
                'scores': float
                # MaveScoresDataset.AdditionalColumns.SCORES: float,
            }
        ).replace(null_values_re, np.NaN)

    variants = create_variants(scores_df, counts_df, None) # , index_col)

    if variants:
        logger.info(f'{item.urn}:{variants[-1]}')
    logger.info(variants)

    # with transaction.atomic():
    logger.info(f'Deleting existing variants for {item.urn}')
    #self.instance.delete_variants()
    logger.info(f'Creating variants for {item.urn}')
    #Variant.bulk_create(self.instance, variants)
    logger.info(f'Saving {item.urn}')
    #self.instance.dataset_columns = dataset_columns
    #self.instance.save()

    #create_variants_task.submit_task(kwargs={
    #    'user_id': None,
    #    'scoreset_urn': item.urn,
    #    'scores': None
    #    # 'counts',
    #    # 'index_col',
    #    # 'dataset_columns'
    #})

    #db.add(item)
    #db.commit()
    #db.refresh(item)
    return item


@router.put("/{urn}", response_model=scoreset.Scoreset, responses={422: {}})
async def update_scoreset(
    *,
    urn: str,
    item_update: scoreset.ScoresetUpdate,
    db: Session = Depends(deps.get_db)
) -> Any:
    '''
    Update a scoreset.
    '''
    if item_update is None:
        return None
    item = db.query(Scoreset).filter(Scoreset.urn == urn).filter(Scoreset.private.is_(False)).one_or_none()
    if item is None:
        return None
    for var, value in vars(item_update).items():
        setattr(item, var, value) if value else None
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def format_csv_rows(variants, columns, dtype, na_rep="NA"):
    """
    Formats each variant into a dictionary row containing the keys specified
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
    rowdicts = []
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
        rowdicts.append(data)
    return rowdicts
