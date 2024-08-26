import logging

import pandas as pd
from cdot.hgvs.dataproviders import RESTDataProvider
from sqlalchemy import delete, select, null
from sqlalchemy.orm import Session

from mavedb.lib.score_sets import (
    calculate_score_set_statistics,
    columns_for_dataset,
    create_variants,
    create_variants_data,
)
from mavedb.lib.slack import send_slack_message
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.dataframe import (
    validate_and_standardize_dataframe_pair,
)
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


async def create_variants_for_score_set(
    ctx, score_set_urn: str, updater_id: int, scores: pd.DataFrame, counts: pd.DataFrame
):
    """
    Create variants for a score set. Intended to be run within a worker.
    On any raised exception, ensure ProcessingState of score set is set to `failed` prior
    to exiting.
    """
    try:
        db: Session = ctx["db"]
        hdp: RESTDataProvider = ctx["hdp"]

        score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
        updated_by = db.scalars(select(User).where(User.id == updater_id)).one()

        score_set.modified_by = updated_by
        score_set.processing_state = ProcessingState.processing

        db.add(score_set)
        db.commit()
        db.refresh(score_set)

        if not score_set.target_genes:
            raise ValueError("Can't create variants when score set has no targets.")

        if score_set.variants:
            db.execute(delete(Variant).where(Variant.score_set_id == score_set.id))

            db.commit()
            db.refresh(score_set)

        validated_scores, validated_counts = validate_and_standardize_dataframe_pair(
            scores, counts, score_set.target_genes, hdp
        )

        score_set.dataset_columns = {
            "score_columns": columns_for_dataset(validated_scores),
            "count_columns": columns_for_dataset(validated_counts),
        }

        variants_data = create_variants_data(validated_scores, validated_counts, None)
        create_variants(db, score_set, variants_data)
        score_set.statistics = calculate_score_set_statistics(score_set)
    
    # Validation errors arise from problematic user data. These should be inserted into the database so failures can
    # be persisted to them.
    except ValidationError as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        logger.error(f"Validation error while processing variants for {score_set.urn}", exc_info=e)
        score_set.processing_errors = {"exception": str(e), "detail": e.triggering_exceptions}

    # NOTE: Since these are likely to be internal errors, it makes less sense to add them to the DB and surface them to the end user.
    # Catch all non-system exiting exceptions.
    except Exception as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        logger.error(f"Encountered an exception while processing variants for {score_set.urn}", exc_info=e)
        score_set.processing_errors = {"exception": str(e), "detail": []}
        send_slack_message(err=e)

    # Catch all other exceptions and raise them. The exceptions caught here will be system exiting.
    except BaseException as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        db.commit()
        raise e
    else:
        score_set.processing_state = ProcessingState.success
        score_set.processing_errors = null()
    finally:
        db.add(score_set)
        db.commit()
        db.refresh(score_set)

    return score_set
