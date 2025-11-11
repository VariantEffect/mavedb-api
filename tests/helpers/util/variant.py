import json
from typing import Optional
from unittest.mock import patch

from arq import ArqRedis
from cdot.hgvs.dataproviders import RESTDataProvider
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.score_sets import columns_for_dataset, create_variants, create_variants_data, csv_data_to_df
from mavedb.lib.validation.dataframe.dataframe import validate_and_standardize_dataframe_pair
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.variant import Variant
from mavedb.view_models.score_set_dataset_columns import DatasetColumnsCreate
from tests.helpers.constants import (
    TEST_MINIMAL_POST_MAPPED_METADATA,
    TEST_MINIMAL_PRE_MAPPED_METADATA,
)


def mock_worker_variant_insertion(
    client: TestClient,
    db: Session,
    data_provider: RESTDataProvider,
    score_set: dict,
    scores_csv_path: str,
    counts_csv_path: Optional[str] = None,
    score_columns_metadata_json_path: Optional[str] = None,
    count_columns_metadata_json_path: Optional[str] = None,
) -> None:
    with (
        open(scores_csv_path, "rb") as score_file,
        patch.object(ArqRedis, "enqueue_job", return_value=None) as worker_queue,
    ):
        files = {"scores_file": (scores_csv_path.name, score_file, "rb")}

        if counts_csv_path is not None:
            counts_file = open(counts_csv_path, "rb")
            files["counts_file"] = (counts_csv_path.name, counts_file, "rb")
        else:
            counts_file = None

        if score_columns_metadata_json_path is not None:
            score_columns_metadata_file = open(score_columns_metadata_json_path, "rb")
            files["score_columns_metadata_file"] = (
                score_columns_metadata_json_path.name,
                score_columns_metadata_file,
                "rb",
            )
        else:
            score_columns_metadata_file = None

        if count_columns_metadata_json_path is not None:
            count_columns_metadata_file = open(count_columns_metadata_json_path, "rb")
            files["count_columns_metadata_file"] = (
                count_columns_metadata_json_path.name,
                count_columns_metadata_file,
                "rb",
            )
        else:
            count_columns_metadata_file = None

        response = client.post(f"/api/v1/score-sets/{score_set['urn']}/variants/data", files=files)

        # Assert we have mocked a job being added to the queue, and that the request succeeded. The
        # response value here isn't important- we will add variants to the score set manually.
        worker_queue.assert_called_once()
        assert response.status_code == 200

        for file in (counts_file, score_columns_metadata_file, count_columns_metadata_file):
            if file is not None:
                file.close()

    # Reopen files since their buffers are consumed while mocking the variant data post request.
    with open(scores_csv_path, "rb") as score_file:
        score_df = csv_data_to_df(score_file)

    if counts_csv_path is not None:
        with open(counts_csv_path, "rb") as counts_file:
            counts_df = csv_data_to_df(counts_file)
    else:
        counts_df = None

    if score_columns_metadata_json_path is not None:
        with open(score_columns_metadata_json_path, "rb") as score_columns_metadata_file:
            score_columns_metadata = json.load(score_columns_metadata_file)
    else:
        score_columns_metadata = None

    if count_columns_metadata_json_path is not None:
        with open(count_columns_metadata_json_path, "rb") as count_columns_metadata_file:
            count_columns_metadata = json.load(count_columns_metadata_file)
    else:
        count_columns_metadata = None

    # Insert variant manually, worker jobs are tested elsewhere separately.
    item = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set["urn"])).one_or_none()
    assert item is not None

    scores, counts, score_columns_metadata, count_columns_metadata = validate_and_standardize_dataframe_pair(
        score_df, counts_df, score_columns_metadata, count_columns_metadata, item.target_genes, data_provider
    )
    variants = create_variants_data(scores, counts, None)
    num_variants = create_variants(db, item, variants)
    assert num_variants == 3

    item.processing_state = ProcessingState.success
    item.dataset_columns = DatasetColumnsCreate(
        score_columns=columns_for_dataset(scores),
        count_columns=columns_for_dataset(counts),
        score_columns_metadata=score_columns_metadata if score_columns_metadata is not None else {},
        count_columns_metadata=count_columns_metadata if count_columns_metadata is not None else {},
    ).model_dump()

    db.add(item)
    db.commit()

    return client.get(f"api/v1/score-sets/{score_set['urn']}").json()


def create_mapped_variants_for_score_set(db, score_set_urn, mapped_variant: dict[str, any]):
    score_set = db.scalar(select(ScoreSet).where(ScoreSet.urn == score_set_urn))
    targets = db.scalars(select(TargetGene).where(TargetGene.score_set_id == score_set.id))
    variants = db.scalars(select(Variant).where(Variant.score_set_id == score_set.id)).all()

    for variant in variants:
        mv = MappedVariant(**mapped_variant, variant_id=variant.id)
        db.add(mv)

    for target in targets:
        target.pre_mapped_metadata = TEST_MINIMAL_PRE_MAPPED_METADATA
        target.post_mapped_metadata = TEST_MINIMAL_POST_MAPPED_METADATA
        db.add(target)

    score_set.mapping_state = MappingState.complete
    db.commit()
    return


def clear_first_mapped_variant_post_mapped(session, score_set_urn):
    db_score_set = session.query(ScoreSet).filter(ScoreSet.urn == score_set_urn).one()
    variants = db_score_set.variants

    if variants:
        first_var = variants[0]
        first_var.mapped_variants[0].post_mapped = None
        session.commit()

        return first_var
