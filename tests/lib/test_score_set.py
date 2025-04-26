import io

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import select

from mavedb.lib.score_sets import (
    HGVSColumns,
    columns_for_dataset,
    create_variants,
    create_variants_data,
    csv_data_to_df,
)
from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    null_values_list,
    required_score_column,
)
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from tests.helpers.constants import TEST_SAVED_SCORE_SET_RANGE
from tests.helpers.util import create_acc_score_set, create_experiment, create_seq_score_set


def test_columns_for_dataset_no_dataset():
    dataset = None
    cols = columns_for_dataset(dataset)

    assert cols == []


def test_columns_for_dataset_empty_df():
    dataset = pd.DataFrame()
    cols = columns_for_dataset(dataset)

    assert cols == []


def test_columns_for_dataset_no_columns():
    columns = []
    dataset = pd.DataFrame(columns=columns)
    cols = columns_for_dataset(dataset)

    assert cols == columns


def test_columns_for_dataset_columns_exist():
    columns = ["col_1", "col_2", "col_3"]
    dataset = pd.DataFrame(columns=columns)
    cols = columns_for_dataset(dataset)

    assert cols == columns


def test_columns_for_dataset_hgvs_options_only():
    columns = HGVSColumns.options()
    dataset = pd.DataFrame(columns=columns)
    cols = columns_for_dataset(dataset)

    assert cols == []


def test_columns_for_dataset_mixed_options():
    columns = HGVSColumns.options() + ["col_1", "col_2", "col_3"]
    dataset = pd.DataFrame(columns=columns)
    cols = columns_for_dataset(dataset)

    assert cols == ["col_1", "col_2", "col_3"]


def test_csv_data_to_df_empty_buffer():
    byte_content = io.BytesIO(b"")
    with pytest.raises(pd.errors.EmptyDataError):
        csv_data_to_df(byte_content)


def test_csv_data_to_df_only_cols():
    byte_content = io.BytesIO(b"col_1,col_2")
    ingested_df = csv_data_to_df(byte_content)

    assert all([col in ingested_df.columns for col in ["col_1", "col_2"] + HGVSColumns.options()])
    assert ingested_df.empty


def test_csv_data_to_df_cols_with_values():
    byte_content = io.BytesIO(b"col_1,col_2\n1,test")
    ingested_df = csv_data_to_df(byte_content)

    assert ingested_df.loc[:, "col_1"].iloc[0] == 1
    assert ingested_df.loc[:, "col_2"].iloc[0] == "test"


def test_csv_data_to_df_cols_with_some_values():
    byte_content = io.BytesIO(b"col_1,col_2\n1,na")
    ingested_df = csv_data_to_df(byte_content)

    assert ingested_df.loc[:, "col_1"].iloc[0] == 1
    assert pd.isna(ingested_df.loc[:, "col_2"].iloc[0])


def test_csv_data_to_df_cols_with_empty_rows():
    byte_content = io.BytesIO(b"col_1,col_2\n,")
    ingested_df = csv_data_to_df(byte_content)

    assert all(~ingested_df.notna())


def test_csv_data_to_df_cols_custom_na_values():
    all_na_values = list(
        set(
            list(null_values_list)
            + [str(x).lower() for x in null_values_list]
            + [str(x).upper() for x in null_values_list]
            + [str(x).capitalize() for x in null_values_list]
        )
    )

    col_string = ""
    val_string = ""
    for idx, val in enumerate(all_na_values):
        col_string += f"col_{idx},"
        val_string += f"{val},"

    col_string.strip(",")
    val_string.strip(",")
    col_string += "\n"

    byte_content = io.BytesIO(str.encode(col_string + val_string))
    ingested_df = csv_data_to_df(byte_content)

    assert all(~ingested_df.notna())


BASE_VARIANTS_SCORE_DF = pd.DataFrame(
    {
        hgvs_nt_column: ["g.1A>G", "g.1A>T"],
        hgvs_splice_column: ["c.1A>G", "c.1A>T"],
        hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
        required_score_column: [1.0, 2.0],
    }
)
BASE_VARIANTS_COUNT_DF = pd.DataFrame(
    {
        hgvs_nt_column: ["g.1A>G", "g.1A>T"],
        hgvs_splice_column: ["c.1A>G", "c.1A>T"],
        hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
        "count1": [3.0, 5.0],
        "count2": [9, 10],
    }
)


def test_create_variants_data_empty_scores():
    variants = create_variants_data(pd.DataFrame())

    assert variants == []


def test_create_variants_data_scores():
    scores_df = BASE_VARIANTS_SCORE_DF
    variants = create_variants_data(scores_df)

    assert variants == [
        {
            "hgvs_nt": "g.1A>G",
            "hgvs_splice": "c.1A>G",
            "hgvs_pro": "p.Met1Val",
            "data": {
                "score_data": {
                    "score": 1.0,
                },
                "count_data": {},
            },
        },
        {
            "hgvs_nt": "g.1A>T",
            "hgvs_splice": "c.1A>T",
            "hgvs_pro": "p.Met1Leu",
            "data": {
                "score_data": {
                    "score": 2.0,
                },
                "count_data": {},
            },
        },
    ]


def test_create_variants_data_scores_and_counts():
    scores_df = BASE_VARIANTS_SCORE_DF
    counts_df = BASE_VARIANTS_COUNT_DF
    variants = create_variants_data(scores_df, counts_df)

    assert variants == [
        {
            "hgvs_nt": "g.1A>G",
            "hgvs_splice": "c.1A>G",
            "hgvs_pro": "p.Met1Val",
            "data": {
                "score_data": {
                    "score": 1.0,
                },
                "count_data": {
                    "count1": 3.0,
                    "count2": 9,
                },
            },
        },
        {
            "hgvs_nt": "g.1A>T",
            "hgvs_splice": "c.1A>T",
            "hgvs_pro": "p.Met1Leu",
            "data": {
                "score_data": {
                    "score": 2.0,
                },
                "count_data": {
                    "count1": 5.0,
                    "count2": 10,
                },
            },
        },
    ]


@pytest.mark.parametrize("null_column", [hgvs_nt_column, hgvs_pro_column, hgvs_splice_column])
def test_create_variants_data_scores_and_counts_na_cols(null_column):
    scores_df = BASE_VARIANTS_SCORE_DF
    counts_df = BASE_VARIANTS_COUNT_DF

    na_series = pd.Series([np.NaN, ""])
    scores_df[null_column] = na_series
    counts_df[null_column] = na_series

    variants = create_variants_data(scores_df, counts_df)

    for variant in variants:
        assert variant[null_column] is None


def test_create_variants_data_scores_and_counts_mismatched_lengths():
    scores_df = pd.DataFrame(
        {
            hgvs_nt_column: ["g.1A>G", "g.1A>T", "g.1A>T"],
            hgvs_splice_column: ["c.1A>G", "c.1A>T", "c.1A>T"],
            hgvs_pro_column: ["p.Met1Val", "p.Met1Leu", "p.Met1Leu"],
            required_score_column: [1.0, 2.0, 3.0],
        }
    )
    counts_df = pd.DataFrame(
        {
            hgvs_nt_column: ["g.1A>G", "g.1A>T"],
            hgvs_splice_column: ["c.1A>G", "c.1A>T"],
            hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
            "count1": [3.0, 5.0],
            "count2": [9, 10],
        }
    )

    with pytest.raises(AssertionError):
        create_variants_data(scores_df, counts_df)


def test_create_variants_seq_score_set(setup_lib_db, client, session):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    score_set = session.scalars(select(ScoreSet)).first()
    variant_data = create_variants_data(BASE_VARIANTS_SCORE_DF)

    num_variants = create_variants(
        session,
        score_set,
        variant_data,
    )

    db_variants = session.scalars(select(Variant)).all()

    assert num_variants == 2

    for db_variant, dict_variant in zip(db_variants, variant_data):
        assert db_variant.data == dict_variant["data"]
        assert db_variant.urn.split("#")[0] == score_set.urn

    session.commit()


def test_create_variants_acc_score_set(setup_lib_db, client, session):
    experiment = create_experiment(client)
    score_set = create_acc_score_set(client, experiment["urn"])
    score_set = session.scalars(select(ScoreSet)).first()
    variant_data = create_variants_data(BASE_VARIANTS_SCORE_DF)

    num_variants = create_variants(
        session,
        score_set,
        variant_data,
    )

    db_variants = session.scalars(select(Variant)).all()

    assert num_variants == 2

    for db_variant, dict_variant in zip(db_variants, variant_data):
        assert db_variant.data == dict_variant["data"]
        assert db_variant.urn.split("#")[0] == score_set.urn

    session.commit()


def test_create_null_score_range(setup_lib_db, client, session):
    experiment = create_experiment(client)
    create_seq_score_set(client, experiment["urn"])
    score_set = session.scalar(select(ScoreSet).where(ScoreSet.score_ranges.is_(None)))

    assert score_set is not None


def test_update_null_score_range(setup_lib_db, client, session):
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"], update={"scoreRanges": TEST_SAVED_SCORE_SET_RANGE})
    db_score_set = session.scalar(select(ScoreSet).where(ScoreSet.score_ranges.is_(None)))
    assert db_score_set is None

    score_set.pop("scoreRanges")
    score_set = client.put(f"/api/v1/score-sets/{score_set['urn']}", json=score_set)
    db_score_set = session.scalar(select(ScoreSet).where(ScoreSet.score_ranges.is_(None)))

    assert db_score_set is not None
