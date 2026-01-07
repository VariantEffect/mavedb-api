import json
from asyncio.unix_events import _UnixSelectorEventLoop
from copy import deepcopy
from unittest.mock import patch
from uuid import uuid4

import cdot
import jsonschema
from sqlalchemy import select

from mavedb.lib.score_sets import csv_data_to_df
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.view_models.experiment import Experiment, ExperimentCreate
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from mavedb.worker.jobs import (
    create_variants_for_score_set,
    map_variants_for_score_set,
)
from tests.helpers.constants import (
    TEST_ACC_SCORESET_VARIANT_MAPPING_SCAFFOLD,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MULTI_TARGET_SCORESET_VARIANT_MAPPING_SCAFFOLD,
    TEST_NT_CDOT_TRANSCRIPT,
    TEST_SEQ_SCORESET_VARIANT_MAPPING_SCAFFOLD,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
)
from tests.helpers.util.mapping import sanitize_mapping_queue


async def setup_records_and_files(async_client, data_files, input_score_set):
    experiment_payload = deepcopy(TEST_MINIMAL_EXPERIMENT)
    jsonschema.validate(instance=experiment_payload, schema=ExperimentCreate.model_json_schema())
    experiment_response = await async_client.post("/api/v1/experiments/", json=experiment_payload)
    assert experiment_response.status_code == 200
    experiment = experiment_response.json()
    jsonschema.validate(instance=experiment, schema=Experiment.model_json_schema())

    score_set_payload = deepcopy(input_score_set)
    score_set_payload["experimentUrn"] = experiment["urn"]
    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.model_json_schema())
    score_set_response = await async_client.post("/api/v1/score-sets/", json=score_set_payload)
    assert score_set_response.status_code == 200
    score_set = score_set_response.json()
    jsonschema.validate(instance=score_set, schema=ScoreSet.model_json_schema())

    scores_fp = (
        "scores_multi_target.csv"
        if len(score_set["targetGenes"]) > 1
        else ("scores.csv" if "targetSequence" in score_set["targetGenes"][0] else "scores_acc.csv")
    )
    counts_fp = (
        "counts_multi_target.csv"
        if len(score_set["targetGenes"]) > 1
        else ("counts.csv" if "targetSequence" in score_set["targetGenes"][0] else "counts_acc.csv")
    )
    with (
        open(data_files / scores_fp, "rb") as score_file,
        open(data_files / counts_fp, "rb") as count_file,
        open(data_files / "score_columns_metadata.json", "rb") as score_columns_file,
        open(data_files / "count_columns_metadata.json", "rb") as count_columns_file,
    ):
        scores = csv_data_to_df(score_file)
        counts = csv_data_to_df(count_file)
        score_columns_metadata = json.load(score_columns_file)
        count_columns_metadata = json.load(count_columns_file)

    return score_set["urn"], scores, counts, score_columns_metadata, count_columns_metadata


async def setup_records_files_and_variants(session, async_client, data_files, input_score_set, worker_ctx):
    score_set_urn, scores, counts, score_columns_metadata, count_columns_metadata = await setup_records_and_files(
        async_client, data_files, input_score_set
    )
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    # Patch CDOT `_get_transcript`, in the event this function is called on an accesssion based scoreset.
    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider,
        "_get_transcript",
        return_value=TEST_NT_CDOT_TRANSCRIPT,
    ):
        result = await create_variants_for_score_set(
            worker_ctx, uuid4().hex, score_set.id, 1, scores, counts, score_columns_metadata, count_columns_metadata
        )

    score_set_with_variants = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()

    assert result["success"]
    assert score_set.processing_state is ProcessingState.success
    assert score_set_with_variants.num_variants == 3

    return score_set_with_variants


async def setup_records_files_and_variants_with_mapping(
    session, async_client, data_files, input_score_set, standalone_worker_context
):
    score_set = await setup_records_files_and_variants(
        session, async_client, data_files, input_score_set, standalone_worker_context
    )
    await sanitize_mapping_queue(standalone_worker_context, score_set)

    async def dummy_mapping_job():
        return await setup_mapping_output(async_client, session, score_set)

    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_mapping_job(),
        ),
        patch("mavedb.worker.jobs.variant_processing.mapping.CLIN_GEN_SUBMISSION_ENABLED", False),
    ):
        result = await map_variants_for_score_set(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]
    return session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()


async def setup_mapping_output(
    async_client, session, score_set, score_set_is_seq_based=True, score_set_is_multi_target=False, empty=False
):
    score_set_response = await async_client.get(f"/api/v1/score-sets/{score_set.urn}")

    if score_set_is_seq_based:
        if score_set_is_multi_target:
            # If this is a multi-target sequence based score set, use the scaffold for that.
            mapping_output = deepcopy(TEST_MULTI_TARGET_SCORESET_VARIANT_MAPPING_SCAFFOLD)
        else:
            mapping_output = deepcopy(TEST_SEQ_SCORESET_VARIANT_MAPPING_SCAFFOLD)
    else:
        # there is not currently a multi-target accession-based score set test
        mapping_output = deepcopy(TEST_ACC_SCORESET_VARIANT_MAPPING_SCAFFOLD)
    mapping_output["metadata"] = score_set_response.json()

    if empty:
        return mapping_output

    variants = session.scalars(select(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).all()
    for variant in variants:
        mapped_score = {
            "pre_mapped": TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
            "post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
            "mavedb_id": variant.urn,
        }

        mapping_output["mapped_scores"].append(mapped_score)

    return mapping_output
