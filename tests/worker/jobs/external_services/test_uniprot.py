# ruff: noqa: E402

from unittest.mock import patch
from uuid import uuid4

import pytest
from requests import HTTPError
from sqlalchemy import select

arq = pytest.importorskip("arq")


from mavedb.lib.uniprot.id_mapping import UniProtIDMappingAPI
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.worker.jobs import (
    poll_uniprot_mapping_jobs_for_score_set,
    submit_uniprot_mapping_jobs_for_score_set,
)
from tests.helpers.constants import (
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE,
    TEST_UNIPROT_JOB_SUBMISSION_RESPONSE,
    TEST_UNIPROT_SWISS_PROT_TYPE,
    VALID_CHR_ACCESSION,
    VALID_UNIPROT_ACCESSION,
)
from tests.helpers.util.setup.worker import (
    setup_records_files_and_variants,
    setup_records_files_and_variants_with_mapping,
)

### Test Submission


@pytest.mark.asyncio
async def test_submit_uniprot_id_mapping_success(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with patch.object(UniProtIDMappingAPI, "submit_id_mapping", return_value=TEST_UNIPROT_JOB_SUBMISSION_RESPONSE):
        result = await submit_uniprot_mapping_jobs_for_score_set(standalone_worker_context, score_set.id, uuid4().hex)

    assert result["success"]
    assert not result["retried"]
    assert result["enqueued_jobs"] is not None


@pytest.mark.asyncio
async def test_submit_uniprot_id_mapping_no_targets(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    score_set.target_genes = []
    session.add(score_set)
    session.commit()

    with patch(
        "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
    ) as mock_slack_message:
        result = await submit_uniprot_mapping_jobs_for_score_set(standalone_worker_context, score_set.id, uuid4().hex)
        mock_slack_message.assert_called_once()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_submit_uniprot_id_mapping_exception_while_spawning_jobs(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch.object(UniProtIDMappingAPI, "submit_id_mapping", side_effect=HTTPError()),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await submit_uniprot_mapping_jobs_for_score_set(standalone_worker_context, score_set.id, uuid4().hex)
        mock_slack_message.assert_called()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_submit_uniprot_id_mapping_too_many_accessions(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch(
            "mavedb.worker.jobs.external_services.uniprot.extract_ids_from_post_mapped_metadata",
            return_value=["AC1", "AC2"],
        ),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await submit_uniprot_mapping_jobs_for_score_set(standalone_worker_context, score_set.id, uuid4().hex)
        mock_slack_message.assert_called()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_submit_uniprot_id_mapping_no_accessions(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with patch(
        "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
    ) as mock_slack_message:
        result = await submit_uniprot_mapping_jobs_for_score_set(standalone_worker_context, score_set.id, uuid4().hex)
        mock_slack_message.assert_called()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_submit_uniprot_id_mapping_error_in_setup(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch("mavedb.worker.jobs.external_services.uniprot.setup_job_state", side_effect=Exception()),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await submit_uniprot_mapping_jobs_for_score_set(standalone_worker_context, score_set.id, uuid4().hex)
        mock_slack_message.assert_called()

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_submit_uniprot_id_mapping_exception_during_submission_generation(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch(
            "mavedb.worker.jobs.external_services.uniprot.extract_ids_from_post_mapped_metadata",
            side_effect=Exception(),
        ),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await submit_uniprot_mapping_jobs_for_score_set(standalone_worker_context, score_set.id, uuid4().hex)
        mock_slack_message.assert_called()

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_submit_uniprot_id_mapping_no_spawned_jobs(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch.object(UniProtIDMappingAPI, "submit_id_mapping", return_value=None),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await submit_uniprot_mapping_jobs_for_score_set(standalone_worker_context, score_set.id, uuid4().hex)
        mock_slack_message.assert_called()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_submit_uniprot_id_mapping_exception_during_enqueue(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch.object(UniProtIDMappingAPI, "submit_id_mapping", return_value=TEST_UNIPROT_JOB_SUBMISSION_RESPONSE),
        patch.object(arq.ArqRedis, "enqueue_job", side_effect=Exception()),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await submit_uniprot_mapping_jobs_for_score_set(standalone_worker_context, score_set.id, uuid4().hex)
        mock_slack_message.assert_called()

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


### Test Polling


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_success(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch.object(UniProtIDMappingAPI, "check_id_mapping_results_ready", return_value=True),
        patch.object(
            UniProtIDMappingAPI, "get_id_mapping_results", return_value=TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE
        ),
    ):
        result = await poll_uniprot_mapping_jobs_for_score_set(
            standalone_worker_context,
            {tg.id: f"job_{idx}" for idx, tg in enumerate(score_set.target_genes)},
            score_set.id,
            uuid4().hex,
        )

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    for target_gene in score_set.target_genes:
        assert target_gene.uniprot_id_from_mapped_metadata == VALID_UNIPROT_ACCESSION


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_no_targets(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    score_set.target_genes = []
    session.add(score_set)
    session.commit()

    with patch(
        "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
    ) as mock_slack_message:
        result = await poll_uniprot_mapping_jobs_for_score_set(
            standalone_worker_context,
            {tg.id: f"job_{idx}" for idx, tg in enumerate(score_set.target_genes)},
            score_set.id,
            uuid4().hex,
        )
        mock_slack_message.assert_called_once()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    for target_gene in score_set.target_genes:
        assert target_gene.uniprot_id_from_mapped_metadata is None


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_too_many_accessions(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch(
            "mavedb.worker.jobs.external_services.uniprot.extract_ids_from_post_mapped_metadata",
            return_value=["AC1", "AC2"],
        ),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await poll_uniprot_mapping_jobs_for_score_set(
            standalone_worker_context,
            {tg.id: f"job_{idx}" for idx, tg in enumerate(score_set.target_genes)},
            score_set.id,
            uuid4().hex,
        )
        mock_slack_message.assert_called()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_no_accessions(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch("mavedb.worker.jobs.external_services.uniprot.extract_ids_from_post_mapped_metadata", return_value=[]),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await poll_uniprot_mapping_jobs_for_score_set(
            standalone_worker_context,
            {tg.id: f"job_{idx}" for idx, tg in enumerate(score_set.target_genes)},
            score_set.id,
            uuid4().hex,
        )
        mock_slack_message.assert_called()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_jobs_not_ready(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch.object(UniProtIDMappingAPI, "check_id_mapping_results_ready", return_value=False),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await poll_uniprot_mapping_jobs_for_score_set(
            standalone_worker_context,
            {tg.id: f"job_{idx}" for idx, tg in enumerate(score_set.target_genes)},
            score_set.id,
            uuid4().hex,
        )
        mock_slack_message.assert_called()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    for target_gene in score_set.target_genes:
        assert target_gene.uniprot_id_from_mapped_metadata is None


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_no_jobs(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    # This case does not get sent to slack
    result = await poll_uniprot_mapping_jobs_for_score_set(
        standalone_worker_context,
        {},
        score_set.id,
        uuid4().hex,
    )

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    for target_gene in score_set.target_genes:
        assert target_gene.uniprot_id_from_mapped_metadata is None


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_no_ids_mapped(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch.object(UniProtIDMappingAPI, "check_id_mapping_results_ready", return_value=True),
        patch.object(UniProtIDMappingAPI, "get_id_mapping_results", return_value={"failedIDs": [VALID_CHR_ACCESSION]}),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await poll_uniprot_mapping_jobs_for_score_set(
            standalone_worker_context,
            {tg.id: f"job_{idx}" for idx, tg in enumerate(score_set.target_genes)},
            score_set.id,
            uuid4().hex,
        )
        mock_slack_message.assert_called()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]

    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)).one()
    for target_gene in score_set.target_genes:
        assert target_gene.uniprot_id_from_mapped_metadata is None


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_too_many_mapped_accessions(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    # Simulate a response with too many mapped IDs
    too_many_mapped_ids_response = TEST_UNIPROT_ID_MAPPING_SWISS_PROT_RESPONSE.copy()
    too_many_mapped_ids_response["results"].append(
        {"from": "AC3", "to": {"primaryAccession": "AC3", "entryType": TEST_UNIPROT_SWISS_PROT_TYPE}}
    )

    with (
        patch.object(UniProtIDMappingAPI, "check_id_mapping_results_ready", return_value=True),
        patch.object(UniProtIDMappingAPI, "get_id_mapping_results", return_value=too_many_mapped_ids_response),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await poll_uniprot_mapping_jobs_for_score_set(
            standalone_worker_context,
            {tg.id: f"job_{idx}" for idx, tg in enumerate(score_set.target_genes)},
            score_set.id,
            uuid4().hex,
        )
        mock_slack_message.assert_called()

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_error_in_setup(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch("mavedb.worker.jobs.external_services.uniprot.setup_job_state", side_effect=Exception()),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await poll_uniprot_mapping_jobs_for_score_set(
            standalone_worker_context,
            {tg.id: f"job_{idx}" for idx, tg in enumerate(score_set.target_genes)},
            score_set.id,
            uuid4().hex,
        )
        mock_slack_message.assert_called_once()

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]


@pytest.mark.asyncio
async def test_poll_uniprot_id_mapping_exception_during_polling(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with (
        patch.object(UniProtIDMappingAPI, "check_id_mapping_results_ready", side_effect=Exception()),
        patch(
            "mavedb.worker.jobs.external_services.uniprot.log_and_send_slack_message", return_value=None
        ) as mock_slack_message,
    ):
        result = await poll_uniprot_mapping_jobs_for_score_set(
            standalone_worker_context,
            {tg.id: f"job_{idx}" for idx, tg in enumerate(score_set.target_genes)},
            score_set.id,
            uuid4().hex,
        )
        mock_slack_message.assert_called_once()

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_jobs"]
