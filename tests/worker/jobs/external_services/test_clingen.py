# ruff: noqa: E402

from asyncio.unix_events import _UnixSelectorEventLoop
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select

arq = pytest.importorskip("arq")

from mavedb.lib.clingen.services import (
    ClinGenAlleleRegistryService,
    ClinGenLdhService,
    clingen_allele_id_from_ldh_variation,
)
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.worker.jobs import (
    link_clingen_variants,
    submit_score_set_mappings_to_car,
    submit_score_set_mappings_to_ldh,
)
from tests.helpers.constants import (
    TEST_CLINGEN_ALLELE_OBJECT,
    TEST_CLINGEN_LDH_LINKING_RESPONSE,
    TEST_CLINGEN_SUBMISSION_BAD_RESQUEST_RESPONSE,
    TEST_CLINGEN_SUBMISSION_RESPONSE,
    TEST_CLINGEN_SUBMISSION_UNAUTHORIZED_RESPONSE,
    TEST_MINIMAL_SEQ_SCORESET,
)
from tests.helpers.util.exceptions import awaitable_exception
from tests.helpers.util.setup.worker import (
    setup_records_files_and_variants,
    setup_records_files_and_variants_with_mapping,
)

############################################################################################################################################
# ClinGen CAR Submission
############################################################################################################################################


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_car_success(
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
        patch.object(ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[TEST_CLINGEN_ALLELE_OBJECT]),
        patch(
            "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
            "https://reg.test.genome.network/pytest",
        ),
    ):
        result = await submit_score_set_mappings_to_car(standalone_worker_context, uuid4().hex, score_set.id)

    mapped_variants_with_caid_for_score_set = session.scalars(
        select(MappedVariant)
        .join(Variant)
        .join(ScoreSetDbModel)
        .filter(ScoreSetDbModel.urn == score_set.urn, MappedVariant.clingen_allele_id.is_not(None))
    ).all()

    assert len(mapped_variants_with_caid_for_score_set) == score_set.num_variants

    assert result["success"]
    assert not result["retried"]
    assert result["enqueued_job"] is not None


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_car_exception_in_setup(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with patch(
        "mavedb.worker.jobs.external_services.clingen.setup_job_state",
        side_effect=Exception(),
    ):
        result = await submit_score_set_mappings_to_car(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_car_no_variants_exist(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    result = await submit_score_set_mappings_to_car(standalone_worker_context, uuid4().hex, score_set.id)

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_car_exception_in_hgvs_dict_creation(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with patch(
        "mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped",
        side_effect=Exception(),
    ):
        result = await submit_score_set_mappings_to_car(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_car_exception_during_submission(
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
        patch.object(ClinGenAlleleRegistryService, "dispatch_submissions", side_effect=Exception()),
        patch(
            "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
            "https://reg.test.genome.network/pytest",
        ),
    ):
        result = await submit_score_set_mappings_to_car(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_car_exception_in_allele_association(
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
        patch("mavedb.worker.jobs.external_services.clingen.get_allele_registry_associations", side_effect=Exception()),
        patch(
            "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
            "https://reg.test.genome.network/pytest",
        ),
    ):
        result = await submit_score_set_mappings_to_car(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_car_exception_during_ldh_enqueue(
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
            "mavedb.worker.jobs.external_services.clingen.CAR_SUBMISSION_ENDPOINT",
            "https://reg.test.genome.network/pytest",
        ),
        patch.object(ClinGenAlleleRegistryService, "dispatch_submissions", return_value=[TEST_CLINGEN_ALLELE_OBJECT]),
        patch.object(arq.ArqRedis, "enqueue_job", side_effect=Exception()),
    ):
        result = await submit_score_set_mappings_to_car(standalone_worker_context, uuid4().hex, score_set.id)

    mapped_variants_with_caid_for_score_set = session.scalars(
        select(MappedVariant)
        .join(Variant)
        .join(ScoreSetDbModel)
        .filter(ScoreSetDbModel.urn == score_set.urn, MappedVariant.clingen_allele_id.is_not(None))
    ).all()

    assert len(mapped_variants_with_caid_for_score_set) == score_set.num_variants

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


############################################################################################################################################
# ClinGen LDH Submission
############################################################################################################################################


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_ldh_success(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_submission_job():
        return [TEST_CLINGEN_SUBMISSION_RESPONSE, None]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_submission_job(),
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert result["success"]
    assert not result["retried"]
    assert result["enqueued_job"] is not None


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_ldh_exception_in_setup(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with patch(
        "mavedb.worker.jobs.external_services.clingen.setup_job_state",
        side_effect=Exception(),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_ldh_exception_in_auth(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with patch.object(
        ClinGenLdhService,
        "_existing_jwt",
        side_effect=Exception(),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_ldh_no_variants_exist(
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
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_ldh_exception_in_hgvs_generation(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with patch(
        "mavedb.worker.jobs.external_services.clingen.get_hgvs_from_post_mapped",
        side_effect=Exception(),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_ldh_exception_in_ldh_submission_construction(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with patch(
        "mavedb.lib.clingen.content_constructors.construct_ldh_submission",
        side_effect=Exception(),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_ldh_exception_during_submission(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def failed_submission_job():
        return Exception()

    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            side_effect=failed_submission_job(),
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_response", [TEST_CLINGEN_SUBMISSION_BAD_RESQUEST_RESPONSE, TEST_CLINGEN_SUBMISSION_UNAUTHORIZED_RESPONSE]
)
async def test_submit_score_set_mappings_to_ldh_submission_failures_exist(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis, error_response
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_submission_job():
        return [None, error_response]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_submission_job(),
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_ldh_exception_during_linking_enqueue(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_submission_job():
        return [TEST_CLINGEN_SUBMISSION_RESPONSE, None]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_submission_job(),
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
        patch.object(arq.ArqRedis, "enqueue_job", side_effect=Exception()),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_submit_score_set_mappings_to_ldh_linking_not_queued_when_expected(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_submission_job():
        return [TEST_CLINGEN_SUBMISSION_RESPONSE, None]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_submission_job(),
        ),
        patch.object(ClinGenLdhService, "_existing_jwt", return_value="test_jwt"),
        patch.object(arq.ArqRedis, "enqueue_job", return_value=None),
    ):
        result = await submit_score_set_mappings_to_ldh(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


##############################################################################################################################################
## ClinGen Linkage
##############################################################################################################################################


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_success(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_linking_job():
        return [
            (variant_urn, TEST_CLINGEN_LDH_LINKING_RESPONSE)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        return_value=dummy_linking_job(),
    ):
        result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert result["success"]
    assert not result["retried"]
    assert result["enqueued_job"]

    for variant in session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
    ):
        assert variant.clingen_allele_id == clingen_allele_id_from_ldh_variation(TEST_CLINGEN_LDH_LINKING_RESPONSE)


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_exception_in_setup(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    with patch(
        "mavedb.worker.jobs.external_services.clingen.setup_job_state",
        side_effect=Exception(),
    ):
        result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]

    for variant in session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
    ):
        assert variant.clingen_allele_id is None


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_no_variants_to_link(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_exception_during_linkage(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with patch.object(
        _UnixSelectorEventLoop,
        "run_in_executor",
        side_effect=Exception(),
    ):
        result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_exception_while_parsing_linkages(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_linking_job():
        return [
            (variant_urn, TEST_CLINGEN_LDH_LINKING_RESPONSE)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_linking_job(),
        ),
        patch(
            "mavedb.worker.jobs.external_services.clingen.clingen_allele_id_from_ldh_variation",
            side_effect=Exception(),
        ),
    ):
        result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_failures_exist_but_do_not_eclipse_retry_threshold(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_linking_job():
        return [
            (variant_urn, None)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_linking_job(),
        ),
        patch(
            "mavedb.worker.jobs.external_services.clingen.LINKED_DATA_RETRY_THRESHOLD",
            2,
        ),
    ):
        result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert result["success"]
    assert not result["retried"]
    assert result["enqueued_job"]


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_failures_exist_and_eclipse_retry_threshold(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_linking_job():
        return [
            (variant_urn, None)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_linking_job(),
        ),
        patch(
            "mavedb.worker.jobs.external_services.clingen.LINKED_DATA_RETRY_THRESHOLD",
            1,
        ),
        patch(
            "mavedb.worker.jobs.external_services.clingen.LINKING_BACKOFF_IN_SECONDS",
            0,
        ),
    ):
        result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert not result["success"]
    assert result["retried"]
    assert result["enqueued_job"]


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_failures_exist_and_eclipse_retry_threshold_cant_enqueue(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_linking_job():
        return [
            (variant_urn, None)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_linking_job(),
        ),
        patch(
            "mavedb.worker.jobs.external_services.clingen.LINKED_DATA_RETRY_THRESHOLD",
            1,
        ),
        patch.object(arq.ArqRedis, "enqueue_job", return_value=awaitable_exception()),
    ):
        result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_failures_exist_and_eclipse_retry_threshold_retries_exceeded(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_linking_job():
        return [
            (variant_urn, None)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_linking_job(),
        ),
        patch(
            "mavedb.worker.jobs.external_services.clingen.LINKED_DATA_RETRY_THRESHOLD",
            1,
        ),
        patch(
            "mavedb.worker.jobs.external_services.clingen.LINKING_BACKOFF_IN_SECONDS",
            0,
        ),
        patch(
            "mavedb.worker.jobs.utils.retry.ENQUEUE_BACKOFF_ATTEMPT_LIMIT",
            1,
        ),
    ):
        result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 2)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_ldh_objects_error_in_gnomad_job_enqueue(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    async def dummy_linking_job():
        return [
            (variant_urn, TEST_CLINGEN_LDH_LINKING_RESPONSE)
            for variant_urn in session.scalars(
                select(Variant.urn).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
            ).all()
        ]

    # We are unable to mock requests via requests_mock that occur inside another event loop. Instead, patch the return
    # value of the EventLoop itself, which would have made the request.
    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_linking_job(),
        ),
        patch.object(arq.ArqRedis, "enqueue_job", return_value=awaitable_exception()),
    ):
        result = await link_clingen_variants(standalone_worker_context, uuid4().hex, score_set.id, 1)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]
