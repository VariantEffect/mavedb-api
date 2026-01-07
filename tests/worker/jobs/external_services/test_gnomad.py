# ruff: noqa: E402

from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select

arq = pytest.importorskip("arq")

from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.worker.jobs import (
    link_gnomad_variants,
)
from tests.helpers.constants import (
    TEST_GNOMAD_DATA_VERSION,
    TEST_MINIMAL_SEQ_SCORESET,
    VALID_CLINGEN_CA_ID,
)
from tests.helpers.util.setup.worker import (
    setup_records_files_and_variants,
    setup_records_files_and_variants_with_mapping,
)


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_gnomad_variants_success(
    setup_worker_db,
    standalone_worker_context,
    session,
    async_client,
    data_files,
    arq_worker,
    arq_redis,
    mocked_gnomad_variant_row,
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    # We need to set the ClinGen Allele ID for the Mapped Variants, so that the gnomAD job can link them.
    mapped_variants = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    for mapped_variant in mapped_variants:
        mapped_variant.clingen_allele_id = VALID_CLINGEN_CA_ID
    session.commit()

    # Patch Athena connection with mock object which returns a mocked gnomAD variant row w/ CAID=VALID_CLINGEN_CA_ID.
    with (
        patch(
            "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
            return_value=[mocked_gnomad_variant_row],
        ),
        patch("mavedb.lib.gnomad.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION),
    ):
        result = await link_gnomad_variants(standalone_worker_context, uuid4().hex, score_set.id)

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]

    for variant in session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
    ):
        assert variant.gnomad_variants


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_gnomad_variants_exception_in_setup(
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
        "mavedb.worker.jobs.external_services.gnomad.setup_job_state",
        side_effect=Exception(),
    ):
        result = await link_gnomad_variants(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]

    for variant in session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
    ):
        assert not variant.gnomad_variants


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_gnomad_variants_no_variants_to_link(
    setup_worker_db, standalone_worker_context, session, async_client, data_files, arq_worker, arq_redis
):
    score_set = await setup_records_files_and_variants(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    result = await link_gnomad_variants(standalone_worker_context, uuid4().hex, score_set.id)

    assert result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]

    for variant in session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
    ):
        assert not variant.gnomad_variants


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_gnomad_variants_exception_while_fetching_variant_data(
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
            "mavedb.worker.jobs.external_services.gnomad.setup_job_state",
            side_effect=Exception(),
        ),
        patch("mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids", side_effect=Exception()),
    ):
        result = await link_gnomad_variants(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]

    for variant in session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
    ):
        assert not variant.gnomad_variants


@pytest.mark.asyncio
async def test_link_score_set_mappings_to_gnomad_variants_exception_while_linking_variants(
    setup_worker_db,
    standalone_worker_context,
    session,
    async_client,
    data_files,
    arq_worker,
    arq_redis,
    mocked_gnomad_variant_row,
):
    score_set = await setup_records_files_and_variants_with_mapping(
        session,
        async_client,
        data_files,
        TEST_MINIMAL_SEQ_SCORESET,
        standalone_worker_context,
    )

    # We need to set the ClinGen Allele ID for the Mapped Variants, so that the gnomAD job can link them.
    mapped_variants = session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
    ).all()

    for mapped_variant in mapped_variants:
        mapped_variant.clingen_allele_id = VALID_CLINGEN_CA_ID
    session.commit()

    with (
        patch(
            "mavedb.worker.jobs.external_services.gnomad.gnomad_variant_data_for_caids",
            return_value=[mocked_gnomad_variant_row],
        ),
        patch(
            "mavedb.worker.jobs.external_services.gnomad.link_gnomad_variants_to_mapped_variants",
            side_effect=Exception(),
        ),
    ):
        result = await link_gnomad_variants(standalone_worker_context, uuid4().hex, score_set.id)

    assert not result["success"]
    assert not result["retried"]
    assert not result["enqueued_job"]

    for variant in session.scalars(
        select(MappedVariant).join(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
    ):
        assert not variant.gnomad_variants
