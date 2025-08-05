import os
from concurrent import futures
from inspect import getsourcefile
from posixpath import abspath

import cdot.hgvs.dataproviders
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from httpx import AsyncClient
from unittest.mock import patch

from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.authorization import require_current_user
from mavedb.models.user import User
from mavedb.server_main import app
from mavedb.deps import get_db, get_worker, hgvs_data_provider
from arq.worker import Worker
from mavedb.worker.jobs import (
    create_variants_for_score_set,
    map_variants_for_score_set,
    link_clingen_variants,
    submit_score_set_mappings_to_ldh,
    variant_mapper_manager,
    link_gnomad_variants,
)

from tests.helpers.constants import ADMIN_USER, EXTRA_USER, TEST_USER

####################################################################################################
# REDIS
####################################################################################################


# Defer imports of redis and arq to support cases where validation tests are called with only core dependencies installed.
@pytest_asyncio.fixture
async def arq_redis():
    """
    If the `enqueue_job` method of the ArqRedis object is not mocked and you need to run worker
    processes from within a test client, it can only be run within the `httpx.AsyncClient` object.
    The `fastapi.testclient.TestClient` object does not provide sufficient support for invocations
    of asynchronous events. Note that any tests using the worker directly should be marked as async:

    ```
    @pytest.mark.asyncio
    async def some_test_with_worker(async_client, arq_redis):
        ...
    ```

    You can mock the `enqueue_job` method with:

    ```
    from unittest.mock import patch
    def some_test(client, arq_redis):
        with patch.object(ArqRedis, "enqueue_job", return_value=None) as worker_queue:

            # Enqueue a job directly
            worker_queue.enqueue_job(some_job)

            # Hit an endpoint which enqueues a job
            client.post("/some/endpoint/that/invokes/the/worker")

            # Ensure at least one job was queued
            worker_queue.assert_called()
    ```
    """
    from arq import ArqRedis
    from fakeredis import FakeServer
    from fakeredis.aioredis import FakeConnection
    from redis.asyncio.connection import ConnectionPool

    redis_ = ArqRedis(
        connection_pool=ConnectionPool(
            server=FakeServer(),
            connection_class=FakeConnection,
        )
    )
    await redis_.flushall()
    try:
        yield redis_
    finally:
        await redis_.aclose(close_connection_pool=True)


@pytest_asyncio.fixture()
async def arq_worker(data_provider, session, arq_redis):
    """
    Run worker tasks in the test environment by including it as a fixture in a test,
    enqueueing a job on the ArqRedis object, and then running the worker. See the arq_redis
    fixture for limitations about running worker jobs from within a TestClient object.

    ```
    async def worker_test(arq_redis, arq_worker):
        await arq_redis.enqueue_job('some_job')
        await arq_worker.async_run()
        await arq_worker.run_check()
    ```
    """

    async def on_startup(ctx):
        pass

    async def on_job(ctx):
        ctx["db"] = session
        ctx["hdp"] = data_provider
        ctx["state"] = {}
        ctx["pool"] = futures.ProcessPoolExecutor()

    worker_ = Worker(
        functions=[
            create_variants_for_score_set,
            map_variants_for_score_set,
            variant_mapper_manager,
            submit_score_set_mappings_to_ldh,
            link_clingen_variants,
            link_gnomad_variants,
        ],
        redis_pool=arq_redis,
        burst=True,
        poll_delay=0,
        on_startup=on_startup,
        on_job_start=on_job,
    )
    # `fakeredis` does not support `INFO`
    with patch("arq.worker.log_redis_info"):
        try:
            yield worker_
        finally:
            await worker_.close()


@pytest.fixture
def standalone_worker_context(session, data_provider, arq_redis):
    yield {
        "db": session,
        "hdp": data_provider,
        "state": {},
        "job_id": "test_job",
        "redis": arq_redis,
        "pool": futures.ProcessPoolExecutor(),
    }


####################################################################################################
# FASTA DATA PROVIDER
####################################################################################################


@pytest.fixture
def data_provider():
    """
    To provide the transcript for the FASTA file without a network request, use:

    ```
    from helpers.utils.constants import TEST_NT_CDOT_TRANSCRIPT, TEST_PRO_CDOT_TRANSCRIPT
    from unittest.mock import patch
    import cdot.hgvs.dataproviders
    with patch.object(cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_NT_CDOT_TRANSCRIPT):
        ...
    with patch.object(cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_PRO_CDOT_TRANSCRIPT):
        ...
    ```
    """
    this_file_dir = os.path.dirname(abspath(getsourcefile(lambda: 0)))
    test_nt_fasta_file = os.path.join(this_file_dir, "helpers/data/refseq.NM_001637.3.fasta")
    test_pro_fasta_file = os.path.join(this_file_dir, "helpers/data/refseq.NP_001637.4.fasta")

    data_provider = cdot.hgvs.dataproviders.RESTDataProvider(
        seqfetcher=cdot.hgvs.dataproviders.ChainedSeqFetcher(
            cdot.hgvs.dataproviders.FastaSeqFetcher(test_nt_fasta_file),
            cdot.hgvs.dataproviders.FastaSeqFetcher(test_pro_fasta_file),
            # Include normal seqfetcher to fall back on mocked requests (or expose test shortcomings via socket connection attempts).
            cdot.hgvs.dataproviders.SeqFetcher(),
        )
    )

    yield data_provider


####################################################################################################
# FASTAPI CLIENT
####################################################################################################


@pytest.fixture()
def app_(session, data_provider, arq_redis):
    def override_get_db():
        try:
            yield session
        finally:
            session.close()

    async def override_get_worker():
        yield arq_redis

    def override_current_user():
        default_user = session.query(User).filter(User.username == TEST_USER["username"]).one_or_none()
        yield UserData(default_user, default_user.roles)

    def override_require_user():
        default_user = session.query(User).filter(User.username == TEST_USER["username"]).one_or_none()
        yield UserData(default_user, default_user.roles)

    def override_hgvs_data_provider():
        yield data_provider

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_worker] = override_get_worker
    app.dependency_overrides[get_current_user] = override_current_user
    app.dependency_overrides[require_current_user] = override_require_user
    app.dependency_overrides[hgvs_data_provider] = override_hgvs_data_provider

    yield app


@pytest.fixture()
def anonymous_app_overrides(session, data_provider, arq_redis):
    def override_get_db():
        try:
            yield session
        finally:
            session.close()

    async def override_get_worker():
        yield arq_redis

    def override_current_user():
        yield None

    def override_hgvs_data_provider():
        yield data_provider

    anonymous_overrides = {
        get_db: override_get_db,
        get_worker: override_get_worker,
        get_current_user: override_current_user,
        require_current_user: require_current_user,
        hgvs_data_provider: override_hgvs_data_provider,
    }

    yield anonymous_overrides


@pytest.fixture()
def extra_user_app_overrides(session, data_provider, arq_redis):
    def override_get_db():
        try:
            yield session
        finally:
            session.close()

    async def override_get_worker():
        yield arq_redis

    def override_current_user():
        default_user = session.query(User).filter(User.username == EXTRA_USER["username"]).one_or_none()
        yield UserData(default_user, default_user.roles)

    def override_require_user():
        default_user = session.query(User).filter(User.username == EXTRA_USER["username"]).one_or_none()
        yield UserData(default_user, default_user.roles)

    def override_hgvs_data_provider():
        yield data_provider

    anonymous_overrides = {
        get_db: override_get_db,
        get_worker: override_get_worker,
        get_current_user: override_current_user,
        require_current_user: override_require_user,
        hgvs_data_provider: override_hgvs_data_provider,
    }

    yield anonymous_overrides


@pytest.fixture()
def admin_app_overrides(session, data_provider, arq_redis):
    def override_get_db():
        try:
            yield session
        finally:
            session.close()

    async def override_get_worker():
        yield arq_redis

    def override_current_user():
        admin_user = session.query(User).filter(User.username == ADMIN_USER["username"]).one_or_none()
        yield UserData(admin_user, admin_user.roles)

    def override_require_user():
        admin_user = session.query(User).filter(User.username == ADMIN_USER["username"]).one_or_none()
        yield UserData(admin_user, admin_user.roles)

    def override_hgvs_data_provider():
        yield data_provider

    admin_overrides = {
        get_db: override_get_db,
        get_worker: override_get_worker,
        get_current_user: override_current_user,
        require_current_user: override_require_user,
        hgvs_data_provider: override_hgvs_data_provider,
    }

    yield admin_overrides


@pytest.fixture
def client(app_):
    with TestClient(app=app_, base_url="http://testserver") as tc:
        yield tc


@pytest_asyncio.fixture
async def async_client(app_):
    async with AsyncClient(app=app_, base_url="http://testserver") as ac:
        yield ac
