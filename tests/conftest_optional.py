import os
import shutil
import tempfile
from concurrent import futures
from inspect import getsourcefile
from posixpath import abspath
from unittest.mock import patch

import cdot.hgvs.dataproviders
import pytest
import pytest_asyncio
from arq.worker import Worker
from biocommons.seqrepo import SeqRepo
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy import Column, Float, Integer, MetaData, String, Table

from mavedb.db.session import create_engine, sessionmaker
from mavedb.deps import get_db, get_seqrepo, get_worker, hgvs_data_provider
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.authorization import require_current_user
from mavedb.lib.gnomad import gnomad_table_name
from mavedb.models.user import User
from mavedb.server_main import app
from mavedb.worker.jobs import BACKGROUND_CRONJOBS, BACKGROUND_FUNCTIONS
from mavedb.worker.lib.managers.types import JobResultData
from tests.helpers.constants import ADMIN_USER, EXTRA_USER, TEST_SEQREPO_INITIAL_STATE, TEST_USER

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


async def dummy_arq_function(ctx, *args, **kwargs) -> JobResultData:
    return {"status": "ok", "data": {}, "exception_details": None}


@pytest_asyncio.fixture()
async def arq_worker(data_provider, session, arq_redis):
    """
    Run worker tasks in the test environment by including it as a fixture in a test,
    enqueueing a job on the ArqRedis object, and then running the worker. See the arq_redis
    fixture for limitations about running worker jobs from within a TestClient object.

    ```
    async def worker_test(arq_redis, arq_worker):
        await arq_redis.enqueue_job('dummy_arq_function')
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
        functions=BACKGROUND_FUNCTIONS + [dummy_arq_function],
        cron_jobs=BACKGROUND_CRONJOBS,
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
def standalone_worker_context(data_provider, arq_redis):
    yield {
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


#####################################################################################################
# SEQREPO
#####################################################################################################


@pytest.fixture()
def seqrepo_root_dir():
    """
    Provides the root directory for the SeqRepo instance.
    """
    tmpdir = tempfile.mkdtemp(prefix="seqrepo_pytest_")
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)


def _create_seqrepo(root_dir: str) -> SeqRepo:
    """
    Provides a SeqRepo instance for testing purposes. The root of this directory is
    temporary and will be deleted after the test completes.

    Note that running these tests requires htslib on OS X, which can be installed via Homebrew:
    ```
    brew install htslib
    ```
    or tabix on Ubuntu:
    ```
    sudo apt install -y tabix
    ```

    This internal helper function is necessary to ensure that when this SeqRepo instance is used in tests
    via FastAPI, it is created from within the FastAPI application. When the SeqRepo instance is created
    as a fixture, it is always created in a different thread from the FastAPI application prior to the
    initialization of the client. If created in a different thread, it will raise an error when trying to
    access the SeqRepo instance due to SQLite's threading limitations. Using SeqRepo in this manner will
    still generate warnings from SQLite about thread safety, but it will not raise an error. As long as the
    input to this method is the root directory *provided as a fixture*, the SeqRepo instance will be torn down
    after the test completes.

    Note that although `check_same_threads` is exposed as a parameter, it will always be set to `True` in the
    `SeqRepo` constructor if the object is writeable.
    """
    sr = SeqRepo(
        root_dir=root_dir,
        writeable=True,
    )

    for entry in TEST_SEQREPO_INITIAL_STATE:
        state = list(entry.values())[0]
        sr.sequences.store(state["seq_id"], state["seq"])
        sr.aliases.store_alias(state["seq_id"], state["namespace"], state["alias"])

    sr.sequences.commit()
    sr.aliases.commit()

    assert len(sr.sequences) == len(TEST_SEQREPO_INITIAL_STATE)
    assert sr.aliases.stats()["n_sequences"] == len(TEST_SEQREPO_INITIAL_STATE)

    return sr


@pytest.fixture()
def seqrepo(seqrepo_root_dir):
    """
    Provides a SeqRepo instance as a fixture.
    """
    sr = _create_seqrepo(seqrepo_root_dir)
    try:
        yield sr
    finally:
        del sr


####################################################################################################
# FASTAPI CLIENT
####################################################################################################


@pytest.fixture()
def app_(session, data_provider, arq_redis, seqrepo_root_dir):
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

    def override_seqrepo():
        sr = _create_seqrepo(seqrepo_root_dir)
        yield sr

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_worker] = override_get_worker
    app.dependency_overrides[get_current_user] = override_current_user
    app.dependency_overrides[require_current_user] = override_require_user
    app.dependency_overrides[hgvs_data_provider] = override_hgvs_data_provider
    app.dependency_overrides[get_seqrepo] = override_seqrepo

    yield app


@pytest.fixture()
def anonymous_app_overrides(session, data_provider, arq_redis, seqrepo_root_dir):
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

    def override_seqrepo():
        sr = _create_seqrepo(seqrepo_root_dir)
        yield sr

    anonymous_overrides = {
        get_db: override_get_db,
        get_worker: override_get_worker,
        get_current_user: override_current_user,
        require_current_user: require_current_user,
        hgvs_data_provider: override_hgvs_data_provider,
        get_seqrepo: override_seqrepo,
    }

    yield anonymous_overrides


@pytest.fixture()
def extra_user_app_overrides(session, data_provider, arq_redis, seqrepo_root_dir):
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

    def override_seqrepo():
        sr = _create_seqrepo(seqrepo_root_dir)
        yield sr

    anonymous_overrides = {
        get_db: override_get_db,
        get_worker: override_get_worker,
        get_current_user: override_current_user,
        require_current_user: override_require_user,
        hgvs_data_provider: override_hgvs_data_provider,
        get_seqrepo: override_seqrepo,
    }

    yield anonymous_overrides


@pytest.fixture()
def admin_app_overrides(session, data_provider, arq_redis, seqrepo_root_dir):
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

    def override_seqrepo():
        sr = _create_seqrepo(seqrepo_root_dir)
        yield sr

    admin_overrides = {
        get_db: override_get_db,
        get_worker: override_get_worker,
        get_current_user: override_current_user,
        require_current_user: override_require_user,
        hgvs_data_provider: override_hgvs_data_provider,
        get_seqrepo: override_seqrepo,
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


#####################################################################################################
# Athena
#####################################################################################################


@pytest.fixture
def athena_engine():
    """Create and yield a SQLAlchemy engine connected to a mock Athena database."""
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()

    # TODO: Define your table schema here
    my_table = Table(
        gnomad_table_name(),
        metadata,
        Column("id", Integer, primary_key=True),
        Column("locus.contig", String),
        Column("locus.position", Integer),
        Column("alleles", String),
        Column("caid", String),
        Column("joint.freq.all.ac", Integer),
        Column("joint.freq.all.an", Integer),
        Column("joint.fafmax.faf95_max_gen_anc", String),
        Column("joint.fafmax.faf95_max", Float),
    )
    metadata.create_all(engine)

    session = sessionmaker(autocommit=False, autoflush=False, bind=engine)()

    # Insert test data
    session.execute(
        my_table.insert(),
        [
            {
                "id": 1,
                "locus.contig": "chr1",
                "locus.position": 12345,
                "alleles": "[G, A]",
                "caid": "CA123",
                "joint.freq.all.ac": 23,
                "joint.freq.all.an": 32432423,
                "joint.fafmax.faf95_max_gen_anc": "anc1",
                "joint.fafmax.faf95_max": 0.000006763700000000002,
            }
        ],
    )
    session.commit()
    session.close()

    try:
        yield engine
    finally:
        engine.dispose()
