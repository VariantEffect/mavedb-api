import os
import shutil
import tempfile
from concurrent import futures
from contextlib import ExitStack
from inspect import getsourcefile
from posixpath import abspath
from typing import Any
from unittest.mock import Mock, patch

import cdot.hgvs.dataproviders
import pytest
import pytest_asyncio
from arq.worker import Worker
from biocommons.seqrepo import SeqRepo
from ga4gh.vrs.dataproxy import SeqRepoDataProxy
from httpx import AsyncClient
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, select

from mavedb.db.principal import bind_principal
from mavedb.db.session import create_engine, sessionmaker
from mavedb.deps import get_db, get_seqrepo, get_worker, hgvs_data_provider
from mavedb.lib import authentication
from mavedb.lib.gnomad import gnomad_table_name
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.collection import Collection
from mavedb.models.collection_user_association import CollectionUserAssociation
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.user_role import UserRole
from mavedb.models.role import Role
from mavedb.models.user import User
from mavedb.server_main import app
from mavedb.worker.jobs import BACKGROUND_CRONJOBS, BACKGROUND_FUNCTIONS
from tests.helpers.authentication import (
    TEST_JWT_AUDIENCE,
    AuthenticatingClient,
    Credential,
    Jwt,
    Persona,
    credential_headers,
    run_signing_key,
)
from tests.helpers.constants import (
    ADMIN_USER,
    ANONYMOUS,
    COLLECTION_MEMBER_USERS,
    CONTRIBUTOR_USER,
    MAPPER_USER,
    OUTSIDER_USER,
    TEST_SEQREPO_INITIAL_STATE,
    TEST_USER,
    VALID_CAID,
)
from tests.helpers.util.contributor import add_contributor

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


async def dummy_arq_function(ctx, *args, **kwargs) -> JobExecutionOutcome:
    return JobExecutionOutcome.succeeded()


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

    # One pool for the fixture, shut down on teardown, so jobs don't each leave worker processes behind.
    pool = futures.ProcessPoolExecutor()

    async def on_startup(ctx):
        pass

    async def on_job(ctx):
        ctx["db"] = session
        ctx["hdp"] = data_provider
        ctx["seqrepo"] = Mock(spec=SeqRepoDataProxy)  # mirrors lifecycle.on_job_start
        ctx["state"] = {}
        ctx["pool"] = pool

    worker_ = Worker(
        functions=BACKGROUND_FUNCTIONS + [dummy_arq_function],
        cron_jobs=BACKGROUND_CRONJOBS,
        redis_pool=arq_redis,
        burst=True,
        poll_delay=0,
        job_timeout=600,
        on_startup=on_startup,
        on_job_start=on_job,
    )
    # `fakeredis` does not support `INFO`
    with patch("arq.worker.log_redis_info"):
        try:
            yield worker_
        finally:
            await worker_.close()
            pool.shutdown(cancel_futures=True)


@pytest.fixture
def standalone_worker_context(data_provider, arq_redis):
    pool = futures.ProcessPoolExecutor()
    try:
        yield {
            "hdp": data_provider,
            "seqrepo": Mock(spec=SeqRepoDataProxy),  # mirrors lifecycle.on_job_start
            "state": {},
            "job_id": "test_job",
            "redis": arq_redis,
            "pool": pool,
        }
    finally:
        pool.shutdown(cancel_futures=True)


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


@pytest.fixture
def trusted_signing_key(monkeypatch):
    """Make ``get_current_user`` trust this run's signing key in place of ORCID's, and return the key.

    ``app_`` already requests it, so router tests never need it. Request it in a test that builds its own app and
    signs requests in with ``key.token_for(username)``. It patches two module constants in
    ``mavedb.lib.authentication``, and becomes a settings value once settings are read at call time.
    """
    key = run_signing_key()
    monkeypatch.setattr(authentication, "ORCID_JWT_SIGNING_PUBLIC_KEY", key.public_pem)
    monkeypatch.setattr(authentication, "ORCID_JWT_AUDIENCE", TEST_JWT_AUDIENCE)
    return key


@pytest.fixture()
def app_(session, data_provider, arq_redis, seqrepo_root_dir, trusted_signing_key):
    def override_get_db():
        try:
            yield session
        finally:
            session.close()
            # Production gives each request a new session. This one is shared, so drop the caller it was bound to,
            # or the test's own queries and the next request's first transaction would run as them.
            bind_principal(session, None)

    async def override_get_worker():
        yield arq_redis

    def override_hgvs_data_provider():
        yield data_provider

    def override_seqrepo():
        sr = _create_seqrepo(seqrepo_root_dir)
        yield sr

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_worker] = override_get_worker
    app.dependency_overrides[hgvs_data_provider] = override_hgvs_data_provider
    app.dependency_overrides[get_seqrepo] = override_seqrepo

    yield app


def _seed_user(session, user: dict) -> None:
    """Add ``user`` unless it exists. A JWT for an unknown user creates the account by calling ORCID."""
    if session.scalar(select(User).where(User.username == user["username"])) is None:
        session.add(User(**user))
        session.commit()


@pytest.fixture
def client_for(app_, session):
    """Builds a client signed in as any persona, adding the user if it doesn't exist yet.

    A relationship the test sets up, such as a contributor, comes from the fixture that creates it, like
    ``make_contributor``, so the client's name is given where it becomes true.

    ```
    reader = client_for(EXTRA_USER)
    key_holder = client_for(ADMIN_USER, via=ApiKey(role=UserRole.admin))
    ```
    """
    with ExitStack() as clients:

        def build(user: Persona, *, via: Credential = Jwt()) -> AuthenticatingClient:
            if isinstance(user, dict):
                _seed_user(session, user)
            return clients.enter_context(
                AuthenticatingClient(app=app_, base_url="http://testserver", db=session, user=user, via=via)
            )

        yield build


@pytest.fixture
def owner_client(client_for, setup_router_db):
    """Signed in as ``TEST_USER``, who creates the test's records."""
    return client_for(TEST_USER)


@pytest.fixture
def outsider_client(client_for):
    """Signed in as ``OUTSIDER_USER``, with no relation to anything the test creates."""
    return client_for(OUTSIDER_USER)


@pytest.fixture
def admin_client(client_for, setup_router_db):
    """Signed in as ``ADMIN_USER``, acting with every role they hold."""
    return client_for(ADMIN_USER)


@pytest.fixture
def mapper_client(client_for, session):
    """Signed in as ``MAPPER_USER``, who holds the mapper role, acting with it."""
    role = session.scalar(select(Role).where(Role.name == UserRole.mapper)) or Role(name=UserRole.mapper)
    session.add(User(**MAPPER_USER, role_objs=[role]))
    session.commit()
    return client_for(MAPPER_USER)


@pytest.fixture
def anonymous_client(client_for):
    """Sends no credentials."""
    return client_for(ANONYMOUS)


@pytest.fixture
def make_contributor(client_for, session):
    """Adds ``CONTRIBUTOR_USER`` as a contributor to a record, and returns a client signed in as them.

    ```
    contributor_client = make_contributor(score_set["urn"], ScoreSet)
    ```
    """

    def make(urn: str, model: Any) -> AuthenticatingClient:
        contributor_client = client_for(CONTRIBUTOR_USER)
        add_contributor(
            session,
            urn,
            model,
            CONTRIBUTOR_USER["username"],
            CONTRIBUTOR_USER["first_name"],
            CONTRIBUTOR_USER["last_name"],
        )
        return contributor_client

    return make


@pytest.fixture
def make_collection_member(client_for, session):
    """Gives a user ``role`` on a collection, and returns a client signed in as them. Each role is a different
    user, from ``COLLECTION_MEMBER_USERS``.

    ```
    viewer_client = make_collection_member(collection["urn"], ContributionRole.viewer)
    ```
    """

    def make(urn: str, role: ContributionRole) -> AuthenticatingClient:
        member = COLLECTION_MEMBER_USERS[role]
        member_client = client_for(member)
        collection = session.scalars(select(Collection).where(Collection.urn == urn)).one()
        user = session.scalars(select(User).where(User.username == member["username"])).one()
        session.add(CollectionUserAssociation(collection=collection, user=user, contribution_role=role))
        session.commit()
        return member_client

    return make


@pytest.fixture
def client(app_, session):
    """A client signed in as ``TEST_USER`` with a real JWT, for setting up data.

    Make the request under test through a persona client instead, from ``client_for`` or a fixture such as
    ``owner_client``, so the test names its actor.
    """
    with AuthenticatingClient(app=app_, base_url="http://testserver", db=session, user=TEST_USER) as tc:
        yield tc


@pytest_asyncio.fixture
async def async_client(app_, session):
    """An async client signed in as ``TEST_USER``. Pass ``credential_headers(...)`` per request to sign in as someone else."""
    headers = credential_headers(session, TEST_USER)
    async with AsyncClient(app=app_, base_url="http://testserver", headers=headers) as ac:
        yield ac


#####################################################################################################
# Athena
#####################################################################################################


@pytest.fixture
def athena_engine():
    """Create and yield a SQLAlchemy engine connected to a mock Athena database."""
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()

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
                "caid": VALID_CAID,
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
