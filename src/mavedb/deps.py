from typing import Any, AsyncGenerator, Generator

from arq import ArqRedis, create_pool
from biocommons.seqrepo import SeqRepo
from cdot.hgvs.dataproviders import RESTDataProvider
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from mavedb.data_providers.services import cdot_rest, seqrepo
from mavedb.db.session import SessionLocal
from mavedb.lib.logging.context import save_to_logging_context
from mavedb.worker.settings import RedisWorkerSettings


def get_db() -> Generator[Session, Any, None]:
    db = SessionLocal()
    db.current_user_id = None  # type: ignore
    try:
        yield db
    finally:
        _close(db)


def _close(db: Session) -> None:
    """Close ``db``, without raising when the server already ended its session.

    A streamed response holds the request's transaction open until the last byte is sent, so a slow client can
    keep it idle past ``idle_in_transaction_session_timeout``. Postgres then ends the session and rolls the
    transaction back itself, and the rollback in ``close()`` fails on the dead connection after the client
    already has its response. Recorded in the request's canonical log as ``db_session_ended_by_server``.
    """
    try:
        db.close()
    except DBAPIError as exc:
        if not exc.connection_invalidated:
            raise
        save_to_logging_context({"db_session_ended_by_server": True})


async def get_worker() -> AsyncGenerator[ArqRedis, Any]:
    redis = await create_pool(RedisWorkerSettings)
    try:
        yield redis
    finally:
        await redis.close()


def hgvs_data_provider() -> RESTDataProvider:
    return cdot_rest()


def get_seqrepo() -> SeqRepo:
    return seqrepo()
