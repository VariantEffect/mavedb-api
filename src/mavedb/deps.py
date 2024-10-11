# import os

from typing import Any, AsyncGenerator, Generator

from arq import ArqRedis, create_pool
from cdot.hgvs.dataproviders import RESTDataProvider
from sqlalchemy.orm import Session

from mavedb.data_providers.services import cdot_rest
from mavedb.db.session import SessionLocal
from mavedb.worker.settings import RedisWorkerSettings


def get_db() -> Generator[Session, Any, None]:
    db = SessionLocal()
    db.current_user_id = None  # type: ignore
    try:
        yield db
    finally:
        db.close()


async def get_worker() -> AsyncGenerator[ArqRedis, Any]:
    redis = await create_pool(RedisWorkerSettings)
    try:
        yield redis
    finally:
        await redis.close()


def hgvs_data_provider() -> RESTDataProvider:
    return cdot_rest()
