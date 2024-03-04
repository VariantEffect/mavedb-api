# import os

from typing import AsyncGenerator, Generator

from arq import create_pool
from cdot.hgvs.dataproviders import RESTDataProvider, ChainedSeqFetcher, FastaSeqFetcher
from sqlalchemy.dialects.postgresql import JSONB

from mavedb.db.session import SessionLocal
from mavedb.worker.settings import RedisWorker


def get_db() -> Generator:
    db = SessionLocal()
    db.current_user_id = None  # type: ignore
    try:
        yield db
    finally:
        db.close()


async def get_worker() -> AsyncGenerator:
    queue = await create_pool(RedisWorker)
    try:
        yield queue
    finally:
        await queue.close()


def hgvs_data_provider() -> RESTDataProvider:
    # Prioritize fetching from GRCh38, then GRCh37.
    seqfetcher = ChainedSeqFetcher(
        FastaSeqFetcher("/data/GCF_000001405.39_GRCh38.p13_genomic.fna.gz"),
        FastaSeqFetcher("/data/GCF_000001405.25_GRCh37.p13_genomic.fna.gz"),
    )
    return RESTDataProvider(seqfetcher=seqfetcher)
