# import os
import sys
from typing import Generator

from cdot.hgvs.dataproviders import RESTDataProvider, ChainedSeqFetcher, FastaSeqFetcher, SeqFetcher
from sqlalchemy.dialects.postgresql import JSONB as POSTGRES_JSONB
from sqlalchemy.types import JSON

from mavedb.db.session import SessionLocal


def get_db() -> Generator:
    db = SessionLocal()
    db.current_user_id = None
    try:
        yield db
    finally:
        db.close()


def hgvs_data_provider() -> RESTDataProvider:
    grch38_fetcher = FastaSeqFetcher("/data/GCF_000001405.39_GRCh38.p13_genomic.fna.gz")
    grch37_fetcher = FastaSeqFetcher("/data/GCF_000001405.25_GRCh37.p13_genomic.fna.gz")

    # Prioritize fetching from SeqRepo, then GRCh38, then GRCh37.
    seqfetcher = ChainedSeqFetcher(SeqFetcher(), grch38_fetcher, grch37_fetcher)

    return RESTDataProvider(seqfetcher=seqfetcher)


# if 'PYTEST_RUN_CONFIG' in os.environ:
if "pytest" in sys.modules:
    JSONB = JSON
else:
    JSONB = POSTGRES_JSONB
