# import os

from typing import Generator

from cdot.hgvs.dataproviders import RESTDataProvider, ChainedSeqFetcher, FastaSeqFetcher, SeqFetcher
from sqlalchemy.dialects.postgresql import JSONB

from mavedb.db.session import SessionLocal


def get_db() -> Generator:
    db = SessionLocal()
    db.current_user_id = None
    try:
        yield db
    finally:
        db.close()


def hgvs_data_provider() -> RESTDataProvider:
    # Prioritize fetching from SeqRepo, then GRCh38, then GRCh37.
    seqfetcher = ChainedSeqFetcher(SeqFetcher())

    # Attempt to resolve FASTA Seq fetchers from data files, but don't fail if neither file is
    # available. This way, we at least retain some ability to resolve sequences if we don't have
    # FASTA file access and we are able to run our test suite without needing access to large genomic
    # files.
    try:
        grch38_fetcher = FastaSeqFetcher("/data/GCF_000001405.39_GRCh38.p13_genomic.fna.gz")
        seqfetcher.seq_fetchers.append(grch38_fetcher)
    except OSError:
        pass

    try:
        grch37_fetcher = FastaSeqFetcher("/data/GCF_000001405.25_GRCh37.p13_genomic.fna.gz")
        seqfetcher.seq_fetchers.append(grch37_fetcher)
    except OSError:
        pass

    return RESTDataProvider(seqfetcher=seqfetcher)
