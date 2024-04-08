from cdot.hgvs.dataproviders import ChainedSeqFetcher, FastaSeqFetcher, RESTDataProvider

GENOMIC_FASTA_FILES = [
    "/data/GCF_000001405.39_GRCh38.p13_genomic.fna.gz",
    "/data/GCF_000001405.25_GRCh37.p13_genomic.fna.gz",
]


def seqfetcher() -> ChainedSeqFetcher:
    return ChainedSeqFetcher(*[FastaSeqFetcher(file) for file in GENOMIC_FASTA_FILES])


def cdot_rest() -> RESTDataProvider:
    return RESTDataProvider(seqfetcher=seqfetcher())
