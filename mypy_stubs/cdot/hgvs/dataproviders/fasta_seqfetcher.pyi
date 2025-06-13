from typing import Union

class SeqFetcher:
    def __init__(self, *args) -> None: ...

class FastaSeqFetcher:
    def __init__(self, *args, cache: bool = True) -> None: ...

class ChainedSeqFetcher:
    seq_fetchers: list[Union[SeqFetcher, FastaSeqFetcher]]
    def __init__(self, *args) -> None: ...
