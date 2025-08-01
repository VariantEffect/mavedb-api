from typing import Any, Generator, Optional

from biocommons.seqrepo.fastadir import FastaDir
from biocommons.seqrepo.seqaliasdb import SeqAliasDB

class SeqRepo:
    _root_dir: str
    _upcase: bool
    _db_path: str
    _seq_path: str
    _pending_sequences: int
    _pending_sequences_len: int
    _pending_aliases: int
    _writeable: bool
    _check_same_thread: bool
    use_sequenceproxy: bool
    sequences: FastaDir
    aliases: SeqAliasDB

    def __init__(
        self,
        root_dir: str,
        writeable: bool = False,
        upcase: bool = True,
        translate_ncbi_namespace: Optional[bool] = None,
        check_same_thread: bool = False,
        use_sequenceproxy: bool = True,
        fd_cache_size: int = 0,
    ) -> None: ...
    def __contains__(self, nsa: str) -> bool: ...
    def __getitem__(self, nsa: str) -> str: ...
    def __iter__(self) -> Generator[tuple[dict[Any, Any], Generator[dict[Any, Any], Any, None]], Any, None]: ...
    def __str__(self) -> str: ...
    def commit(self) -> None: ...
    def fetch(
        self, alias: str, start: Optional[int] = None, end: Optional[int] = None, namespace: Optional[str] = None
    ) -> str: ...
    def fetch_uri(self, uri: str, start: Optional[int] = None, end: Optional[int] = None) -> str: ...
    def store(self, seq: str, nsaliases: list[dict[str, str]]) -> None: ...
    def translate_alias(
        self,
        alias: str,
        namespace: Optional[str] = None,
        target_namespaces: Optional[list[str]] = None,
        translate_ncbi_namespace: Optional[bool] = None,
    ) -> list[str]: ...
    def translate_identifier(
        self,
        identifier: str,
        target_namespaces: Optional[list[str]] = None,
        translate_ncbi_namespace: Optional[bool] = None,
    ) -> list[str]: ...
    def _get_unique_seqid(self, alias: str, namespace: str) -> str: ...
    def _update_digest_aliases(self, seq_id: str, seq: str) -> int: ...
