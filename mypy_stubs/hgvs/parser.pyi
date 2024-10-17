from typing import Optional

import hgvs.sequencevariant

class Parser:
    def __init__(self, grammar_fn: Optional[str] = None, export_all_rules: bool = False) -> None: ...
    def parse(self, v: str) -> hgvs.sequencevariant.SequenceVariant: ...
