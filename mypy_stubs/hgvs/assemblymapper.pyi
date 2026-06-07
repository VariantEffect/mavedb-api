from typing import Any

import hgvs.sequencevariant

class AssemblyMapper:
    def __init__(
        self,
        hdp: Any,
        assembly_name: str = ...,
        alt_aln_method: str = ...,
        normalize: bool = ...,
        prevalidation_level: str | None = ...,
        in_par_assume: str = ...,
        replace_reference: bool = ...,
        *args: Any,
        **kwargs: Any,
    ) -> None: ...
    def g_to_t(self, var_g: Any, tx_ac: str) -> hgvs.sequencevariant.SequenceVariant: ...
    def c_to_g(self, var_c: Any) -> hgvs.sequencevariant.SequenceVariant: ...
    def c_to_p(self, var_c: Any, translation_table: Any = ...) -> hgvs.sequencevariant.SequenceVariant: ...
