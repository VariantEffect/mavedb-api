from typing import Optional, Union, Tuple, List, Sequence, Mapping, Any
from .position import VariantPosition


class Variant:

    def __init__(
        self,
        s: Union[str, Mapping[str, Any], Sequence[Mapping[str, Any]]],
        targetseq: Optional[str] = None,
        relaxed_ordering: bool = False
    ): ...

    positions: Optional[
        Union[
            VariantPosition,
            Tuple[VariantPosition, VariantPosition],
            List[Union[VariantPosition, Tuple[VariantPosition, VariantPosition]]],
        ]
    ]

    prefix: str

    sequence: Union[
        str,
        Tuple[str, str],
        List[Optional[Union[str, Tuple[str, str]]]],
        None
    ]
