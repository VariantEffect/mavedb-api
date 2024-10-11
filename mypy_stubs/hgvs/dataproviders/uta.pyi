from typing import Any, Optional

import hgvs

class UTABase:
    pass

def connect(
    db_url: Optional[str] = None,
    pooling: bool = hgvs.global_config.uta.pooling,
    application_name: Optional[str] = None,
    mode: Any = None,
    cache: Any = None,
) -> UTABase: ...
