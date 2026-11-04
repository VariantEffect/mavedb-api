"""Library modules that must import with only the core dependencies installed (#459)."""

import subprocess
import sys


def test_library_modules_importable_without_server_dependencies():
    blocked = ("aiocache", "fastapi", "starlette", "starlette_context")
    script = "\n".join(
        [
            "import sys",
            *(f"sys.modules[{name!r}] = None" for name in blocked),
            "import mavedb.lib.annotation.annotate",
            "import mavedb.lib.csv.score_set",
            "import mavedb.lib.variant_detail",
            "import mavedb.lib.taxonomies",
        ]
    )

    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
