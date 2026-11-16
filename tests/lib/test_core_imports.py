"""Library modules that must import with only the core dependencies installed (#459).

This is a stopgap, not the design we want. The core-only CI job already fails when one of these modules picks up a
server package, but only through the tests that import it, and a ``pytest.importorskip`` added to quiet that failure
hides it again. Blocking the packages here can't be skipped that way. The list is just the modules #459 freed; it
doesn't mean the rest of ``lib`` is clean.

The goal is a ``lib`` layered so core code never reaches a server package, with the web, worker and service clients
built on top of it. Then imports enforce the boundary themselves, and this test and the test suite's import guards
can go.
"""

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
