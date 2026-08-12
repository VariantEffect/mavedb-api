# ruff: noqa: E402

"""Tests for the Principal.

A principal is what gets threaded through a fan-out read, so these cases cover both halves of its job:
handing out the right viewer for a caller, and never becoming shared state between callers.
"""

import pytest

pytest.importorskip("fastapi", reason="Skipping permissions tests; FastAPI is required but not installed.")

import importlib
import inspect
import pkgutil

import mavedb
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.permissions.viewer import Viewer
from tests.lib.permissions.conftest import EntityTestHelper


class TestPrincipal:
    def test_a_viewer_is_built_once_and_reused(self) -> None:
        # This is what makes threading one Principal cheaper than constructing a viewer per record: the
        # viewer's own READ memoization only pays off if the viewer itself survives.
        principal = Principal()

        assert principal.viewer_for(ScoreCalibrationViewer) is principal.viewer_for(ScoreCalibrationViewer)

    def test_a_viewer_inherits_the_principals_caller(self) -> None:
        admin = EntityTestHelper.create_user_data("admin")
        calibration = EntityTestHelper.create_score_calibration(entity_state="private")

        assert Principal(admin).viewer_for(ScoreCalibrationViewer).may_read(calibration) is True
        assert Principal().viewer_for(ScoreCalibrationViewer).may_read(calibration) is False

    def test_distinct_principals_do_not_share_viewers(self) -> None:
        # Two principals in flight at once must not be able to answer for each other.
        anonymous, admin = Principal(), Principal(EntityTestHelper.create_user_data("admin"))

        assert anonymous.viewer_for(ScoreCalibrationViewer) is not admin.viewer_for(ScoreCalibrationViewer)

    def test_an_anonymous_principal_is_the_default(self) -> None:
        assert Principal().user_data is None


class TestNoSharedPrincipalOrViewerDefaults:
    """No function may default a parameter to an Principal or Viewer instance.

    Python evaluates default arguments once at import, so such an instance — and its permission caches —
    would be shared by every request for the life of the process. A calibration published mid-process would
    keep its stale verdict, and two callers could be answered from one another's cache. The correct shape is
    ``Optional[Principal] = None``, building one when it is missing.
    """

    def test_no_module_defaults_a_parameter_to_a_live_principal_or_viewer(self) -> None:
        offenders = []

        for module_info in pkgutil.walk_packages(mavedb.__path__, prefix="mavedb."):
            try:
                module = importlib.import_module(module_info.name)
            except Exception:  # optional extras (arq, cdot) are not installed in every environment
                continue

            for name, function in inspect.getmembers(module, inspect.isfunction):
                if inspect.getmodule(function) is not module:
                    continue
                for parameter in inspect.signature(function).parameters.values():
                    if isinstance(parameter.default, (Principal, Viewer)):
                        offenders.append(f"{module_info.name}.{name}({parameter.name}=...)")

        assert offenders == [], (
            "These defaults would be shared across every request for the life of the process; "
            f"take Optional[...] = None instead: {offenders}"
        )
