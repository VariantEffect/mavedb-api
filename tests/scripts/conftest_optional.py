"""Fixtures for tests/scripts that depend on the `server` extras (fastapi, requests).

Split out of conftest.py so the rest of the fixture suite stays importable on core dependencies alone —
see tests/worker/conftest_optional.py for the same pattern.
"""

import pytest

from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.score_calibrations import variants_for_functional_classification
from mavedb.models.acmg_classification import ACMGClassification
from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.enums.functional_classification import FunctionalClassification
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification


@pytest.fixture
def anonymous_principal():
    """The principal the dump is built for.

    The archive has no requesting user, so every artifact is composed for the public. Constructing this
    explicitly in tests keeps that from being an accident of a default argument.
    """
    return Principal()


@pytest.fixture
def anonymous_viewer(anonymous_principal):
    """The calibration viewer the dump's artifacts are scoped to."""
    return anonymous_principal.viewer_for(ScoreCalibrationViewer)


@pytest.fixture
def make_dump_calibration(session, sample_user, dump_acmg_classifications):
    """Factory for a calibration with an abnormal (PS3) range below -1.0 and a normal (BS3) range above 1.0.

    `private` and `research_use_only` are the two axes the dump treats differently: the first decides
    whether an anonymous viewer may read it at all, the second only how it is labeled.

    Range membership is materialized from each variant's score by the same helper the creation endpoint
    uses, so a score of -2.0 lands in the abnormal range and one of 3.0 in the normal range. Membership is
    stored as an association rather than recomputed at read time, so a fixture that sets the bounds
    without populating it would classify every variant as `indeterminate` no matter its score, and every
    assertion about a calibration column would silently hold in that one degenerate state.

    Call this only after the score set's variants exist; a variant added later is not classified.
    """
    counter = {"n": 0}

    def _make(score_set, *, private=False, research_use_only=False, title=None):
        counter["n"] += 1
        urn = f"urn:mavedb:calibration-{counter['n']:08d}-0000-0000-0000-000000000000"

        calibration = ScoreCalibration(
            score_set_id=score_set.id,
            urn=urn,
            title=title or f"Dump Calibration {counter['n']}",
            baseline_score=0.0,
            research_use_only=research_use_only,
            # The view models reject a primary calibration that is private or research-use-only, so
            # `primary` is derived rather than exposed: a fixture that set it independently could build a
            # score set the public-dump view models refuse to validate.
            primary=not (private or research_use_only),
            private=private,
            calibration_metadata={},
            created_by_id=sample_user.id,
            modified_by_id=sample_user.id,
        )
        session.add(calibration)
        session.commit()
        session.refresh(calibration)

        abnormal = session.query(ACMGClassification).filter(ACMGClassification.criterion == ACMGCriterion.PS3).first()
        normal = session.query(ACMGClassification).filter(ACMGClassification.criterion == ACMGCriterion.BS3).first()

        ranges = [
            ScoreCalibrationFunctionalClassification(
                calibration=calibration,
                label="abnormal range",
                description="An abnormal functional range",
                functional_classification=FunctionalClassification.abnormal,
                range=[-5.0, -1.0],
                inclusive_lower_bound=True,
                inclusive_upper_bound=False,
                acmg_classification_id=abnormal.id,
            ),
            ScoreCalibrationFunctionalClassification(
                calibration=calibration,
                label="normal range",
                description="A normal functional range",
                functional_classification=FunctionalClassification.normal,
                range=[1.0, 5.0],
                inclusive_lower_bound=True,
                inclusive_upper_bound=False,
                acmg_classification_id=normal.id,
            ),
        ]
        for functional_range in ranges:
            session.add(functional_range)
        session.commit()

        for functional_range in ranges:
            functional_range.variants = variants_for_functional_classification(session, functional_range, use_sql=True)
        session.commit()
        session.refresh(calibration)
        return calibration

    return _make
