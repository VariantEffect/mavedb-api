"""
Tests for mavedb.lib.annotation.condition.

A calibration's disease condition is serialized directly from its stored MONDO term (see
mavedb.lib.mondo); an unspecified disease resolves to the generic term at write time.
"""

# ruff: noqa: E402

from types import SimpleNamespace

import pytest

pytest.importorskip("psycopg2")

from ga4gh.core.models import Coding, MappableConcept
from ga4gh.core.models import iriReference as IRI
from ga4gh.va_spec.base.domain_entities import Condition

from mavedb.lib.annotation.condition import calibration_disease_condition
from mavedb.lib.mondo import MONDO_GENERIC_CODE, MONDO_GENERIC_LABEL, MONDO_SYSTEM, mondo_iri
from mavedb.models.mondo_term import MondoTerm


def _calibration_with_disease(term: MondoTerm) -> SimpleNamespace:
    """A stand-in calibration exposing only the disease_term the condition builder reads."""
    return SimpleNamespace(disease_term=term)


@pytest.mark.unit
class TestCalibrationDiseaseConditionUnit:
    """Unit tests for the calibration disease condition."""

    def test_builds_condition_from_the_calibration_disease_term(self):
        term = MondoTerm(
            code="MONDO:0015263", system=MONDO_SYSTEM, system_version="2026-01-01", label="Brugada syndrome"
        )
        condition = calibration_disease_condition(_calibration_with_disease(term))
        assert isinstance(condition, Condition)
        assert isinstance(condition.root, MappableConcept)
        assert condition.root.conceptType == "Disease"
        assert condition.root.name == "Brugada syndrome"

    def test_primary_coding_reflects_the_stored_term(self):
        term = MondoTerm(code="MONDO:0015263", system=MONDO_SYSTEM, label="Brugada syndrome")
        coding = calibration_disease_condition(_calibration_with_disease(term)).root.primaryCoding
        assert isinstance(coding, Coding)
        assert coding.code.root == "MONDO:0015263"
        assert coding.system == MONDO_SYSTEM
        assert isinstance(coding.iris, list)
        assert len(coding.iris) == 1
        assert isinstance(coding.iris[0], IRI)
        assert coding.iris[0].root == mondo_iri("MONDO:0015263")

    def test_generic_disease_term_yields_the_mondo_root(self):
        term = MondoTerm(code=MONDO_GENERIC_CODE, system=MONDO_SYSTEM, label=MONDO_GENERIC_LABEL)
        condition = calibration_disease_condition(_calibration_with_disease(term))
        assert condition.root.primaryCoding.code.root == MONDO_GENERIC_CODE
        assert condition.root.name == MONDO_GENERIC_LABEL
