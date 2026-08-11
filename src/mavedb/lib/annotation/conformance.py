"""Structural conformance checks for emitted VA-Spec annotations.

An annotation that constructs successfully is not necessarily one a consumer can read back. Two
defects of exactly that shape have reached production: a required ``Extension.value`` combined with a
null baseline score stripped by ``model_dump(exclude_none=True)`` (fixed in 5c155f4d), and a
reference-identical variant whose VRS state is a ``ReferenceLengthExpression`` (fixed in 84125081).
Neither was caught by a test that only asserted the object had been built.

This module is declared here so it can be run from both the test suite and from script based conformance
checks.
"""

import json
from typing import TypeVar

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityStatement
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement

Annotation = TypeVar(
    "Annotation", ExperimentalVariantFunctionalImpactStudyResult, Statement, VariantPathogenicityStatement
)


class AnnotationRoundTripError(Exception):
    """An emitted annotation did not survive serialization and re-validation."""


def round_trip_annotation(annotation: Annotation) -> Annotation:
    """Serialize an annotation the way the API emits it, then read it back.

    Mirrors the emission path exactly — ``model_dump(exclude_none=True)`` then ``json.dumps(default=str)``
    — because the failures worth catching are the ones those two steps introduce.

    The assertion is that emission is a fixed point: the re-validated object dumps to the same JSON it
    was parsed from. Object equality would be the wrong test. VA-Spec declares container fields in terms
    of base classes — ``Statement.hasEvidenceLines`` is a ``list[EvidenceLine]`` — so a
    ``VariantPathogenicityEvidenceLine`` re-validates as a plain ``EvidenceLine``. That narrowing loses
    no data and produces byte-identical JSON, so it is not a defect this check should report.

    Returns the re-validated object.

    Raises:
        AnnotationRoundTripError: The emitted JSON did not re-validate, or re-validated to something
            that serializes differently than what was emitted.
    """
    model = type(annotation)
    emitted = json.dumps(annotation.model_dump(exclude_none=True), default=str)

    try:
        reparsed = model.model_validate(json.loads(emitted))
    except Exception as err:
        raise AnnotationRoundTripError(f"{model.__name__} did not re-validate after emission: {err}") from err

    re_emitted = json.dumps(reparsed.model_dump(exclude_none=True), default=str)
    if re_emitted != emitted:
        raise AnnotationRoundTripError(f"{model.__name__} did not survive a round trip unchanged.")

    return reparsed
