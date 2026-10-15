"""Structural conformance of emitted annotations, across every mapped-variant shape.

The contract asserted here is deliberately narrow and applies to every (surface, shape) pair: an
annotation function returns either an object that survives serialization and re-validation, or None.
It never raises. Content is asserted elsewhere; this file exists because content assertions on a
single variant shape cannot catch a payload-dependent structural regression.
"""

# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")
pytest.importorskip("fastapi")

from mavedb.lib.annotation.annotate import (
    variant_functional_impact_statement,
    variant_highest_level_annotation,
    variant_pathogenicity_statement,
    variant_study_result,
)
from mavedb.lib.annotation.conformance import round_trip_annotation
from tests.helpers.mocks.factories import (
    create_mock_mapped_variant,
    create_mock_mapped_variant_with_functional_calibration_score_set,
    create_mock_mapped_variant_with_pathogenicity_calibration_score_set,
)
from tests.helpers.variant_shapes import VARIANT_SHAPES, shape_ids
from tests.lib.annotation.conftest import annotation_context_for, scope_of

# Each surface is paired with the factory that can actually exercise it: a statement built on a score
# set with no calibrations returns None for every shape, which would test nothing.
ANNOTATION_SURFACES = [
    ("study_result", variant_study_result, create_mock_mapped_variant),
    (
        "functional_impact_statement",
        variant_functional_impact_statement,
        create_mock_mapped_variant_with_functional_calibration_score_set,
    ),
    (
        "pathogenicity_statement",
        variant_pathogenicity_statement,
        create_mock_mapped_variant_with_pathogenicity_calibration_score_set,
    ),
    (
        "highest_level_annotation",
        variant_highest_level_annotation,
        create_mock_mapped_variant_with_pathogenicity_calibration_score_set,
    ),
]

SURFACE_IDS = [name for name, _, _ in ANNOTATION_SURFACES]


@pytest.mark.unit
@pytest.mark.parametrize("shape", VARIANT_SHAPES, ids=shape_ids())
@pytest.mark.parametrize("surface", ANNOTATION_SURFACES, ids=SURFACE_IDS)
class TestAnnotationConformance:
    def test_annotation_round_trips_or_is_none(self, surface, shape):
        """Never raises, and anything emitted can be read back."""
        _, annotate, factory = surface

        annotation = annotate(annotation_context_for(shape.build(factory)))

        if annotation is None:
            pytest.skip(f"{shape.name} produces no annotation on this surface")
        round_trip_annotation(annotation)

    def test_calibration_scope_survives_the_round_trip(self, surface, shape):
        """`mavedb_calibration_scope` is emitted unconditionally, so a missing scope is never ambiguous
        between "public" and "produced before disclosure existed". It has to come back too."""
        _, annotate, factory = surface

        annotation = annotate(annotation_context_for(shape.build(factory)))

        if annotation is None:
            pytest.skip(f"{shape.name} produces no annotation on this surface")
        assert scope_of(round_trip_annotation(annotation)) == scope_of(annotation)


@pytest.mark.unit
def test_every_surface_emits_something_for_at_least_one_shape():
    """Guards the suite above: a surface that returned None everywhere would silently skip every case."""
    for name, annotate, factory in ANNOTATION_SURFACES:
        emitted = [
            shape.name for shape in VARIANT_SHAPES if annotate(annotation_context_for(shape.build(factory))) is not None
        ]
        assert emitted, f"{name} emitted nothing for any shape; its conformance cases are all skips"
