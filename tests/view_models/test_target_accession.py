import pytest

from mavedb.view_models.target_accession import TargetAccessionCreate
from tests.helpers.constants import VALID_ACCESSION


def test_can_create_target_accession_with_assembly():
    assembly = "Test Assembly"
    target_accession = TargetAccessionCreate(
        accession=VALID_ACCESSION,
        assembly=assembly,
        gene=None,
    )

    assert target_accession.accession == VALID_ACCESSION
    assert target_accession.assembly == assembly
    assert not target_accession.gene


def test_can_create_target_accession_with_gene():
    gene = "Test Gene"
    target_accession = TargetAccessionCreate(
        accession=VALID_ACCESSION,
        assembly=None,
        gene=gene,
    )

    assert target_accession.accession == VALID_ACCESSION
    assert not target_accession.assembly
    assert target_accession.gene == gene


def test_cannot_create_target_accession_without_assembly_or_gene():
    with pytest.raises(ValueError) as exc_info:
        TargetAccessionCreate(
            accession=VALID_ACCESSION,
            assembly=None,
            gene=None,
        )

    assert "Could not create TargetAccessionCreate object: Either a `gene` or `assembly` is required" in str(
        exc_info.value
    )
