import pytest  # noqa: F401

from mavedb.lib.mapping import extract_ids_from_post_mapped_metadata

### tests for extract_ids_from_post_mapped_metadata


def test_extract_ids_with_protein_sequence_accessions():
    metadata = {"protein": {"sequence_accessions": ["P12345", "Q67890"]}}
    result = extract_ids_from_post_mapped_metadata(metadata)
    assert result == ["P12345", "Q67890"]


def test_extract_ids_with_cdna_sequence_accessions():
    metadata = {"cdna": {"sequence_accessions": ["NM_001", "NM_002"]}}
    result = extract_ids_from_post_mapped_metadata(metadata)
    assert result == ["NM_001", "NM_002"]


def test_extract_ids_with_both_protein_and_cdna():
    metadata = {"protein": {"sequence_accessions": ["P11111"]}, "cdna": {"sequence_accessions": ["NM_003"]}}
    # Should prefer "protein" if present
    result = extract_ids_from_post_mapped_metadata(metadata)
    assert result == ["P11111"]


def test_extract_ids_with_no_sequence_accessions():
    metadata = {"protein": {}, "cdna": {}}
    result = extract_ids_from_post_mapped_metadata(metadata)
    assert result is None


def test_extract_ids_with_empty_dict():
    metadata = {}
    result = extract_ids_from_post_mapped_metadata(metadata)
    assert result is None


def test_extract_ids_with_none():
    result = extract_ids_from_post_mapped_metadata(None)
    assert result is None


def test_extract_ids_with_unrelated_keys():
    metadata = {"genomic": {"sequence_accessions": ["G0001"]}}
    result = extract_ids_from_post_mapped_metadata(metadata)
    assert result is None
