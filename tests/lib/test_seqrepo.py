# ruff: noqa: E402
import pytest

pytest.importorskip("biocommons")
pytest.importorskip("bioutils")

from mavedb.lib.seqrepo import get_sequence_ids, _generate_nsa_options, seqrepo_versions
from mavedb.lib import seqrepo as seqrepo_lib

from tests.helpers.constants import TEST_SEQREPO_INITIAL_STATE


@pytest.mark.parametrize("entry", TEST_SEQREPO_INITIAL_STATE)
def test_get_fully_qualified_sequence_ids(entry, seqrepo):
    alias = list(entry.keys())[0]
    result = get_sequence_ids(seqrepo, alias)
    assert sorted(result) == sorted([entry[alias]["seq_id"]])


@pytest.mark.parametrize("entry", TEST_SEQREPO_INITIAL_STATE)
def test_get_inferred_sequence_ids(entry, seqrepo):
    alias = list(entry.keys())[0]
    unqualified_alias = alias.split(":")[-1]  # Remove namespace prefix
    result = get_sequence_ids(seqrepo, unqualified_alias)

    if "ga4gh" in alias:
        # For GA4GH identifiers, we cannot infer the namespace.
        assert sorted(result) == []
    else:
        assert sorted(result) == sorted([entry[alias]["seq_id"]])


def test_get_sequence_ids_no_match(seqrepo):
    result = get_sequence_ids(seqrepo, "notfound")
    assert [] == result


@pytest.mark.parametrize(
    "query,expected",
    [
        # Fully-qualified identifier
        ("refseq:NM_000551.3", [("refseq", "NM_000551.3")]),
        ("gi:123456789", [("gi", "123456789")]),
        # Namespace inferred
        ("ENST00000530893.6", [("ensembl", "ENST00000530893.6")]),
        ("NM_000551.3", [("refseq", "NM_000551.3")]),
        # Hex digest (MD5/VMC)
        ("01234abcde", [("MD5", "01234abcde%"), ("VMC", "GS_ASNKvN4=%")]),
        # No match, fallback to (None, query)
        ("notfound", [(None, "notfound")]),
    ],
)
def test_generate_nsa_options(query, expected):
    result = _generate_nsa_options(query)
    assert result == expected


@pytest.mark.parametrize(
    "env_value,expected_data_version",
    [
        (None, "unknown"),
        ("", "unknown"),
        ("/some/path/seqrepo/20240101", "20240101"),
        ("/another/path/seqrepo_data/v2023.12", "v2023.12"),
        ("/seqrepo", "seqrepo"),
    ],
)
def test_seqrepo_versions(monkeypatch, env_value, expected_data_version):
    # Patch os.getenv to return env_value for HGVS_SEQREPO_DIR
    monkeypatch.setattr("os.getenv", lambda key: env_value if key == "HGVS_SEQREPO_DIR" else None)
    # Patch seqrepo_dep_version to a known value for test
    monkeypatch.setattr(seqrepo_lib, "seqrepo_dep_version", "1.2.3")
    result = seqrepo_versions()
    assert result["seqrepo_dependency_version"] == "1.2.3"
    assert result["seqrepo_data_version"] == expected_data_version
