"""Network tests for the HGNC REST client. Require a live connection to rest.genenames.org."""

import pytest

from mavedb.lib.exceptions import HGNCGeneNotFoundError
from mavedb.lib.hgnc.client import HGNCGeneInfo, fetch_gene_info


@pytest.mark.network
class TestFetchGeneInfoNetwork:
    def test_known_gene_returns_expected_fields(self):
        result = fetch_gene_info("BRCA1")

        assert isinstance(result, HGNCGeneInfo)
        assert result.symbol == "BRCA1"
        assert result.name
        assert result.hgnc_id and result.hgnc_id.startswith("HGNC:")

    def test_hyphen_containing_symbol_resolves(self):
        result = fetch_gene_info("HLA-A")

        assert isinstance(result, HGNCGeneInfo)
        assert result.symbol == "HLA-A"
        assert result.hgnc_id and result.hgnc_id.startswith("HGNC:")

    def test_unknown_symbol_raises_not_found(self):
        with pytest.raises(HGNCGeneNotFoundError):
            fetch_gene_info("NOTAREALSYMBOL99999")
