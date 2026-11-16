import pytest

from mavedb.lib.clingen.urls import clingen_allele_url


@pytest.mark.unit
@pytest.mark.parametrize("allele_id", ["CA123", "PA456"])
def test_clingen_allele_url_links_the_registry_page(allele_id):
    assert clingen_allele_url(allele_id) == (
        f"https://reg.clinicalgenome.org/redmine/projects/registry/genboree_registry/by_caid?caid={allele_id}"
    )
