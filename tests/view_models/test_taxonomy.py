from mavedb.view_models.taxonomy import TaxonomyCreate, Taxonomy

from tests.helpers.util import dummy_attributed_object_from_dict
from tests.helpers.constants import TEST_MINIMAL_TAXONOMY, TEST_POPULATED_TAXONOMY, TEST_SAVED_TAXONOMY


def test_minimal_taxonomy_create():
    taxonomy = TaxonomyCreate(**TEST_MINIMAL_TAXONOMY)
    assert all(taxonomy.__getattribute__(k) == v for k, v in TEST_MINIMAL_TAXONOMY.items())


def test_populated_taxonomy_create():
    taxonomy = TaxonomyCreate(**TEST_POPULATED_TAXONOMY)
    assert all(taxonomy.__getattribute__(k) == v for k, v in TEST_POPULATED_TAXONOMY.items())


def test_saved_taxonomy():
    taxonomy = Taxonomy.model_validate(dummy_attributed_object_from_dict(TEST_SAVED_TAXONOMY))
    assert all(taxonomy.__getattribute__(k) == v for k, v in TEST_SAVED_TAXONOMY.items())
