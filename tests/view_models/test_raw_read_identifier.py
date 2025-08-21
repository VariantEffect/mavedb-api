from mavedb.view_models.raw_read_identifier import RawReadIdentifierCreate, RawReadIdentifier

from tests.helpers.util.common import dummy_attributed_object_from_dict
from tests.helpers.constants import TEST_MINIMAL_RAW_READ_IDENTIFIER, TEST_SAVED_MINIMAL_RAW_READ_IDENTIFIER


def test_minimal_raw_read_identifier_create():
    raw_read_identifier = RawReadIdentifierCreate(**TEST_MINIMAL_RAW_READ_IDENTIFIER)

    assert all(raw_read_identifier.__getattribute__(k) == v for k, v in TEST_MINIMAL_RAW_READ_IDENTIFIER.items())


def test_minimal_raw_read_identifier():
    saved_raw_read_identifier = RawReadIdentifier.model_validate(
        dummy_attributed_object_from_dict(TEST_SAVED_MINIMAL_RAW_READ_IDENTIFIER)
    )

    assert all(
        saved_raw_read_identifier.__getattribute__(k) == v for k, v in TEST_SAVED_MINIMAL_RAW_READ_IDENTIFIER.items()
    )
