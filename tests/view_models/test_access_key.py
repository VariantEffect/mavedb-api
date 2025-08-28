from mavedb.view_models.access_key import SavedAccessKey

from tests.helpers.util.common import dummy_attributed_object_from_dict


def test_minimal_access_key():
    key_id = "test_access_key"
    access_key = dummy_attributed_object_from_dict({"key_id": key_id})
    saved_access_key = SavedAccessKey.model_validate(access_key)

    assert saved_access_key.key_id == key_id
