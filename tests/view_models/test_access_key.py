from mavedb.view_models.access_key import SavedAccessKey
from tests.helpers.util.common import dummy_attributed_object_from_dict


def test_minimal_access_key():
    access_key = dummy_attributed_object_from_dict({"id": 1, "key_id": "stored hash", "key_hint": "wxyz"})
    saved_access_key = SavedAccessKey.model_validate(access_key)

    assert saved_access_key.id == 1
    assert saved_access_key.key_hint == "wxyz"
    assert not hasattr(saved_access_key, "key_id")
