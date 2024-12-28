from mavedb.view_models.api_version import ApiVersion


def test_minimal_api_version():
    name = "test_api_version"
    version = "a.0.0"
    api_version = ApiVersion.model_validate({"name": name, "version": version})

    assert api_version.name == name
    assert api_version.version == version
