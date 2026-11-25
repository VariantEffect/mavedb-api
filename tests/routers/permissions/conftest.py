import pytest

from tests.routers.permissions.canary import plant_canary


@pytest.fixture
def canary(
    session,
    data_provider,
    owner_client,
    outsider_client,
    mapper_client,
    admin_client,
    anonymous_client,
    client_for,
    make_contributor,
    make_collection_member,
):
    """The planted canary, with a client for each persona."""
    return plant_canary(
        session,
        data_provider,
        owner=owner_client,
        outsider=outsider_client,
        mapper=mapper_client,
        admin=admin_client,
        anonymous=anonymous_client,
        client_for=client_for,
        make_contributor=make_contributor,
        make_collection_member=make_collection_member,
    )
