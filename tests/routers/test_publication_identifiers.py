import pytest  # noqa: F401

from sqlalchemy import select, delete

from mavedb.models.publication_identifier import PublicationIdentifier

from tests.helpers.constants import TEST_PUBMED_PUBLICATION


# TODO#497: Expand test coverage for publication identifier routes.


def test_show_publication_identifiers_multiple_exist(client, setup_router_db, session):
    """
    Test the endpoint for listing publication identifiers when multiple exist.
    """
    new_journal_name = "New Journal"

    # Create a duplicate publication identifier (skip PK/dates)
    existing_item = session.scalars(select(PublicationIdentifier)).first()
    duplicate_item = PublicationIdentifier(
        identifier=existing_item.identifier,
        db_name=existing_item.db_name,
        db_version=existing_item.db_version,
        title=existing_item.title,
        abstract=existing_item.abstract,
        authors=existing_item.authors,
        doi=existing_item.doi,
        publication_year=existing_item.publication_year,
        publication_journal=new_journal_name,  # override for clarity in assertions
        url=existing_item.url,
        reference_html=existing_item.reference_html,
    )
    session.add(duplicate_item)
    session.commit()

    response = client.get("/api/v1/publication-identifiers/journals")

    # Assert the response
    assert response.status_code == 200
    assert response.json() == sorted([TEST_PUBMED_PUBLICATION["publication_journal"], new_journal_name])


def test_show_publication_journals_one_exists(client, setup_router_db):
    """
    Test the endpoint for listing publication journals when one exists.
    """

    # Call the API endpoint
    response = client.get("/api/v1/publication-identifiers/journals")

    # Assert the response
    assert response.status_code == 200
    assert response.json() == [TEST_PUBMED_PUBLICATION["publication_journal"]]


def test_show_publication_journals_none_type_journal(client, setup_router_db, session):
    """
    Test the endpoint for listing publication journals when one is of type None.
    """
    item = session.scalars(select(PublicationIdentifier)).first()
    item.publication_journal = None
    session.add(item)
    session.commit()

    # Call the API endpoint
    response = client.get("/api/v1/publication-identifiers/journals")

    # Assert the response
    assert response.status_code == 200
    assert response.json() == []


def test_show_publication_journals_none_exist(client, setup_router_db, session):
    """
    Test the endpoint for listing publication journals when none are available.
    """
    session.execute(delete(PublicationIdentifier))
    session.commit()

    # Call the API endpoint
    response = client.get("/api/v1/publication-identifiers/journals")

    # Assert the response
    assert response.status_code == 200
    assert response.json() == []
