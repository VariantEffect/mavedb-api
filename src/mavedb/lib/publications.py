"""Finding and storing the publications that records cite.

Fetching from the publication databases themselves lives in :mod:`mavedb.lib.external_publications`.
"""

import json
import logging
from typing import Mapping, Optional, Union

import requests
from idutils import is_doi, normalize_doi
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.exceptions import AmbiguousIdentifierError, NonexistentIdentifierError
from mavedb.lib.external_publications import (
    ExternalPublication,
    fetch_biorxiv_article,
    fetch_crossref_work,
    fetch_medrxiv_article,
    fetch_pubmed_article,
)
from mavedb.lib.validation.publication import identifier_valid_for, infer_identifier_from_url
from mavedb.models.publication_identifier import PublicationIdentifier

logger = logging.getLogger(__name__)


def find_generic_article(
    db: Session, identifier: str, db_name: Optional[str] = None
) -> Mapping[str, Union[PublicationIdentifier, ExternalPublication, None]]:
    """
    Check if a provided publication identifier ambiguously identifies a publication,
    ie the same identifier is identifies publications in multiple publication databases
    that we accept.

    :param db: An active database session
    :param identifier: A valid publication identifier
    :return: A list of databases where this identifier exists.
    """
    db_specific_fetches = {
        "Crossref": fetch_crossref_work,
        "PubMed": fetch_pubmed_article,
        "bioRxiv": fetch_biorxiv_article,
        "medRxiv": fetch_medrxiv_article,
    }

    # We also accept URLs from our accepted publications. Attempt to convert a potential URL to an identifier.
    identifier = infer_identifier_from_url(identifier)

    # Only check entries with the appropriate `db_name` if one is provided.
    db_specific_match: dict[str, Union[PublicationIdentifier, ExternalPublication, None]] = {}
    if db_name:
        if db_name == "Crossref":
            internal_publication_query = select(PublicationIdentifier).filter(PublicationIdentifier.doi == identifier)
        else:
            internal_publication_query = select(PublicationIdentifier).filter(
                PublicationIdentifier.identifier == identifier
            )

        existing_publication = db.execute(
            internal_publication_query.filter(PublicationIdentifier.db_name == db_name)
        ).scalar_one_or_none()

        if not existing_publication:
            external_publication = db_specific_fetches[db_name](identifier)
            db_specific_match = {
                db_name: (
                    ExternalPublication(identifier, db_name, external_publication) if external_publication else None
                )
            }
        else:
            db_specific_match = {db_name: existing_publication}

        return db_specific_match

    # If the identifier is a DOI, it will necessarily have a unique match in Crossref (if such a match exists).
    # Return this match directly where possible.
    found_articles: dict[str, Union[PublicationIdentifier, ExternalPublication, None]] = {}
    if is_doi(identifier):
        identifier = normalize_doi(identifier)

        existing_publication = db.execute(
            select(PublicationIdentifier).filter(PublicationIdentifier.doi == identifier)
        ).scalar_one_or_none()

        if not existing_publication:
            external_publication = fetch_crossref_work(identifier)
            found_articles["Crossref"] = (
                ExternalPublication(identifier, "Crossref", external_publication) if external_publication else None
            )
        else:
            # When we find an existing publication via DOI, it is not always the case that it came from Crossref originally.
            # Use the existing publication db as the article key if it exists, otherwise default to Crossref since this is a DOI.
            existing_db_name = existing_publication.db_name if existing_publication.db_name else "Crossref"
            found_articles[existing_db_name] = existing_publication

        return found_articles

    # When we are not provided a db name, we must try to match the provided identifier to a
    # publication from each one of our accepted databases.
    last_fetch_error: Optional[Exception] = None
    for publication_db, identifier_valid in identifier_valid_for(identifier).items():
        if identifier_valid:
            existing_publication = db.execute(
                select(PublicationIdentifier)
                .filter(PublicationIdentifier.identifier == identifier)
                .filter(PublicationIdentifier.db_name == publication_db)
            ).scalar_one_or_none()

            if not existing_publication:
                try:
                    external_publication = db_specific_fetches[publication_db](identifier)
                except (json.JSONDecodeError, requests.exceptions.RequestException) as exc:
                    logger.warning(
                        "Failed to fetch identifier %r from %s while fanning out over candidate databases.",
                        identifier,
                        publication_db,
                        exc_info=True,
                    )

                    external_publication = None
                    last_fetch_error = exc

                found_articles[publication_db] = (
                    ExternalPublication(identifier, publication_db, external_publication)
                    if external_publication
                    else None
                )
            else:
                found_articles[publication_db] = existing_publication

    if last_fetch_error is not None and not any(found_articles.values()):
        raise last_fetch_error

    return found_articles


def create_generic_article(article: ExternalPublication) -> PublicationIdentifier:
    """
    Create a new (unsaved) publication identifier object based on the provided identifier, article metadata,
    and publication database name.
    """
    return PublicationIdentifier(
        identifier=article.identifier,
        db_name=article.db_name,
        url=article.url,
        title=article.title,
        abstract=article.abstract,
        authors=article.authors,
        publication_year=article.publication_year,
        doi=article.doi,
        publication_journal=article.publication_journal,
        reference_html=article.reference_html,
    )


def find_or_create_publication_identifier(
    db: Session, identifier: str, db_name: Optional[str] = None
) -> PublicationIdentifier:
    """
    Find an existing publication identifier record with the specified identifier string, or create a new one.

    :param db: An active database session
    :param identifier: A valid publication identifier
    :return: An existing PublicationIdentifier containing the specified identifier string, or a new, unsaved PublicationIdentifier
    """
    article_matches = find_generic_article(db, identifier, db_name)

    if not any(article_matches.values()):
        raise NonexistentIdentifierError(
            f"No matching articles found for identifier {identifier} across all accepted publication databases."
        )

    if sum(article is not None for article in article_matches.values()) > 1:
        raise AmbiguousIdentifierError(
            f"Found multiple articles associated with identifier {identifier}. Specify a `db_name` along with this identifier to avoid ambiguity."
        )

    matched_article = next(article for article in article_matches.values() if article is not None)

    # If the article already exists, return it directly.
    if isinstance(matched_article, PublicationIdentifier):
        return matched_article

    # TODO(#214): It may be useful for internal consistency to use the Crossref record fetched via DOI if it exists. If a publication did not
    #             have a DOI, we would need to use the record as returned by PubMed/bioRxiv/medRxiv.

    return create_generic_article(matched_article)
