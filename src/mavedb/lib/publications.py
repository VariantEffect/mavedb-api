"""Finding, storing and citing the publications that records cite.

Fetching from the publication databases themselves lives in :mod:`mavedb.lib.external_publications`.
"""

import logging
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, TypeVar, Union

from idutils import is_doi, normalize_doi
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from mavedb.lib.exceptions import AmbiguousIdentifierError, NonexistentIdentifierError, PublicationServiceError
from mavedb.lib.external_publications import (
    ExternalPublication,
    fetch_biorxiv_article,
    fetch_crossref_work,
    fetch_medrxiv_article,
    fetch_pubmed_article,
)
from mavedb.lib.validation.publication import identifier_valid_for, infer_identifier_from_url
from mavedb.models.experiment_publication_identifier import ExperimentPublicationIdentifierAssociation
from mavedb.models.mixins.publication_citation import PublicationKey
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation

if TYPE_CHECKING:
    from mavedb.view_models.publication_identifier import PublicationIdentifierBase

logger = logging.getLogger(__name__)

RecordCitation = TypeVar(
    "RecordCitation", ExperimentPublicationIdentifierAssociation, ScoreSetPublicationIdentifierAssociation
)


def find_generic_article(
    db: Session, identifier: str, db_name: Optional[str] = None
) -> Mapping[str, Union[PublicationIdentifier, ExternalPublication, None]]:
    """
    Check if a provided publication identifier ambiguously identifies a publication,
    ie the same identifier is identifies publications in multiple publication databases
    that we accept.

    Stored publications stand in for fetching the article. A stored publication the caller can't see is treated as
    absent and fetched, so the result doesn't reveal that MaveDB holds it.

    :param db: An active database session
    :param identifier: A valid publication identifier
    :param db_name: The publication database to look in, if the caller knows it
    :return: Each candidate publication database, mapped to its stored or fetched article, or None if it has none.
    :raises PublicationServiceError: A publication database failed, and no other candidate found the article.
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
        existing_publication = _stored_publication(db, db_name, identifier)

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

        # A preprint and its PubMed record can share a DOI, so more than one row may match. Prefer the Crossref
        # row, whose identifier is the DOI, then the oldest.
        existing_publication = db.execute(
            select(PublicationIdentifier)
            .filter(PublicationIdentifier.doi == identifier)
            .order_by(PublicationIdentifier.db_name != "Crossref", PublicationIdentifier.id)
            .limit(1)
        ).scalar_one_or_none()

        if not existing_publication:
            external_publication = fetch_crossref_work(identifier)
            found_articles["Crossref"] = (
                ExternalPublication(identifier, "Crossref", external_publication) if external_publication else None
            )
        else:
            # A publication found by its DOI may have come from PubMed or a preprint server, not Crossref.
            found_articles[existing_publication.db_name] = existing_publication

        return found_articles

    # When we are not provided a db name, we must try to match the provided identifier to a
    # publication from each one of our accepted databases.
    last_fetch_error: Optional[PublicationServiceError] = None
    for publication_db, identifier_valid in identifier_valid_for(identifier).items():
        if identifier_valid:
            existing_publication = _stored_publication(db, publication_db, identifier)

            if not existing_publication:
                try:
                    external_publication = db_specific_fetches[publication_db](identifier)
                except PublicationServiceError as exc:
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


def _stored_publication(db: Session, db_name: str, identifier: str) -> Optional[PublicationIdentifier]:
    return db.execute(
        select(PublicationIdentifier).filter_by(db_name=db_name, identifier=identifier)
    ).scalar_one_or_none()


def resolve_publication(
    db: Session, identifier: str, db_name: Optional[str] = None
) -> Union[PublicationIdentifier, ExternalPublication]:
    """The one publication a submitted identifier names, stored or fetched.

    :param db: An active database session
    :param identifier: A valid publication identifier, or a url from an accepted publication database
    :param db_name: The publication database the identifier belongs to, if the submission named one
    :raises NonexistentIdentifierError: No accepted publication database has the identifier.
    :raises AmbiguousIdentifierError: More than one accepted publication database has the identifier.
    :raises PublicationServiceError: A publication database failed, and no other candidate found the article.
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

    # TODO(#214): It may be useful for internal consistency to use the Crossref record fetched via DOI if it exists. If a publication did not
    #             have a DOI, we would need to use the record as returned by PubMed/bioRxiv/medRxiv.
    return next(article for article in article_matches.values() if article is not None)


def _publication_row(article: ExternalPublication) -> dict[str, Any]:
    return {
        "identifier": article.identifier,
        "db_name": article.db_name,
        "url": article.url,
        "title": article.title,
        "abstract": article.abstract,
        "authors": article.authors,
        "publication_year": article.publication_year,
        "doi": article.doi,
        "publication_journal": article.publication_journal,
        "reference_html": article.reference_html,
    }


def ensure_publications(db: Session, articles: Iterable[ExternalPublication]) -> None:
    """Store the fetched articles that MaveDB doesn't already hold.

    Under row-level security a caller can't see a publication cited only by others' private records, so this can't
    look before it writes. ``ON CONFLICT DO NOTHING`` with no conflict target skips a row that exists whether or not
    the caller can see it; naming a target would need SELECT on the row. The insert is inline, so it has no
    ``RETURNING``, which would fail for a row nothing cites yet.
    """
    rows = [_publication_row(article) for article in articles]
    if rows:
        db.execute(insert(PublicationIdentifier.__table__).on_conflict_do_nothing().inline(), rows)


def ensure_publication_keys(
    db: Session, *submissions: Iterable["PublicationIdentifierBase"]
) -> list[list[PublicationKey]]:
    """The keys of each list of submitted publications, in the order given, storing any that MaveDB doesn't hold.

    A list's keys are distinct, even when two of its submissions name one publication, so callers can match them
    one to one against the citations a record already has. Cite the publications by these keys. The api never reads a publication to cite it, since the caller may not be
    able to see it. Pass every list a record cites together: a publication stored for one list stays hidden from a
    read policy until a citation commits, so resolving the lists separately would fetch it again for the next.

    :raises PublicationServiceError: A publication database failed while resolving a submission (see
        :func:`resolve_publication`).
    """
    submitted = [[(publication.identifier, publication.db_name) for publication in s] for s in submissions]
    resolved = {
        submission: resolve_publication(db, *submission)
        for submission in dict.fromkeys(submission for publications in submitted for submission in publications)
    }
    ensure_publications(db, [article for article in resolved.values() if isinstance(article, ExternalPublication)])
    # Two submissions can name one publication, such as its identifier and its url.
    return [
        list(dict.fromkeys(PublicationKey(resolved[s].db_name, resolved[s].identifier) for s in publications))
        for publications in submitted
    ]


def record_publication_citations(
    citation: type[RecordCitation], primary: Iterable[PublicationKey], secondary: Iterable[PublicationKey]
) -> list[RecordCitation]:
    """Citation rows for a record's primary and secondary publications.

    A record cites a publication once, so one given as both primary and secondary is primary.
    """
    is_primary = {**dict.fromkeys(secondary, False), **dict.fromkeys(primary, True)}
    return [
        citation(db_name=key.db_name, identifier=key.identifier, primary=cited_as_primary)
        for key, cited_as_primary in is_primary.items()
    ]
