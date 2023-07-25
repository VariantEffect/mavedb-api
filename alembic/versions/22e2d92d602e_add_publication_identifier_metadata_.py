"""add publication identifier metadata columns

Revision ID: 22e2d92d602e
Revises: da9ba478647d
Create Date: 2023-06-01 14:51:04.700969

"""
from typing import Optional
import os

import eutils
from eutils._internal.xmlfacades.pubmedarticleset import PubmedArticleSet
import sqlalchemy as sa
from eutils import EutilsNCBIError
from mavedb.lib.exceptions import AmbiguousIdentifierError
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from alembic import op
from mavedb.lib.identifiers import ExternalPublication
from mavedb.lib.rxiv import Rxiv
from mavedb.models.publication_identifier import PublicationIdentifier

# revision identifiers, used by Alembic.
revision = "22e2d92d602e"
down_revision = "33e99d4b90cc"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    session = Session(bind=conn)

    # add new publication data metadata cols
    op.add_column(
        table_name="publication_identifiers",
        column=sa.Column("title", sa.Text, nullable=True),
    )
    op.add_column(
        table_name="publication_identifiers",
        column=sa.Column("abstract", sa.Text, nullable=True),
    )
    op.add_column(
        table_name="publication_identifiers",
        column=sa.Column("authors", JSONB, nullable=True),
    )
    op.add_column(
        table_name="publication_identifiers",
        column=sa.Column("publication_doi", sa.Text, nullable=True),
    )
    op.add_column(
        table_name="publication_identifiers",
        column=sa.Column("preprint_doi", sa.Text, nullable=True),
    )
    # Metapub only provides the publication year
    op.add_column(
        table_name="publication_identifiers",
        column=sa.Column("publication_year", sa.Integer, nullable=True),
    )
    op.add_column(
        table_name="publication_identifiers",
        column=sa.Column("preprint_date", sa.Date, nullable=True),
    )
    op.add_column(
        table_name="publication_identifiers",
        column=sa.Column("publication_journal", sa.Text, nullable=True),
    )

    # Data migration to back-populate all publication identifiers with metadata
    for item in session.query(PublicationIdentifier).filter_by(title=None):
        if item.db_name == "PubMed":
            pub_article = fetch_pubmed_article(item.identifier)
        else:
            pub_article = None
        if item.db_name == "bioRxiv":
            bio_article = fetch_biorxiv_article(item.identifier)
        else:
            bio_article = None
        if item.db_name == "medRxiv":
            med_article = fetch_medrxiv_article(item.identifier)
        else:
            med_article = None

        if pub_article:
            item.title = pub_article.title
            item.abstract = pub_article.abstract
            item.publication_doi = pub_article.publication_doi
            item.publication_year = pub_article.publication_year
            item.publication_journal = pub_article.publication_journal

            authors = [str(author["name"]).replace("'", "''") for author in pub_article.authors]
            authors = [{"name": author, "primary": idx == 0} for idx, author in enumerate(authors)]
            item.authors = authors

        if bio_article:
            item.title = bio_article.title
            item.abstract = bio_article.abstract
            item.preprint_doi = bio_article.preprint_doi
            item.preprint_date = bio_article.preprint_date
            item.reference_html = bio_article.reference_html

            authors = [str(author["name"]).replace("'", "''") for author in bio_article.authors]
            authors = [{"name": author, "primary": idx == 0} for idx, author in enumerate(authors)]
            item.authors = authors

        if med_article:
            item.title = med_article.title
            item.abstract = med_article.abstract
            item.preprint_doi = med_article.preprint_doi
            item.preprint_date = med_article.preprint_date
            item.reference_html = med_article.reference_html

            authors = [str(author["name"]).replace("'", "''") for author in med_article.authors]
            authors = [{"name": author, "primary": idx == 0} for idx, author in enumerate(authors)]
            item.authors = authors

    session.commit()

    # make our title and authors columns non-nullable and add a constraint so that one DOI must be filled
    op.alter_column("publication_identifiers", column_name="title", nullable=False)
    op.alter_column("publication_identifiers", column_name="authors", nullable=False)
    op.create_check_constraint(
        "publication_doi_preprint_doi_not_null_constraint",
        table_name="publication_identifiers",
        condition="(publication_doi IS NULL) <> (preprint_doi IS NULL)",
    )


def downgrade():
    op.drop_constraint(
        table_name="publication_identifiers", constraint_name="publication_doi_preprint_doi_not_null_constraint"
    )

    op.drop_column(table_name="publication_identifiers", column_name="title")
    op.drop_column(table_name="publication_identifiers", column_name="abstract")
    op.drop_column(table_name="publication_identifiers", column_name="authors")
    op.drop_column(table_name="publication_identifiers", column_name="publication_doi")
    op.drop_column(table_name="publication_identifiers", column_name="preprint_doi")
    op.drop_column(table_name="publication_identifiers", column_name="publication_year")
    op.drop_column(table_name="publication_identifiers", column_name="preprint_date")
    op.drop_column(table_name="publication_identifiers", column_name="publication_journal")


# Pull these in here to spoof non-async behavior, ideally we only run this migration once and
# don't have to worry about any drift.
def fetch_pubmed_article(identifier: str) -> Optional[ExternalPublication]:
    """
    Fetch an existing PubMed article from NCBI
    """
    fetch = eutils.QueryService(api_key=os.getenv("NCBI_API_KEY"))
    try:
        fetched_articles = list(PubmedArticleSet(fetch.efetch({"db": "pubmed", "id": identifier})))
        assert len(fetched_articles) < 2
        article = ExternalPublication(identifier=identifier, db_name="PubMed", external_publication=fetched_articles[0])

    except AssertionError as exc:
        raise AmbiguousIdentifierError(f"Fetched more than 1 PubMed article associated with PMID {identifier}") from exc
    except EutilsNCBIError:
        return None
    except IndexError:
        return None
    else:
        return article


def fetch_biorxiv_article(identifier: str) -> Optional[ExternalPublication]:
    """
    Fetch an existing bioRxiv article from Rxiv
    """
    fetch = Rxiv("https://api.biorxiv.org", "biorxiv")
    try:
        article = fetch.content_detail(identifier=identifier)
        article = ExternalPublication(identifier=identifier, db_name="bioRxiv", external_publication=article[-1])
    except IndexError:
        return None
    else:
        return article


def fetch_medrxiv_article(identifier: str) -> Optional[ExternalPublication]:
    """
    Fetch an existing medRxiv article from Rxiv
    """
    fetch = Rxiv("https://api.biorxiv.org", "medrxiv")
    try:
        article = fetch.content_detail(identifier=identifier)
        article = ExternalPublication(identifier=identifier, db_name="medRxiv", external_publication=article[-1])
    except IndexError:
        return None
    else:
        return article
