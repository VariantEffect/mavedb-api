import logging

from sqlalchemy import text
from mavedb.models.score_set import ScoreSet
from alembic_utils.pg_materialized_view import PGMaterializedView

logger = logging.getLogger(__name__)

# TODO(#94): add LICENSE, plus TAX_ID if numeric
# TODO(#89): The query below should be generated from SQLAlchemy
#            models rather than hand-carved SQL

_scoreset_fulltext_view = PGMaterializedView(
    schema="public",
    signature="scoreset_fulltext",
    definition=' union ' .join(
        [
            f"select id as scoreset_id, to_tsvector({c}) as text from scoresets"
            for c in ('urn', 'title', 'short_description', 'abstract_text')
        ] + [
            f"select scoreset_id, to_tsvector({c}) as text from target_genes"
            for c in ('name', 'category')
        ] + [
            f"select scoreset_id, to_tsvector(TX.{c}) as text from target_genes TG join target_sequences TS on \
            (TG.target_sequence_id = TS.id) join taxonomies TX on (TS.taxonomy_id = TX.id)"
            for c in ('organism_name', 'common_name')
        ] + [
            "select scoreset_id, to_tsvector(TA.assembly) as text from target_genes TG join target_accessions TA on \
            (TG.accession_id = TA.id)"
        ] + [
            f"select scoreset_id, to_tsvector(PI.{c}) as text from scoreset_publication_identifiers SPI JOIN \
            publication_identifiers PI ON (SPI.publication_identifier_id = PI.id)"
            for c in ('identifier', 'doi', 'abstract', 'title', 'publication_journal')
        ] + [
            "select scoreset_id, to_tsvector(jsonb_array_elements(authors)->'name') as text from \
            scoreset_publication_identifiers SPI join publication_identifiers PI on \
            SPI.publication_identifier_id = PI.id",
        ] + [
            "select scoreset_id, to_tsvector(DI.identifier) as text from scoreset_doi_identifiers SD join \
            doi_identifiers DI on (SD.doi_identifier_id = DI.id)",
        ] + [
            f"select scoreset_id, to_tsvector(XI.identifier) as text from target_genes TG join {x}_offsets XO on \
            (XO.target_gene_id = TG.id) join {x}_identifiers XI on (XI.id = XO.identifier_id)"
            for x in ('uniprot', 'refseq', 'ensembl')
        ]
    ),
    with_data=True
)


def scoreset_fulltext_create(session):
    logger.warning("Creating %s", _scoreset_fulltext_view.signature)
    session.execute(
        _scoreset_fulltext_view.to_sql_statement_create()
    )
    session.commit()
    logger.warning("Created %s", _scoreset_fulltext_view.signature)


def scoreset_fulltext_destroy(session):
    logger.warning("Destroying %s", _scoreset_fulltext_view.signature)
    session.execute(
        _scoreset_fulltext_view.to_sql_statement_drop()
    )
    session.commit()
    logger.warning("Destroyed %s", _scoreset_fulltext_view.signature)


def scoreset_fulltext_refresh(session):
    session.execute(text(f'refresh materialized view {_scoreset_fulltext_view.signature}'))
    session.commit()


def scoreset_fulltext_filter(query, string):
    return query.filter(ScoreSet.id.in_(
        text(f"select distinct scoreset_id from {_scoreset_fulltext_view.signature} \
             where text @@ websearch_to_tsquery(:text)").params(text=string)
    ))
