"""store DOI, raw read and target gene identifiers on the rows that use them

Revision ID: 2dfdc4a88870
Revises: 6987f253e02a
Create Date: 2026-10-21

DOI, raw read (SRA), Ensembl, RefSeq and UniProt identifiers each lived in a shared table that records linked
to by id. Every column but the identifier was a constant, always null, or derivable from it, so the identifier
moves onto the row that uses it (#944):

- The six DOI and raw read link tables hold the identifier string, keyed by ``(<parent>_id, identifier)``.
  DOIs are upper-cased on the way, the form the DOI registry stores, since they're case-insensitive and a record
  shouldn't cite one twice.
- ``ensembl_offsets``, ``refseq_offsets`` and ``uniprot_offsets`` merge into
  ``target_gene_external_identifiers``, keyed by ``(target_gene_id, db_name)``: a target gene has at most one
  identifier per database.
- The five shared identifier tables are dropped, with any rows no record linked to. Urls are computed when
  serializing.

Under row-level security (#808), these become owned rows, visible exactly when their record is.

The tables are small (a few thousand rows in all), so the rewrites hold their locks briefly.

The downgrade rebuilds the shared tables from the link rows, so the unlinked rows don't come back. Ids are
reassigned, the dates are the day of the downgrade, and urls are rebuilt the way the api used to build them, so
gene identifier urls come back null.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "2dfdc4a88870"
down_revision = "6987f253e02a"
branch_labels = None
depends_on = None

# (link table, parent column, shared table, shared id column)
DOI_LINKS = [
    ("scoreset_doi_identifiers", "scoreset_id", "doi_identifiers", "doi_identifier_id"),
    ("experiment_doi_identifiers", "experiment_id", "doi_identifiers", "doi_identifier_id"),
    ("experiment_set_doi_identifiers", "experiment_set_id", "doi_identifiers", "doi_identifier_id"),
]
RAW_READ_LINKS = [
    ("scoreset_sra_identifiers", "scoreset_id", "sra_identifiers", "sra_identifier_id"),
    ("experiment_sra_identifiers", "experiment_id", "sra_identifiers", "sra_identifier_id"),
    ("experiment_set_sra_identifiers", "experiment_set_id", "sra_identifiers", "sra_identifier_id"),
]

# (db_name, offset table, identifier table, whether the identifier table has reference_html)
GENE_IDENTIFIERS = [
    ("Ensembl", "ensembl_offsets", "ensembl_identifiers", False),
    ("RefSeq", "refseq_offsets", "refseq_identifiers", True),
    ("UniProt", "uniprot_offsets", "uniprot_identifiers", True),
]


def _fold_link_table(link: str, parent: str, shared: str, shared_id: str, normalize: str) -> None:
    op.execute(f"ALTER TABLE {link} ADD COLUMN identifier character varying")
    op.execute(
        f"UPDATE {link} l SET identifier = {normalize.format('s.identifier')} FROM {shared} s WHERE s.id = l.{shared_id}"
    )

    # Two shared rows differing only in case would collide once upper-cased. There are none today.
    op.execute(
        f"DELETE FROM {link} a USING {link} b "
        f"WHERE a.{parent} = b.{parent} AND a.identifier = b.identifier AND a.ctid > b.ctid"
    )

    op.execute(f"ALTER TABLE {link} DROP CONSTRAINT {link}_pkey")
    op.execute(f"ALTER TABLE {link} DROP COLUMN {shared_id}")
    op.execute(f"ALTER TABLE {link} ALTER COLUMN identifier SET NOT NULL")
    op.execute(f"ALTER TABLE {link} ADD CONSTRAINT {link}_pkey PRIMARY KEY ({parent}, identifier)")


def _create_shared_table(shared: str, with_reference_html: bool = False) -> None:
    reference_html = "reference_html character varying," if with_reference_html else ""
    op.execute(
        f"""
        CREATE TABLE {shared} (
            id serial NOT NULL,
            identifier character varying NOT NULL,
            db_name character varying NOT NULL,
            db_version character varying,
            url character varying,
            {reference_html}
            creation_date date NOT NULL,
            modification_date date NOT NULL,
            CONSTRAINT {shared}_pkey PRIMARY KEY (id)
        )
        """
    )


def _unfold_link_table(link: str, parent: str, shared: str, shared_id: str) -> None:
    op.execute(f"ALTER TABLE {link} ADD COLUMN {shared_id} integer")
    op.execute(f"UPDATE {link} l SET {shared_id} = s.id FROM {shared} s WHERE s.identifier = l.identifier")
    op.execute(f"ALTER TABLE {link} DROP CONSTRAINT {link}_pkey")
    op.execute(f"ALTER TABLE {link} DROP COLUMN identifier")
    op.execute(f"ALTER TABLE {link} ALTER COLUMN {shared_id} SET NOT NULL")
    op.execute(f"ALTER TABLE {link} ADD CONSTRAINT {link}_pkey PRIMARY KEY ({parent}, {shared_id})")
    op.execute(
        f"ALTER TABLE {link} ADD CONSTRAINT {link}_{shared_id}_fkey FOREIGN KEY ({shared_id}) REFERENCES {shared}(id)"
    )


def upgrade() -> None:
    for link, parent, shared, shared_id in DOI_LINKS:
        _fold_link_table(link, parent, shared, shared_id, normalize="upper({})")
    for link, parent, shared, shared_id in RAW_READ_LINKS:
        _fold_link_table(link, parent, shared, shared_id, normalize="{}")

    op.execute(
        """
        CREATE TABLE target_gene_external_identifiers (
            target_gene_id integer NOT NULL,
            db_name character varying NOT NULL,
            identifier character varying NOT NULL,
            "offset" integer NOT NULL,
            CONSTRAINT target_gene_external_identifiers_pkey PRIMARY KEY (target_gene_id, db_name),
            CONSTRAINT target_gene_external_identifiers_target_gene_id_fkey
                FOREIGN KEY (target_gene_id) REFERENCES target_genes(id) ON DELETE CASCADE,
            CONSTRAINT ck_target_gene_external_identifiers_db_name_valid
                CHECK (db_name IN ('Ensembl', 'RefSeq', 'UniProt'))
        )
        """
    )
    for db_name, offsets, identifiers, _ in GENE_IDENTIFIERS:
        op.execute(
            f"""
            INSERT INTO target_gene_external_identifiers (target_gene_id, db_name, identifier, "offset")
            SELECT o.target_gene_id, '{db_name}', i.identifier, o."offset"
              FROM {offsets} o JOIN {identifiers} i ON i.id = o.identifier_id
            """
        )
        op.execute(f"DROP TABLE {offsets}")
        op.execute(f"DROP TABLE {identifiers}")

    op.execute("DROP TABLE doi_identifiers")
    op.execute("DROP TABLE sra_identifiers")


def downgrade() -> None:
    for shared, links, db_name, url_prefix in [
        ("doi_identifiers", DOI_LINKS, "DOI", "https://doi.org/"),
        ("sra_identifiers", RAW_READ_LINKS, "SRA", "http://www.ebi.ac.uk/ena/data/view/"),
    ]:
        _create_shared_table(shared)
        every_identifier = " UNION ".join(f"SELECT identifier FROM {link}" for link, *_ in links)
        op.execute(
            f"""
            INSERT INTO {shared} (identifier, db_name, url, creation_date, modification_date)
            SELECT identifier, '{db_name}', '{url_prefix}' || identifier, current_date, current_date
              FROM ({every_identifier}) AS every_identifier
             ORDER BY identifier
            """
        )
        for link, parent, _, shared_id in links:
            _unfold_link_table(link, parent, shared, shared_id)

    for db_name, offsets, identifiers, with_reference_html in GENE_IDENTIFIERS:
        _create_shared_table(identifiers, with_reference_html)
        op.execute(
            f"""
            INSERT INTO {identifiers} (identifier, db_name, creation_date, modification_date)
            SELECT DISTINCT identifier, db_name, current_date, current_date
              FROM target_gene_external_identifiers WHERE db_name = '{db_name}'
             ORDER BY identifier
            """
        )
        op.execute(
            f"""
            CREATE TABLE {offsets} (
                identifier_id integer NOT NULL,
                target_gene_id integer NOT NULL,
                "offset" integer NOT NULL,
                CONSTRAINT {offsets}_pkey PRIMARY KEY (identifier_id, target_gene_id),
                CONSTRAINT {offsets}_identifier_id_fkey FOREIGN KEY (identifier_id) REFERENCES {identifiers}(id),
                CONSTRAINT {offsets}_target_gene_id_fkey FOREIGN KEY (target_gene_id) REFERENCES target_genes(id)
            )
            """
        )
        op.execute(
            f"""
            INSERT INTO {offsets} (identifier_id, target_gene_id, "offset")
            SELECT i.id, t.target_gene_id, t."offset"
              FROM target_gene_external_identifiers t
              JOIN {identifiers} i ON i.identifier = t.identifier
             WHERE t.db_name = '{db_name}'
            """
        )

    op.execute("DROP TABLE target_gene_external_identifiers")
