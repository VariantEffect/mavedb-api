"""cite publications by (db_name, identifier)

Revision ID: d0c23896b0c5
Revises: 2dfdc4a88870
Create Date: 2026-10-23

Citation rows referenced a publication by its numeric id, so writing one meant reading the publication row first.
Under row-level security (#808) a publication is visible only through a citation the caller can see, so that read
fails for a publication cited only by others' private records, and for a brand-new one. Citations now reference
the publication by its natural key instead (#945):

- ``publication_identifiers`` gets a unique ``(db_name, identifier)``. There are no duplicates today.
- The score set, experiment and calibration citation tables replace ``publication_identifier_id`` with
  ``(db_name, identifier)``, a foreign key to that unique key. Foreign key checks ignore row security, so a
  citation of a publication the caller can't see still works. Each gets an index on the key, which the
  publication read policy probes.
- ``experiment_set_publication_identifiers`` is dropped. Nothing has ever written to it, and it holds no rows.

``publication_identifiers.id`` stays as the primary key, so the downgrade restores the id references exactly. It
recreates the experiment set table empty.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d0c23896b0c5"
down_revision = "2dfdc4a88870"
branch_labels = None
depends_on = None

# (citation table, primary key columns with {key} where the publication goes, the baseline's id foreign key name)
CITATIONS = [
    (
        "scoreset_publication_identifiers",
        "scoreset_id, {key}",
        "scoreset_pub_identifiers_publication_identifier_id_fkey",
    ),
    (
        "experiment_publication_identifiers",
        "experiment_id, {key}",
        "experiment_pub_identifiers_publication_identifier_id_fkey",
    ),
    (
        "score_calibration_publication_identifiers",
        "score_calibration_id, {key}, relation",
        "score_calibration_publication_id_publication_identifier_id_fkey",
    ),
]


def upgrade() -> None:
    op.execute(
        "ALTER TABLE publication_identifiers "
        "ADD CONSTRAINT uq_publication_identifiers_db_name_identifier UNIQUE (db_name, identifier)"
    )

    for citation, primary_key, _ in CITATIONS:
        op.execute(
            f"ALTER TABLE {citation} ADD COLUMN db_name character varying, ADD COLUMN identifier character varying"
        )
        op.execute(
            f"UPDATE {citation} c SET db_name = p.db_name, identifier = p.identifier "
            f"FROM publication_identifiers p WHERE p.id = c.publication_identifier_id"
        )
        op.execute(f"ALTER TABLE {citation} DROP CONSTRAINT {citation}_pkey")
        op.execute(f"ALTER TABLE {citation} DROP COLUMN publication_identifier_id")
        op.execute(f"ALTER TABLE {citation} ALTER COLUMN db_name SET NOT NULL, ALTER COLUMN identifier SET NOT NULL")
        op.execute(
            f"ALTER TABLE {citation} ADD CONSTRAINT {citation}_pkey "
            f"PRIMARY KEY ({primary_key.format(key='db_name, identifier')})"
        )
        op.execute(
            f"ALTER TABLE {citation} ADD CONSTRAINT fk_{citation}_publication FOREIGN KEY (db_name, identifier) "
            f"REFERENCES publication_identifiers (db_name, identifier)"
        )
        op.execute(f"CREATE INDEX ix_{citation}_publication ON {citation} (db_name, identifier)")

    # Refuse rather than drop citations, in case something has written one since.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM experiment_set_publication_identifiers) THEN
                RAISE EXCEPTION 'experiment_set_publication_identifiers has rows; move them before dropping it';
            END IF;
        END $$
        """
    )
    op.execute("DROP TABLE experiment_set_publication_identifiers")


def downgrade() -> None:
    op.execute(
        """
        CREATE TABLE experiment_set_publication_identifiers (
            experiment_set_id integer NOT NULL,
            publication_identifier_id integer NOT NULL,
            CONSTRAINT experiment_set_publication_identifiers_pkey
                PRIMARY KEY (experiment_set_id, publication_identifier_id),
            CONSTRAINT experiment_set_pub_identifiers_experiment_set_id_fkey
                FOREIGN KEY (experiment_set_id) REFERENCES experiment_sets(id),
            CONSTRAINT experiment_set_pub_identifiers_publication_identifier_id_fkey
                FOREIGN KEY (publication_identifier_id) REFERENCES publication_identifiers(id)
        )
        """
    )

    for citation, primary_key, id_fkey in CITATIONS:
        op.execute(f"ALTER TABLE {citation} ADD COLUMN publication_identifier_id integer")
        op.execute(
            f"UPDATE {citation} c SET publication_identifier_id = p.id FROM publication_identifiers p "
            f"WHERE p.db_name = c.db_name AND p.identifier = c.identifier"
        )
        op.execute(f"ALTER TABLE {citation} DROP CONSTRAINT {citation}_pkey")
        # Drops the key's foreign key and index with it.
        op.execute(f"ALTER TABLE {citation} DROP COLUMN db_name, DROP COLUMN identifier")
        op.execute(f"ALTER TABLE {citation} ALTER COLUMN publication_identifier_id SET NOT NULL")
        op.execute(
            f"ALTER TABLE {citation} ADD CONSTRAINT {citation}_pkey "
            f"PRIMARY KEY ({primary_key.format(key='publication_identifier_id')})"
        )
        op.execute(
            f"ALTER TABLE {citation} ADD CONSTRAINT {id_fkey} FOREIGN KEY (publication_identifier_id) "
            f"REFERENCES publication_identifiers(id)"
        )

    op.execute("ALTER TABLE publication_identifiers DROP CONSTRAINT uq_publication_identifiers_db_name_identifier")
