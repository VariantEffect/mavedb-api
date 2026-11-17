"""carry score_set_id on target_sequences and target_accessions, with one target per gene

The api inserts a target sequence or accession, with ``RETURNING id``, before the target gene that points at it,
so a policy reading ``target_genes`` would refuse the insert under RLS (#809). Each row belongs to one target
gene, so it carries that gene's score set instead, and its policy reads ``scoresets``.

- ``score_set_id`` is backfilled from ``target_genes`` and made ``NOT NULL``.
- ``target_genes`` refers to each by ``(id, score_set_id)``, so the carried key can't differ from the gene's.
  These composite foreign keys replace the single-column ones.
- ``target_genes_one_target`` requires exactly one of a sequence and an accession.

Existing rows are repaired first, so the constraints hold:

- A sequence or accession shared by several target genes is copied, so each gene gets its own. The lowest
  target gene id keeps the original. The ORM permits sharing, and deleting one sharer's score set would delete
  the shared row and leave the others with no target.
- A sequence or accession no target gene points at is deleted. Nothing can reach it, and it has no score set to
  carry. The downgrade can't restore these rows or merge the copies.

Revision ID: b5d8e3f6a974
Revises: a4c7d2e5f863
Create Date: 2026-11-12

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b5d8e3f6a974"
down_revision = "a4c7d2e5f863"
branch_labels = None
depends_on = None

# (table, the target_genes column pointing at it, its columns other than id and score_set_id, the foreign key
# it replaces, and the composite one replacing it)
TARGETS = [
    (
        "target_sequences",
        "target_sequence_id",
        "sequence_type, sequence, creation_date, modification_date, label, taxonomy_id",
        "target_genes_wt_sequence_id_fkey",
        "target_genes_target_sequence_score_set_fkey",
    ),
    (
        "target_accessions",
        "accession_id",
        "assembly, accession, creation_date, modification_date, gene, is_base_editor",
        "target_genes_accession_id_fkey",
        "target_genes_accession_score_set_fkey",
    ),
]


def upgrade():
    for table, column, copied, old_fkey, new_fkey in TARGETS:
        op.execute(f"""
DO $$
DECLARE
    gene record;
    copy_id integer;
BEGIN
    FOR gene IN
        SELECT tg.id, tg.{column} AS target_id FROM target_genes tg
         WHERE tg.{column} IS NOT NULL
           AND EXISTS (SELECT 1 FROM target_genes o WHERE o.{column} = tg.{column} AND o.id < tg.id)
         ORDER BY tg.id
    LOOP
        INSERT INTO {table} ({copied}) SELECT {copied} FROM {table} WHERE id = gene.target_id RETURNING id INTO copy_id;
        UPDATE target_genes SET {column} = copy_id WHERE id = gene.id;
    END LOOP;
END
$$""")
        op.execute(f"DELETE FROM {table} t WHERE NOT EXISTS (SELECT 1 FROM target_genes tg WHERE tg.{column} = t.id)")

        op.execute(f"ALTER TABLE {table} ADD COLUMN score_set_id integer")
        op.execute(f"UPDATE {table} t SET score_set_id = tg.scoreset_id FROM target_genes tg WHERE tg.{column} = t.id")
        op.execute(f"ALTER TABLE {table} ALTER COLUMN score_set_id SET NOT NULL")
        op.execute(
            f"ALTER TABLE {table} ADD CONSTRAINT {table}_score_set_id_fkey "
            "FOREIGN KEY (score_set_id) REFERENCES scoresets (id)"
        )
        op.execute(f"CREATE INDEX ix_{table}_score_set_id ON {table} (score_set_id)")
        op.execute(f"ALTER TABLE {table} ADD CONSTRAINT uq_{table}_id_score_set_id UNIQUE (id, score_set_id)")

        op.execute(f"ALTER TABLE target_genes DROP CONSTRAINT {old_fkey}")
        op.execute(
            f"ALTER TABLE target_genes ADD CONSTRAINT {new_fkey} "
            f"FOREIGN KEY ({column}, scoreset_id) REFERENCES {table} (id, score_set_id)"
        )

    op.execute(
        "ALTER TABLE target_genes ADD CONSTRAINT target_genes_one_target "
        "CHECK (num_nonnulls(target_sequence_id, accession_id) = 1)"
    )


def downgrade():
    op.execute("ALTER TABLE target_genes DROP CONSTRAINT target_genes_one_target")
    for table, column, _, old_fkey, new_fkey in reversed(TARGETS):
        op.execute(f"ALTER TABLE target_genes DROP CONSTRAINT {new_fkey}")
        op.execute(f"ALTER TABLE target_genes ADD CONSTRAINT {old_fkey} FOREIGN KEY ({column}) REFERENCES {table} (id)")
        op.execute(f"ALTER TABLE {table} DROP COLUMN score_set_id")
