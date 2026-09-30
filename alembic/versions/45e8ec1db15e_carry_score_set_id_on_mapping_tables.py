"""carry score_set_id on mapping_records and mapping_record_alleles

Revision ID: 45e8ec1db15e
Revises: d1c9b7a3e2f4
Create Date: 2026-09-28

Adds a ``score_set_id`` column to ``mapping_records`` and ``mapping_record_alleles`` so an RLS policy can
check a row's score set directly (#833). Both tables otherwise reach their score set only through
``variants``, and delegating through a table that size costs ~700 ms per lookup.

Each column sits under a composite foreign key to its parent's id and score set, so it can't disagree
with the variant it belongs to. The composite keys replace the single-column ``variant_id`` and
``mapping_record_id`` foreign keys. Both cascade on delete, so deleting a variant (on re-upload or score set
deletion) removes its mapping records and their allele links.

**Before deploying to production**, build the ``variants`` index by hand, since a migration can't build it
concurrently (#877). The ``IF NOT EXISTS`` below then does nothing there, and builds the index on an empty
or small database::

    CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_variants_id_scoreset ON variants (id, scoreset_id);

The backfills are sized for staging, where the mapping tables already hold data. In production the tables
are empty when this runs.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "45e8ec1db15e"
down_revision = "d1c9b7a3e2f4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_variants_id_scoreset ON variants (id, scoreset_id)")

    op.execute("ALTER TABLE mapping_records ADD COLUMN score_set_id integer")
    op.execute("UPDATE mapping_records mr SET score_set_id = v.scoreset_id FROM variants v WHERE v.id = mr.variant_id")
    op.execute("ALTER TABLE mapping_records ALTER COLUMN score_set_id SET NOT NULL")
    op.execute("CREATE UNIQUE INDEX uq_mapping_records_id_score_set ON mapping_records (id, score_set_id)")
    op.drop_constraint("fk_mapping_records_variant_id", "mapping_records", type_="foreignkey")
    op.create_foreign_key(
        "fk_mapping_records_variant_score_set",
        "mapping_records",
        "variants",
        ["variant_id", "score_set_id"],
        ["id", "scoreset_id"],
        ondelete="CASCADE",
    )

    op.execute("ALTER TABLE mapping_record_alleles ADD COLUMN score_set_id integer")
    op.execute(
        "UPDATE mapping_record_alleles mra SET score_set_id = mr.score_set_id "
        "FROM mapping_records mr WHERE mr.id = mra.mapping_record_id"
    )
    op.execute("ALTER TABLE mapping_record_alleles ALTER COLUMN score_set_id SET NOT NULL")
    op.drop_constraint("fk_mapping_record_alleles_mapping_record_id", "mapping_record_alleles", type_="foreignkey")
    op.create_foreign_key(
        "fk_mapping_record_alleles_mapping_record_score_set",
        "mapping_record_alleles",
        "mapping_records",
        ["mapping_record_id", "score_set_id"],
        ["id", "score_set_id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_mapping_record_alleles_mapping_record_score_set", "mapping_record_alleles", type_="foreignkey"
    )
    op.create_foreign_key(
        "fk_mapping_record_alleles_mapping_record_id",
        "mapping_record_alleles",
        "mapping_records",
        ["mapping_record_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_column("mapping_record_alleles", "score_set_id")

    op.drop_constraint("fk_mapping_records_variant_score_set", "mapping_records", type_="foreignkey")
    op.create_foreign_key(
        "fk_mapping_records_variant_id",
        "mapping_records",
        "variants",
        ["variant_id"],
        ["id"],
    )
    op.drop_index("uq_mapping_records_id_score_set", table_name="mapping_records")
    op.drop_column("mapping_records", "score_set_id")

    op.drop_index("uq_variants_id_scoreset", table_name="variants")
