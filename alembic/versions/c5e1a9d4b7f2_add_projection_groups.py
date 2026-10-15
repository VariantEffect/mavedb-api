"""add projection groups

Revision ID: c5e1a9d4b7f2
Revises: b4d8f2a6c1e3
Create Date: 2026-10-14

Makes ``mapping_record_alleles.projection_group`` a foreign key to a ``projection_groups`` row that holds
the group's coding, genomic and protein HGVS. A group's levels need not all have an allele: an intronic
coding projection has no VRS representation but is still the variant's coding coordinate.

The composite foreign key on ``(projection_group, mapping_record_id)`` keeps a link inside its own record's
groups. Existing group numbers name no row, so they are cleared; re-run the projection step and reverse
translation to rebuild them. Production has none after the substrate backfill, which writes no groups.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c5e1a9d4b7f2"
down_revision = "b4d8f2a6c1e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "projection_groups",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("mapping_record_id", sa.Integer(), nullable=False),
        sa.Column("score_set_id", sa.Integer(), nullable=False),
        sa.Column("hgvs_c", sa.String(), nullable=True),
        sa.Column("hgvs_g", sa.String(), nullable=True),
        sa.Column("hgvs_p", sa.String(), nullable=True),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["mapping_record_id", "score_set_id"],
            ["mapping_records.id", "mapping_records.score_set_id"],
            name="fk_projection_groups_mapping_record_score_set",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_projection_groups_mapping_record_id", "projection_groups", ["mapping_record_id"])
    # Target of mapping_record_alleles' composite foreign key, which keeps a link inside its own record's groups.
    op.create_index(
        "uq_projection_groups_id_mapping_record", "projection_groups", ["id", "mapping_record_id"], unique=True
    )

    # Existing group numbers name no row.
    mapping_record_alleles = sa.table("mapping_record_alleles", sa.column("projection_group", sa.Integer))
    op.execute(
        mapping_record_alleles.update()
        .where(mapping_record_alleles.c.projection_group.isnot(None))
        .values(projection_group=None)
    )
    op.create_foreign_key(
        "fk_mapping_record_alleles_projection_group",
        "mapping_record_alleles",
        "projection_groups",
        ["projection_group", "mapping_record_id"],
        ["id", "mapping_record_id"],
        ondelete="CASCADE",
    )
    op.create_index("ix_mapping_record_alleles_projection_group", "mapping_record_alleles", ["projection_group"])


def downgrade() -> None:
    op.drop_index("ix_mapping_record_alleles_projection_group", table_name="mapping_record_alleles")
    op.drop_constraint("fk_mapping_record_alleles_projection_group", "mapping_record_alleles", type_="foreignkey")
    op.drop_index("uq_projection_groups_id_mapping_record", table_name="projection_groups")
    op.drop_index("ix_projection_groups_mapping_record_id", table_name="projection_groups")
    op.drop_table("projection_groups")
