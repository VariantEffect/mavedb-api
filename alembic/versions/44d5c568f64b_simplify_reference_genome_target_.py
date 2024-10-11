"""Simplify Reference Genome Target Structure

Revision ID: 44d5c568f64b
Revises: 90e7860964a2
Create Date: 2023-08-24 15:20:01.208691

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "44d5c568f64b"
down_revision = "90e7860964a2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "wild_type_sequences",
        sa.Column("reference_id", sa.Integer, sa.ForeignKey("reference_genomes.id"), nullable=True),
    )
    op.execute(
        """
                UPDATE wild_type_sequences w
                    SET reference_id = l.genome_id
                    FROM
                        (select * from reference_maps inner join target_genes on target_genes.id = reference_maps.target_id) as l
                    WHERE w.id = l.wt_sequence_id
               """
    )

    op.alter_column("wild_type_sequences", "reference_id", nullable=True)
    op.drop_table("reference_maps")


def downgrade():
    op.create_table(
        "reference_maps",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("is_primary", sa.Boolean(), nullable=False),
        sa.Column("genome_id", sa.Integer(), sa.ForeignKey("reference_genomes.id"), nullable=False),
        sa.Column("target_id", sa.Integer(), sa.ForeignKey("target_genes.id"), nullable=False),
        sa.Column("creation_date", sa.Date(), nullable=False),
        sa.Column("modification_date", sa.Date(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute("delete from target_genes where wt_sequence_id is NULL")
    op.execute(
        """
                INSERT INTO reference_maps (is_primary, genome_id, target_id, creation_date, modification_date)
                    SELECT false, wild_type_sequences.reference_id, target_genes.id, current_date, current_date
                    FROM target_genes JOIN wild_type_sequences on target_genes.wt_sequence_id = wild_type_sequences.id
                    WHERE wild_type_sequences.reference_id is not null
                """
    )
    op.execute(
        "delete from reference_maps where target_id in (select id from target_genes where wt_sequence_id is NULL)"
    )
    op.drop_column("wild_type_sequences", "reference_id")
