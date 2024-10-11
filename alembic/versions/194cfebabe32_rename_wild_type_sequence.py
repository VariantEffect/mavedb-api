"""Rename Wild Type Sequence

Revision ID: 194cfebabe32
Revises: 44d5c568f64b
Create Date: 2023-08-29 12:48:18.390567

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "194cfebabe32"
down_revision = "44d5c568f64b"
branch_labels = None
depends_on = None


def upgrade():
    op.rename_table("wild_type_sequences", "target_sequences")
    op.alter_column("target_genes", "wt_sequence_id", new_column_name="target_sequence_id")
    op.execute("ALTER SEQUENCE  wild_type_sequences_id_seq RENAME TO target_sequences_id_seq")
    op.execute("ALTER INDEX wild_type_sequences_pkey RENAME TO target_sequences_pkey")
    op.execute("ALTER INDEX ix_wild_type_sequences_id RENAME TO ix_target_sequences_id")


def downgrade():
    op.rename_table("target_sequences", "wild_type_sequences")
    op.alter_column("target_genes", "target_sequence_id", new_column_name="wt_sequence_id")
    op.execute("ALTER SEQUENCE  target_sequences_id_seq RENAME TO wild_type_sequences_id_seq")
    op.execute("ALTER INDEX target_sequences_pkey RENAME TO wild_type_sequences_pkey")
    op.execute("ALTER INDEX ix_target_sequences_id RENAME TO ix_wild_type_sequences_id")
