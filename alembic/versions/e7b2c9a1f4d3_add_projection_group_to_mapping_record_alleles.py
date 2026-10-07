"""add projection_group to mapping_record_alleles

Revision ID: e7b2c9a1f4d3
Revises: f1a3c7e9b2d5
Create Date: 2026-07-08

Adds a nullable ``projection_group`` integer column to ``mapping_record_alleles`` — a within-record
grouping key that pairs the coding and genomic links of one reverse-translation projection pair.
The two members of a pair share a value; the shared protein apex is NULL, as is every row written
before reverse translation runs. See the model and the reverse_translation worker for the full
semantics (per-record index, authoritative fold-in).

No index is added: the per-URN serving fetch is already covered by
``ix_mapping_record_alleles_mapping_record_id`` and a record's link set is small. No backfill:
existing rows stay NULL and re-group lazily on the next re-map / reverse-translation run.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "e7b2c9a1f4d3"
down_revision = "f1a3c7e9b2d5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "mapping_record_alleles",
        sa.Column("projection_group", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("mapping_record_alleles", "projection_group")
