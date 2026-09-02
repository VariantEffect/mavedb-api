"""add vep_allele_consequences table

Revision ID: a7c4e9d2f1b8
Revises: e5f7a9c1b3d4
Create Date: 2026-06-22

New valid-time table holding a deduplicated allele's VEP functional consequence, replacing the frozen
vep_functional_consequence/vep_access_date columns on mapped_variants for new-model writes (Step 2 of
the annotation infrastructure migration, docs/design/annotation-infrastructure-migration.md). A row is
live while valid_to is NULL; the partial unique index enforces a single live consequence per allele
(VEP's most-severe consequence is one current value, so a change supersedes rather than accumulates).
functional_consequence is nullable (reserved for a future negative cache). source_version is the
Ensembl release the consequence was resolved under (coordinated software + transcript set + vocabulary),
which version-keys the refresh skip like gnomAD's db_version; access_date is retained as a "last
confirmed" audit stamp. The VEP columns on mapped_variants are left untouched (frozen serving).
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a7c4e9d2f1b8"
down_revision = "e5f7a9c1b3d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "vep_allele_consequences",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("allele_id", sa.Integer(), nullable=False),
        sa.Column("functional_consequence", sa.String(), nullable=True),
        sa.Column("source_version", sa.String(), nullable=False),
        sa.Column("access_date", sa.Date(), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["allele_id"],
            ["alleles.id"],
            name="fk_vep_allele_consequences_allele_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_vep_allele_consequences_allele_id",
        "vep_allele_consequences",
        ["allele_id"],
    )
    # One live consequence per allele: VEP's most-severe consequence is a single current value, so a
    # changed result supersedes the prior row rather than accumulating one live row per access.
    op.create_index(
        "uq_vep_allele_consequences_live",
        "vep_allele_consequences",
        ["allele_id"],
        unique=True,
        postgresql_where=sa.text("valid_to IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_vep_allele_consequences_live", table_name="vep_allele_consequences")
    op.drop_index("ix_vep_allele_consequences_allele_id", table_name="vep_allele_consequences")
    op.drop_table("vep_allele_consequences")
