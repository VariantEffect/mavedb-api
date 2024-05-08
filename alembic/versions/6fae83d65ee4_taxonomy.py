"""taxonomy

Revision ID: 6fae83d65ee4
Revises: fecb3e0d181d
Create Date: 2023-12-21 18:06:18.912925

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6fae83d65ee4"
down_revision = "fecb3e0d181d"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "taxonomies",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("tax_id", sa.Integer(), nullable=False),
        sa.Column("organism_name", sa.String(), nullable=True),
        sa.Column("common_name", sa.String(), nullable=True),
        sa.Column("rank", sa.String(), nullable=True),
        sa.Column("has_described_species_name", sa.Boolean(), nullable=True),
        sa.Column("url", sa.String(), nullable=False),
        sa.Column("article_reference", sa.String(), nullable=True),
        sa.Column("genome_identifier_id", sa.Integer(), nullable=True),
        sa.Column("creation_date", sa.Date(), nullable=False),
        sa.Column("modification_date", sa.Date(), nullable=False),
        sa.ForeignKeyConstraint(
            ["genome_identifier_id"],
            ["genome_identifiers.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_taxonomies_id"), "taxonomies", ["id"], unique=False)
    op.add_column("target_sequences", sa.Column("taxonomy_id", sa.Integer(), nullable=True))
    op.drop_constraint("wild_type_sequences_reference_id_fkey", "target_sequences", type_="foreignkey")
    op.create_foreign_key(
        "target_sequence_taxonomy_id_foreign_key_constraint", "target_sequences", "taxonomies", ["taxonomy_id"], ["id"]
    )
    op.drop_column("target_sequences", "reference_id")
    op.drop_index("ix_reference_genomes_id", table_name="reference_genomes")
    op.drop_table("reference_genomes")
    ## end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "reference_genomes",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("short_name", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("organism_name", sa.VARCHAR(), autoincrement=False, nullable=False),
        sa.Column("genome_identifier_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.Column("creation_date", sa.DATE(), autoincrement=False, nullable=False),
        sa.Column("modification_date", sa.DATE(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ["genome_identifier_id"], ["genome_identifiers.id"], name="reference_genomes_genome_identifier_id_fkey"
        ),
        sa.PrimaryKeyConstraint("id", name="reference_genomes_pkey"),
    )

    op.add_column("target_sequences", sa.Column("reference_id", sa.INTEGER(), autoincrement=False, nullable=True))
    op.drop_constraint("target_sequence_taxonomy_id_foreign_key_constraint", "target_sequences", type_="foreignkey")
    op.create_foreign_key(
        "wild_type_sequences_reference_id_fkey", "target_sequences", "reference_genomes", ["reference_id"], ["id"]
    )
    op.drop_column("target_sequences", "taxonomy_id")

    op.create_index("ix_reference_genomes_id", "reference_genomes", ["id"], unique=False)
    op.drop_index(op.f("ix_taxonomies_id"), table_name="taxonomies")
    op.drop_table("taxonomies")
    # ### end Alembic commands ###
