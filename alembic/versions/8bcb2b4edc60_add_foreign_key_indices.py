"""Add foreign key indices

Revision ID: 8bcb2b4edc60
Revises: 8e26f1a1160d
Create Date: 2024-05-14 22:36:47.095490

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "8bcb2b4edc60"
down_revision = "8e26f1a1160d"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(op.f("ix_access_keys_user_id"), "access_keys", ["user_id"], unique=False)
    op.drop_index("ix_doi_identifiers_id", table_name="doi_identifiers")
    op.drop_index("ix_ensembl_identifiers_id", table_name="ensembl_identifiers")
    op.drop_index("ix_experiment_sets_id", table_name="experiment_sets")
    op.create_index(op.f("ix_experiment_sets_created_by_id"), "experiment_sets", ["created_by_id"], unique=False)
    op.create_index(op.f("ix_experiment_sets_modified_by_id"), "experiment_sets", ["modified_by_id"], unique=False)
    op.drop_index("ix_experiments_id", table_name="experiments")
    op.create_index(op.f("ix_experiments_created_by_id"), "experiments", ["created_by_id"], unique=False)
    op.create_index(op.f("ix_experiments_experiment_set_id"), "experiments", ["experiment_set_id"], unique=False)
    op.create_index(op.f("ix_experiments_modified_by_id"), "experiments", ["modified_by_id"], unique=False)
    op.drop_index("ix_genome_identifiers_id", table_name="genome_identifiers")
    op.drop_index("ix_keywords_id", table_name="keywords")
    op.drop_index("ix_licenses_id", table_name="licenses")
    op.drop_index("ix_mapped_variants_id", table_name="mapped_variants")
    op.create_index(op.f("ix_mapped_variants_variant_id"), "mapped_variants", ["variant_id"], unique=False)
    op.drop_index("ix_publication_identifiers_id", table_name="publication_identifiers")
    op.drop_index("ix_refseq_identifiers_id", table_name="refseq_identifiers")
    op.drop_index("ix_roles_name", table_name="roles")
    op.drop_index("ix_scoresets_id", table_name="scoresets")
    op.create_index(op.f("ix_scoresets_created_by_id"), "scoresets", ["created_by_id"], unique=False)
    op.create_index(op.f("ix_scoresets_experiment_id"), "scoresets", ["experiment_id"], unique=False)
    op.create_index(op.f("ix_scoresets_licence_id"), "scoresets", ["licence_id"], unique=False)
    op.create_index(op.f("ix_scoresets_modified_by_id"), "scoresets", ["modified_by_id"], unique=False)
    op.create_index(op.f("ix_scoresets_replaces_id"), "scoresets", ["replaces_id"], unique=False)
    op.drop_index("ix_sra_identifiers_id", table_name="sra_identifiers")
    op.drop_index("ix_target_genes_id", table_name="target_genes")
    op.create_index(op.f("ix_target_genes_accession_id"), "target_genes", ["accession_id"], unique=False)
    op.create_index(op.f("ix_target_genes_scoreset_id"), "target_genes", ["scoreset_id"], unique=False)
    op.create_index(op.f("ix_target_genes_target_sequence_id"), "target_genes", ["target_sequence_id"], unique=False)
    op.drop_index("ix_target_sequences_id", table_name="target_sequences")
    op.create_index(op.f("ix_target_sequences_taxonomy_id"), "target_sequences", ["taxonomy_id"], unique=False)
    op.drop_index("ix_taxonomies_id", table_name="taxonomies")
    op.create_index(op.f("ix_taxonomies_genome_identifier_id"), "taxonomies", ["genome_identifier_id"], unique=False)
    op.drop_index("ix_uniprot_identifiers_id", table_name="uniprot_identifiers")
    op.drop_index("ix_variants_id", table_name="variants")
    op.create_index(op.f("ix_variants_scoreset_id"), "variants", ["scoreset_id"], unique=False)


def downgrade():
    op.drop_index(op.f("ix_variants_scoreset_id"), table_name="variants")
    op.create_index("ix_variants_id", "variants", ["id"], unique=False)
    op.create_index("ix_uniprot_identifiers_id", "uniprot_identifiers", ["id"], unique=False)
    op.drop_index(op.f("ix_taxonomies_genome_identifier_id"), table_name="taxonomies")
    op.create_index("ix_taxonomies_id", "taxonomies", ["id"], unique=False)
    op.drop_index(op.f("ix_target_sequences_taxonomy_id"), table_name="target_sequences")
    op.create_index("ix_target_sequences_id", "target_sequences", ["id"], unique=False)
    op.drop_index(op.f("ix_target_genes_target_sequence_id"), table_name="target_genes")
    op.drop_index(op.f("ix_target_genes_scoreset_id"), table_name="target_genes")
    op.drop_index(op.f("ix_target_genes_accession_id"), table_name="target_genes")
    op.create_index("ix_target_genes_id", "target_genes", ["id"], unique=False)
    op.create_index("ix_sra_identifiers_id", "sra_identifiers", ["id"], unique=False)
    op.drop_index(op.f("ix_scoresets_replaces_id"), table_name="scoresets")
    op.drop_index(op.f("ix_scoresets_modified_by_id"), table_name="scoresets")
    op.drop_index(op.f("ix_scoresets_licence_id"), table_name="scoresets")
    op.drop_index(op.f("ix_scoresets_experiment_id"), table_name="scoresets")
    op.drop_index(op.f("ix_scoresets_created_by_id"), table_name="scoresets")
    op.create_index("ix_scoresets_id", "scoresets", ["id"], unique=False)
    op.create_index("ix_roles_name", "roles", ["name"], unique=False)
    op.create_index("ix_refseq_identifiers_id", "refseq_identifiers", ["id"], unique=False)
    op.create_index("ix_publication_identifiers_id", "publication_identifiers", ["id"], unique=False)
    op.drop_index(op.f("ix_mapped_variants_variant_id"), table_name="mapped_variants")
    op.create_index("ix_mapped_variants_id", "mapped_variants", ["id"], unique=False)
    op.create_index("ix_licenses_id", "licenses", ["id"], unique=False)
    op.create_index("ix_keywords_id", "keywords", ["id"], unique=False)
    op.create_index("ix_genome_identifiers_id", "genome_identifiers", ["id"], unique=False)
    op.drop_index(op.f("ix_experiments_modified_by_id"), table_name="experiments")
    op.drop_index(op.f("ix_experiments_experiment_set_id"), table_name="experiments")
    op.drop_index(op.f("ix_experiments_created_by_id"), table_name="experiments")
    op.create_index("ix_experiments_id", "experiments", ["id"], unique=False)
    op.drop_index(op.f("ix_experiment_sets_modified_by_id"), table_name="experiment_sets")
    op.drop_index(op.f("ix_experiment_sets_created_by_id"), table_name="experiment_sets")
    op.create_index("ix_experiment_sets_id", "experiment_sets", ["id"], unique=False)
    op.create_index("ix_ensembl_identifiers_id", "ensembl_identifiers", ["id"], unique=False)
    op.create_index("ix_doi_identifiers_id", "doi_identifiers", ["id"], unique=False)
    op.drop_index(op.f("ix_access_keys_user_id"), table_name="access_keys")
