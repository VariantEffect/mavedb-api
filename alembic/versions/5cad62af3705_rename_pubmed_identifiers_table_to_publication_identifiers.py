"""rename pubmed_identifiers table to publication_identifiers

Revision ID: 5cad62af3705
Revises: 988ca84c701b
Create Date: 2023-05-09 16:18:41.360541

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5cad62af3705"
down_revision = "f11fd758436e"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint(
        constraint_name="experiment_pubmed_identifiers_pubmed_identifier_id_fkey",
        table_name="experiment_pubmed_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="experiment_set_pubmed_identifiers_pubmed_identifier_id_fkey",
        table_name="experiment_set_pubmed_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="scoreset_pubmed_identifiers_pubmed_identifier_id_fkey",
        table_name="scoreset_pubmed_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="experiment_pubmed_identifiers_experiment_id_fkey",
        table_name="experiment_pubmed_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="experiment_set_pubmed_identifiers_experiment_set_id_fkey",
        table_name="experiment_set_pubmed_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="scoreset_pubmed_identifiers_scoreset_id_fkey",
        table_name="scoreset_pubmed_identifiers",
        type_="foreignkey",
    )

    op.rename_table("pubmed_identifiers", "publication_identifiers")

    op.execute("ALTER SEQUENCE pubmed_identifiers_id_seq RENAME TO publication_identifiers_id_seq")
    op.execute("ALTER INDEX pubmed_identifiers_pkey RENAME TO publication_identifiers_pkey")
    op.execute("ALTER INDEX ix_pubmed_identifiers_id RENAME TO ix_publication_identifiers_id")

    op.rename_table("experiment_pubmed_identifiers", "experiment_publication_identifiers")
    op.rename_table("experiment_set_pubmed_identifiers", "experiment_set_publication_identifiers")
    op.rename_table("scoreset_pubmed_identifiers", "scoreset_publication_identifiers")

    op.alter_column(
        "experiment_publication_identifiers",
        "pubmed_identifier_id",
        nullable=False,
        new_column_name="publication_identifier_id",
    )
    op.alter_column(
        "experiment_set_publication_identifiers",
        "pubmed_identifier_id",
        nullable=False,
        new_column_name="publication_identifier_id",
    )
    op.alter_column(
        "scoreset_publication_identifiers",
        "pubmed_identifier_id",
        nullable=False,
        new_column_name="publication_identifier_id",
    )

    op.execute("ALTER INDEX experiment_pubmed_identifiers_pkey RENAME TO experiment_publication_identifiers_pkey")
    op.execute(
        "ALTER INDEX experiment_set_pubmed_identifiers_pkey RENAME TO experiment_set_publication_identifiers_pkey"
    )
    op.execute("ALTER INDEX scoreset_pubmed_identifiers_pkey RENAME TO scoreset_publication_identifiers_pkey")

    # If we follow our naming conventions, the next two constraint names would exceed an
    # internal postgres 63 char limit. This value is technically configurable if we edit
    # and recompile our psql source code, but it's much easier to just skirt our naming
    # conventions in the case of publication identifiers. -capodb 10.5.2023
    op.create_foreign_key(
        constraint_name="experiment_pub_identifiers_publication_identifier_id_fkey",
        source_table="experiment_publication_identifiers",
        referent_table="publication_identifiers",
        local_cols=["publication_identifier_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="experiment_set_pub_identifiers_publication_identifier_id_fkey",
        source_table="experiment_set_publication_identifiers",
        referent_table="publication_identifiers",
        local_cols=["publication_identifier_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="scoreset_pub_identifiers_publication_identifier_id_fkey",
        source_table="scoreset_publication_identifiers",
        referent_table="publication_identifiers",
        local_cols=["publication_identifier_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="experiment_pub_identifiers_experiment_id_fkey",
        source_table="experiment_publication_identifiers",
        referent_table="experiments",
        local_cols=["experiment_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="experiment_set_pub_identifiers_experiment_set_id_fkey",
        source_table="experiment_set_publication_identifiers",
        referent_table="experiment_sets",
        local_cols=["experiment_set_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="scoreset_pub_identifiers_scoreset_id_fkey",
        source_table="scoreset_publication_identifiers",
        referent_table="scoresets",
        local_cols=["scoreset_id"],
        remote_cols=["id"],
    )


def downgrade():
    op.drop_constraint(
        constraint_name="experiment_pub_identifiers_publication_identifier_id_fkey",
        table_name="experiment_publication_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="experiment_set_pub_identifiers_publication_identifier_id_fkey",
        table_name="experiment_set_publication_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="scoreset_pub_identifiers_publication_identifier_id_fkey",
        table_name="scoreset_publication_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="experiment_pub_identifiers_experiment_id_fkey",
        table_name="experiment_publication_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="experiment_set_pub_identifiers_experiment_set_id_fkey",
        table_name="experiment_set_publication_identifiers",
        type_="foreignkey",
    )
    op.drop_constraint(
        constraint_name="scoreset_pub_identifiers_scoreset_id_fkey",
        table_name="scoreset_publication_identifiers",
        type_="foreignkey",
    )

    op.rename_table("publication_identifiers", "pubmed_identifiers")

    op.execute("ALTER SEQUENCE publication_identifiers_id_seq RENAME TO pubmed_identifiers_id_seq")
    op.execute("ALTER INDEX publication_identifiers_pkey RENAME TO pubmed_identifiers_pkey")
    op.execute("ALTER INDEX ix_publication_identifiers_id RENAME TO ix_pubmed_identifiers_id")

    op.rename_table("experiment_publication_identifiers", "experiment_pubmed_identifiers")
    op.rename_table("experiment_set_publication_identifiers", "experiment_set_pubmed_identifiers")
    op.rename_table("scoreset_publication_identifiers", "scoreset_pubmed_identifiers")

    op.alter_column(
        "experiment_pubmed_identifiers",
        "publication_identifier_id",
        nullable=False,
        new_column_name="pubmed_identifier_id",
    )
    op.alter_column(
        "experiment_set_pubmed_identifiers",
        "publication_identifier_id",
        nullable=False,
        new_column_name="pubmed_identifier_id",
    )
    op.alter_column(
        "scoreset_pubmed_identifiers",
        "publication_identifier_id",
        nullable=False,
        new_column_name="pubmed_identifier_id",
    )

    op.execute("ALTER INDEX experiment_publication_identifiers_pkey RENAME TO experiment_pubmed_identifiers_pkey")
    op.execute(
        "ALTER INDEX experiment_set_publication_identifiers_pkey RENAME TO experiment_set_pubmed_identifiers_pkey"
    )
    op.execute("ALTER INDEX scoreset_publication_identifiers_pkey RENAME TO scoreset_pubmed_identifiers_pkey")

    op.create_foreign_key(
        constraint_name="experiment_pubmed_identifiers_pubmed_identifier_id_fkey",
        source_table="experiment_pubmed_identifiers",
        referent_table="pubmed_identifiers",
        local_cols=["pubmed_identifier_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="experiment_set_pubmed_identifiers_pubmed_identifier_id_fkey",
        source_table="experiment_set_pubmed_identifiers",
        referent_table="pubmed_identifiers",
        local_cols=["pubmed_identifier_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="scoreset_pubmed_identifiers_pubmed_identifier_id_fkey",
        source_table="scoreset_pubmed_identifiers",
        referent_table="pubmed_identifiers",
        local_cols=["pubmed_identifier_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="experiment_pubmed_identifiers_experiment_id_fkey",
        source_table="experiment_pubmed_identifiers",
        referent_table="experiments",
        local_cols=["experiment_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="experiment_set_pubmed_identifiers_experiment_set_id_fkey",
        source_table="experiment_set_pubmed_identifiers",
        referent_table="experiment_sets",
        local_cols=["experiment_set_id"],
        remote_cols=["id"],
    )
    op.create_foreign_key(
        constraint_name="scoreset_pubmed_identifiers_scoreset_id_fkey",
        source_table="scoreset_pubmed_identifiers",
        referent_table="scoresets",
        local_cols=["scoreset_id"],
        remote_cols=["id"],
    )
