"""add unique constraint to clinical_controls (db_name, db_identifier, db_version)

Prevents duplicate ClinicalControl rows that can arise when concurrent
refresh_clinvar_controls jobs race on the same (db_name, db_identifier,
db_version) tuple. The upsert in the job code relies on this constraint.

Revision ID: 398067c53257
Revises: 8c4a2f1d9e6b
Create Date: 2026-05-07 14:54:48.633770

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "398067c53257"
down_revision = "8c4a2f1d9e6b"
branch_labels = None
depends_on = None


def upgrade():
    # Re-point mapped_variants_clinical_controls rows that reference a duplicate
    # clinical_control to the surviving (lowest-id) row for that group, so no
    # association links are lost during deduplication.
    #
    # Step 1: Drop association rows that would produce a composite-PK conflict
    # after re-pointing (i.e. the mapped_variant is already linked to the
    # surviving control).
    op.execute(
        """
        WITH survivors AS (
            SELECT db_name, db_identifier, db_version, MIN(id) AS survivor_id
            FROM clinical_controls
            GROUP BY db_name, db_identifier, db_version
            HAVING COUNT(*) > 1
        ),
        duplicates AS (
            SELECT cc.id AS duplicate_id, s.survivor_id
            FROM clinical_controls cc
            JOIN survivors s
                ON  cc.db_name       = s.db_name
                AND cc.db_identifier = s.db_identifier
                AND cc.db_version    = s.db_version
                AND cc.id           != s.survivor_id
        )
        DELETE FROM mapped_variants_clinical_controls mvcc
        USING duplicates d
        WHERE mvcc.clinical_control_id = d.duplicate_id
          AND EXISTS (
              SELECT 1
              FROM mapped_variants_clinical_controls existing
              WHERE existing.mapped_variant_id   = mvcc.mapped_variant_id
                AND existing.clinical_control_id = d.survivor_id
          )
        """
    )

    # Step 2: Re-point remaining association rows from duplicate IDs to the
    # survivor, preserving all mapped_variant links.
    op.execute(
        """
        WITH survivors AS (
            SELECT db_name, db_identifier, db_version, MIN(id) AS survivor_id
            FROM clinical_controls
            GROUP BY db_name, db_identifier, db_version
            HAVING COUNT(*) > 1
        ),
        duplicates AS (
            SELECT cc.id AS duplicate_id, s.survivor_id
            FROM clinical_controls cc
            JOIN survivors s
                ON  cc.db_name       = s.db_name
                AND cc.db_identifier = s.db_identifier
                AND cc.db_version    = s.db_version
                AND cc.id           != s.survivor_id
        )
        UPDATE mapped_variants_clinical_controls mvcc
        SET clinical_control_id = d.survivor_id
        FROM duplicates d
        WHERE mvcc.clinical_control_id = d.duplicate_id
        """
    )

    # Step 3: Delete the now-unlinked duplicate clinical_control rows.
    op.execute(
        """
        DELETE FROM clinical_controls
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM clinical_controls
            GROUP BY db_name, db_identifier, db_version
        )
        """
    )

    # CREATE UNIQUE INDEX CONCURRENTLY must run outside a transaction.
    # autocommit_block() commits the cleanup steps above, then builds the index
    # without holding an AccessExclusiveLock for the full duration.
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
            "uq_clinical_controls_db_name_identifier_version "
            "ON clinical_controls (db_name, db_identifier, db_version)"
        )

    # Promote the index to a named constraint (brief catalog update only).
    op.execute(
        "ALTER TABLE clinical_controls "
        "ADD CONSTRAINT uq_clinical_controls_db_name_identifier_version "
        "UNIQUE USING INDEX uq_clinical_controls_db_name_identifier_version"
    )


def downgrade():
    op.drop_constraint(
        "uq_clinical_controls_db_name_identifier_version",
        "clinical_controls",
        type_="unique",
    )
