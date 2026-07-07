"""enforce one live authoritative link per mapping record

Revision ID: f1a3c7e9b2d5
Revises: c9b2a4f8e1d3
Create Date: 2026-07-07

Adds a partial unique index enforcing at most one live authoritative link per mapping record. The
existing uq_mapping_record_alleles_live keys on (mapping_record, allele) and so cannot prevent two
different alleles both being flagged is_authoritative for the same record. The serving layer depends
on there being exactly one: the lean whole-set view (lib/score_set_variants) joins the authoritative
link expecting it 1:1 with the variant, so a second live authoritative link would silently duplicate
the variant and double-count its score in the histogram rather than raise. This index moves that
failure to write time in the mapping job, where the bug actually lives.

Assumes no pre-existing record with two live authoritative links — the invariant is upheld today by
the mapping job's write logic. If this ever runs against data that violates it, the unique index
creation fails and the offending duplicates must be retired first. To check before applying:

    SELECT mapping_record_id, count(*)
    FROM mapping_record_alleles
    WHERE is_authoritative AND valid_to IS NULL
    GROUP BY mapping_record_id HAVING count(*) > 1;
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f1a3c7e9b2d5"
down_revision = "c9b2a4f8e1d3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "uq_mapping_record_alleles_live_authoritative",
        "mapping_record_alleles",
        ["mapping_record_id"],
        unique=True,
        postgresql_where=sa.text("is_authoritative AND valid_to IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_mapping_record_alleles_live_authoritative", table_name="mapping_record_alleles")
