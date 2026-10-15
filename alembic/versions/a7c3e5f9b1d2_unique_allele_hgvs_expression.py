"""enforce one allele per HGVS expression

Revision ID: a7c3e5f9b1d2
Revises: 024370c4bca7
Create Date: 2026-09-30

Adds a unique index on the allele's HGVS expression (whichever of hgvs_g / hgvs_c / hgvs_p is set; the
model guarantees exactly one). An allele digest covers the refget of the sequence the allele sits on,
and alleles deduplicate on that digest. The same expression under two digests therefore means two
writers built the allele on different sequences (this happened with NM_007294.3 and NP_001346945.1 when
the mapper and reverse translation resolved one accession to different sequences), and the copies never
merge: reverse translation's fold-in misses the measured allele and the convergent / projection
labelling breaks. Nothing else detects it, because both digests are internally consistent. This index
moves the failure to write time, in get_or_create_allele, which raises AlleleIdentityConflictError with
both digests, both refgets and how to debug it.

Built CONCURRENTLY: mapping and reverse translation write to alleles, and a plain CREATE UNIQUE INDEX
would block them. A concurrent build that fails leaves an INVALID index behind, which must be dropped
before retrying:

    DROP INDEX CONCURRENTLY IF EXISTS uq_alleles_hgvs;

Assumes no two alleles share an HGVS expression. Check first:

    SELECT coalesce(hgvs_g, hgvs_c, hgvs_p) AS hgvs, count(*), array_agg(id ORDER BY id)
    FROM alleles
    GROUP BY 1 HAVING count(*) > 1;

If that returns rows, remove the duplicates before applying this migration. In each group keep the
allele whose refget is the one SeqRepo holds for its accession. Delete the other allele and the rows
that reference it (mapping_record_alleles, gnomad_allele_links, vep_allele_consequences,
clinvar_allele_links, annotation_event; all ON DELETE RESTRICT), then remap the affected score sets.
Delete before remapping: get_or_create_allele refuses to create an allele while another holds its HGVS.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a7c3e5f9b1d2"
down_revision = "024370c4bca7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            "uq_alleles_hgvs",
            "alleles",
            [sa.text("(coalesce(hgvs_g, hgvs_c, hgvs_p))")],
            unique=True,
            postgresql_concurrently=True,
        )
    op.execute(
        "COMMENT ON INDEX uq_alleles_hgvs IS "
        "'One allele per HGVS expression. A violation means two writers disagree about the sequence an "
        "allele sits on (see mavedb.lib.variant_translations.AlleleIdentityConflictError).'"
    )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index("uq_alleles_hgvs", table_name="alleles", postgresql_concurrently=True)
