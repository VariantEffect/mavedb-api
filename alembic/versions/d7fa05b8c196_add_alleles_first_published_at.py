"""add alleles.first_published_at, set by trigger once a public score set links the allele

The ``alleles`` RLS policy shows an allele when a visible mapping link points at it. About 85% of alleles are
linked from a public score set, and for those that ``EXISTS`` is wasted work, costing 3-10x on per-score-set
reads after reverse translation (#832). This column lets the policy skip it.

- It's write-once: it only goes from null to set. Once a public score set links an allele, the allele's
  existence no longer says what anyone is working on. That holds only while publication can't be reversed
  (#893); a change that allows it, such as unpublishing or an embargo, has to revisit this column.
- ``mavedb_mark_alleles_published_by_links`` sets it when links to a public score set are inserted, and
  ``mavedb_mark_alleles_published_by_score_set`` when a score set goes public, for the alleles it already links.
  Links aren't re-pointed after insert (retiring one updates only its ``valid_to``), so inserts are enough.
- The value is the date the linking score set was published.

Both functions are plain, not ``SECURITY DEFINER``. They run as whoever writes the link or publishes, who can see
the alleles involved.

The backfill updates about 1.5M rows in production. It takes row locks only, so reads go on, but stop the worker
during the deploy so it doesn't wait on them.

Revision ID: d7fa05b8c196
Revises: c6e9f4a7b085
Create Date: 2026-11-12

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d7fa05b8c196"
down_revision = "c6e9f4a7b085"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE alleles ADD COLUMN first_published_at date")

    op.execute("""CREATE FUNCTION mavedb_mark_alleles_published_by_links() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE alleles a
       SET first_published_at = coalesce(s.published_date, current_date)
      FROM new_links l
      JOIN scoresets s ON s.id = l.score_set_id
     WHERE a.id = l.allele_id
       AND NOT s.private
       AND a.first_published_at IS NULL;
    RETURN NULL;
END
$$""")
    op.execute(
        "CREATE TRIGGER mapping_record_alleles_mark_alleles_published AFTER INSERT ON mapping_record_alleles "
        "REFERENCING NEW TABLE AS new_links FOR EACH STATEMENT "
        "EXECUTE FUNCTION mavedb_mark_alleles_published_by_links()"
    )

    op.execute("""CREATE FUNCTION mavedb_mark_alleles_published_by_score_set() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE alleles a
       SET first_published_at = coalesce(NEW.published_date, current_date)
      FROM mapping_record_alleles m
     WHERE m.score_set_id = NEW.id
       AND a.id = m.allele_id
       AND a.first_published_at IS NULL;
    RETURN NULL;
END
$$""")
    op.execute(
        "CREATE TRIGGER scoresets_mark_alleles_published AFTER UPDATE OF private ON scoresets "
        "FOR EACH ROW WHEN (OLD.private AND NOT NEW.private) "
        "EXECUTE FUNCTION mavedb_mark_alleles_published_by_score_set()"
    )

    op.execute("""UPDATE alleles a
   SET first_published_at = p.published
  FROM (SELECT m.allele_id, min(coalesce(s.published_date, s.creation_date)) AS published
          FROM mapping_record_alleles m
          JOIN scoresets s ON s.id = m.score_set_id
         WHERE NOT s.private
         GROUP BY m.allele_id) p
 WHERE a.id = p.allele_id""")


def downgrade():
    op.execute("DROP TRIGGER scoresets_mark_alleles_published ON scoresets")
    op.execute("DROP FUNCTION mavedb_mark_alleles_published_by_score_set()")
    op.execute("DROP TRIGGER mapping_record_alleles_mark_alleles_published ON mapping_record_alleles")
    op.execute("DROP FUNCTION mavedb_mark_alleles_published_by_links()")
    op.execute("ALTER TABLE alleles DROP COLUMN first_published_at")
