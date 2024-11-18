import sqlalchemy as sa
from sqlalchemy.orm import Session, configure_mappers

from mavedb.models import *

from mavedb.lib.score_sets import refresh_variant_urns

from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant

from mavedb.db.session import SessionLocal

configure_mappers()


def do_migration(db: Session):
    published_score_sets_with_associated_tmp_variants: sa.ScalarResult[str]
    published_score_sets_with_associated_tmp_variants = db.execute(
        sa.select(sa.distinct(ScoreSet.urn)).join(Variant).where(ScoreSet.published_date.is_not(None), Variant.urn.like("%tmp:%"))
    ).scalars()

    for score_set_urn in published_score_sets_with_associated_tmp_variants:
        refresh_variant_urns(db, db.execute(sa.select(ScoreSet).where(ScoreSet.urn == score_set_urn)).scalar_one())


if __name__ == "__main__":
    db = SessionLocal()
    db.current_user = None  # type: ignore

    do_migration(db)

    db.commit()
    db.close()
