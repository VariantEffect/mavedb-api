import sqlalchemy as sa
from sqlalchemy.orm import Session, configure_mappers

from mavedb.models import *

from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_accession import TargetAccession

from mavedb.db.session import SessionLocal

configure_mappers()


def do_migration(db: Session):
    accession_based_score_sets = db.execute(
        sa.select(ScoreSet).join(TargetGene).where(TargetGene.accession_id.isnot(None))
    ).scalars()

    for score_set in accession_based_score_sets:
        total_targets = len(
            list(db.execute(sa.select(TargetGene).where(TargetGene.score_set_id == score_set.id)).scalars())
        )

        # Variants from score sets with multiple targets are already in the desired format.
        if total_targets > 1:
            continue

        target_accession = db.execute(
            sa.select(TargetAccession.accession).join(TargetGene).where(TargetGene.score_set_id == score_set.id)
        ).scalar()
        variants = db.execute(sa.select(Variant).where(Variant.score_set_id == score_set.id)).scalars()

        if target_accession is None:
            raise ValueError("target accession should never be None.")

        for variant in variants:
            if variant.hgvs_nt:
                variant.hgvs_nt = f"{target_accession}:{variant.hgvs_nt}"
            if variant.hgvs_pro:
                variant.hgvs_pro = f"{target_accession}:{variant.hgvs_pro}"
            if variant.hgvs_splice:
                variant.hgvs_splice = f"{target_accession}:{variant.hgvs_splice}"

            db.add(variant)


if __name__ == "__main__":
    db = SessionLocal()
    db.current_user = None  # type: ignore

    do_migration(db)

    db.commit()
    db.close()
