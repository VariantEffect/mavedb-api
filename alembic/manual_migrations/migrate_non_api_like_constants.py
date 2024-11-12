import sqlalchemy as sa
from sqlalchemy.orm import Session, configure_mappers

from mavedb.models import *
from mavedb.models.enums.target_category import TargetCategory
from mavedb.models.target_gene import TargetGene

from mavedb.db.session import SessionLocal

configure_mappers()

def api_like_target_gene_category(category: str):
    if category == "Protein coding":
        return TargetCategory.protein_coding
    elif category == "Other noncoding":
        return TargetCategory.other_noncoding
    elif category == "Regulatory":
        return TargetCategory.regulatory
    else:
        raise ValueError()


def do_migration(db: Session):
    target_genes = db.scalars(sa.select(TargetGene)).all()

    for target in target_genes:
        target.category = api_like_target_gene_category(target.category)
        db.add(target)

    db.commit()


if __name__ == "__main__":
    db = SessionLocal()
    db.current_user = None  # type: ignore

    do_migration(db)

    db.commit()
    db.close()
