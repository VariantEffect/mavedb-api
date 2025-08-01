from sqlalchemy.orm import Session
from mavedb.models.license import License
from mavedb.models.score_set import ScoreSet


def change_to_inactive_license(db: Session, urn: str) -> None:
    """Change the license of the score set with given urn to an inactive license."""
    item = db.query(ScoreSet).filter(ScoreSet.urn == urn).one_or_none()
    assert item is not None

    license = db.query(License).filter(License.active.is_(False)).first()
    assert license is not None

    item.license_id = license.id
    db.add(item)
    db.commit()
