from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing import Any

from mavedb.models.contributor import Contributor


def add_contributor(db: Session, urn: str, model: Any, orcid_id: str, given_name: str, family_name: str) -> None:
    """Without making an API call, add a new contributor to the record (experiment or score set) with given urn and model."""
    item = db.query(model).filter(model.urn == urn).one_or_none()
    assert item is not None

    try:
        contributor = db.execute(select(Contributor).where(Contributor.orcid_id == orcid_id)).one()
    except NoResultFound:
        contributor = Contributor(orcid_id=orcid_id, given_name=given_name, family_name=family_name)
        db.add(contributor)

    item.contributors = [contributor]
    db.add(item)
    db.commit()
