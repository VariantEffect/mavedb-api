import logging
from operator import or_
from typing import Optional

from sqlalchemy.orm import Session

from mavedb.models.experiment import Experiment
from mavedb.models.scoreset import Scoreset
from mavedb.models.user import User
from mavedb.view_models.search import ExperimentsSearch

logger = logging.getLogger(__name__)


def search_experiments(db: Session, owner: Optional[User], search: ExperimentsSearch) -> list[Scoreset]:
    query = db.query(Experiment)
    # .filter(Scoreset.private.is_(False))

    if owner is not None:
        query = query.filter(Experiment.created_by_id == owner.id)

    if search.published is not None:
        if search.published:
            query = query.filter(Experiment.published_date is not None)
        else:
            query = query.filter(Experiment.published_date is None)

    if search.text:
        lower_search_text = search.text.lower()
        query = query.filter(
            or_(
                Experiment.urn.contains(lower_search_text),
                Experiment.title.contains(lower_search_text),
                Experiment.short_description.contains(lower_search_text),
                Experiment.abstract_text.contains(lower_search_text),
            )
        )

    items: list[Experiment] = query.order_by(Experiment.title).all()
    if not items:
        items = []
    return items
