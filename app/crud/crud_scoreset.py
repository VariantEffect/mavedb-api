from app.crud.base import CrudBase
from app.models.scoreset import Scoreset
from app.view_models.scoreset import ScoresetCreate, ScoresetUpdate


class CrudScoreset(CrudBase[Scoreset, ScoresetCreate, ScoresetUpdate]):
    ...


scoreset = CrudScoreset(Scoreset)
