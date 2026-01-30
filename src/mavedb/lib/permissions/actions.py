from enum import Enum


class Action(Enum):
    LOOKUP = "lookup"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    ADD_EXPERIMENT = "add_experiment"
    ADD_SCORE_SET = "add_score_set"
    SET_SCORES = "set_scores"
    ADD_ROLE = "add_role"
    PUBLISH = "publish"
    ADD_BADGE = "add_badge"
    CHANGE_RANK = "change_rank"
