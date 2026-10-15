from typing import Iterable, Optional

from mavedb.lib.permissions import Action, has_permission
from mavedb.lib.score_sets import readable_score_set_urns
from mavedb.lib.types.authentication import UserData
from mavedb.models.collection import Collection
from mavedb.view_models.collection import OfficialCollection


def readable_official_collections(
    user_data: Optional[UserData], collections: Iterable[Collection]
) -> list[OfficialCollection]:
    """Serialize official collections, carrying only what the caller may read.

    An official collection is a badge on every score set and experiment it holds, so it is serialized into
    responses about those records. Collections the caller may not read are dropped, and each remaining
    collection's member URNs are narrowed to the members the caller may read: a public score set can share
    a badge collection with private ones.

    Narrows validated views rather than the ORM collections, whose member relationships would stage writes
    if reassigned.
    """
    narrowed = []
    for collection in collections:
        if not has_permission(user_data, collection, Action.READ).permitted:
            continue

        score_set_urns = readable_score_set_urns(user_data, collection.score_sets)
        experiment_urns = {
            experiment.urn
            for experiment in collection.experiments
            if has_permission(user_data, experiment, Action.READ).permitted
        }

        view = OfficialCollection.model_validate(collection)
        narrowed.append(
            view.model_copy(
                update={
                    "score_set_urns": [urn for urn in view.score_set_urns if urn in score_set_urns],
                    "experiment_urns": [urn for urn in view.experiment_urns if urn in experiment_urns],
                }
            )
        )

    return narrowed
