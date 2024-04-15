"""
Script that generates a dump of published MaveDB data.

Usage:
```
python3 -m mavedb.scripts.export_public_data
```

This generates a ZIP archive named `mavedb-dump.zip` in the working directory. the ZIP file has the following contents:
- main.json: A JSON file providing metadata for all of the published experiment sets, experiments, and score sets
- variants/
  - [URN].counts.csv (for each variant URN): The score set's variant count columns,
    sorted by variant number
  - [URN].scores.csv (for each variant URN): The score set's variant count columns,
    sorted by variant number

In the exported JSON metadata, the root object's `experimentSets` property gives an array of experiment sets.
Experiments are nested in their parent experiment sets, and score sets in their parent experiments.

The variant URNs used in filenames do not include the `urn:mavedb:` scheme identifier, so they look like
`00000001-a-1.counts.csv` and `00000001-a-1.scores.csv`, for instance.

Unpublished data are not included in the dump, and user details are limited to ORCID IDs and names of contributors to
published data sets.
"""

from itertools import chain
import json
import logging
from typing import Callable, Iterable, TypeVar
from zipfile import ZipFile

from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.orm import lazyload

from mavedb.lib.score_sets import get_score_set_counts_as_csv, get_score_set_scores_as_csv
from mavedb.lib.script_environment import init_script_environment
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set import ScoreSet
from mavedb.view_models.experiment_set import ExperimentSetPublicDump

db = init_script_environment()

logger = logging.getLogger(__name__)

S = TypeVar("S")
T = TypeVar("T")


def flatmap(f: Callable[[S], Iterable[T]], items: Iterable[S]) -> Iterable[T]:
    return chain.from_iterable(map(f, items))


experiment_sets_query = db.scalars(
    select(ExperimentSet)
    .where(ExperimentSet.published_date.is_not(None))
    .options(
        lazyload(ExperimentSet.experiments.and_(Experiment.published_date.is_not(None))).options(
            lazyload(Experiment.score_sets.and_(ScoreSet.published_date.is_not(None)))
        )
    )
    .execution_options(populate_existing=True)
)

# TODO To support very large data sets, we may want to use custom code for JSON-encoding an iterator.
# See, for instance, https://stackoverflow.com/questions/12670395/json-encoding-very-long-iterators.

experiment_sets = experiment_sets_query.all()
experiment_set_views = map(lambda es: ExperimentSetPublicDump.from_orm(es), experiment_sets)

# Get a list of IDS of all the score sets included. (There is no flat_map function...)
score_set_ids = flatmap(
    lambda es: flatmap(lambda e: map(lambda ss: ss.id, e.score_sets), es.experiments), experiment_sets
)

zip_file_name = "mavedb-dump.zip"

logger.info(f"Exporting public data set metadata to {zip_file_name}/main.json")
json_data = {"experiment_sets": list(experiment_set_views)}

with ZipFile(zip_file_name, "w") as zipfile:
    zipfile.writestr("main.json", json.dumps(jsonable_encoder(json_data)))
    for score_set_id in score_set_ids:
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one_or_none()
        if score_set is not None and score_set.urn is not None:
            logger.info(f"Exporting variants for score set {score_set.urn}")
            csv_filename_base = score_set.urn.removeprefix("urn:mavedb:").removeprefix("urn:tmp:")

            csv_str = get_score_set_scores_as_csv(db, score_set)
            zipfile.writestr(f"variants/{csv_filename_base}.scores.csv", csv_str)

            csv_str = get_score_set_counts_as_csv(db, score_set)
            zipfile.writestr(f"variants/{csv_filename_base}.counts.csv", csv_str)
