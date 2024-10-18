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

Unpublished data and data sets licensed other than under the Create Commmons Zero license are not included in the dump,
and user details are limited to ORCID IDs and names of contributors to published data sets.
"""

import json
import logging
import os
from datetime import datetime, timezone
from itertools import chain
from typing import Callable, Iterable, TypeVar
from zipfile import ZipFile

from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.orm import lazyload, Session

from mavedb.lib.score_sets import get_score_set_counts_as_csv, get_score_set_scores_as_csv
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.license import License
from mavedb.models.score_set import ScoreSet
from mavedb.view_models.experiment_set import ExperimentSetPublicDump

from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)

S = TypeVar("S")
T = TypeVar("T")


def filter_experiment_sets(experiment_sets: Iterable[ExperimentSet]) -> Iterable[ExperimentSet]:
    """
    Filter a list of experiment sets. Exclude any experiments with no score sets, then exclude experiment sets with no
    experiments.

    Filtering is done on the basis of the current contents of Experiment.score_set, which will have been loaded using a
    query that excludes unpublished score sets and those licensed other than under CC0.
    """
    return filter(filter_experiment_set, experiment_sets)


def filter_experiment_set(experiment_set: ExperimentSet):
    """
    Filter an experiment set. Exclude any experiments it contains that do not contain score sets, and return a value
    indicating whether any experiments remain.

    Filtering is done on the basis of the current contents of Experiment.score_set, which will have been loaded using a
    query that excludes unpublished score sets and those licensed other than under CC0.
    """
    experiment_set.experiments = list(filter_experiments(experiment_set.experiments))
    return len(experiment_set.experiments) > 0


def filter_experiments(experiments: Iterable[Experiment]) -> Iterable[Experiment]:
    """
    Filter a list of experiments, excluding any whose score_sets collection is empty.

    Filtering is done on the basis of the current contents of score_sets, which will have been loaded using a query that
    excludes unpublished score sets and those licensed other than under CC0.
    """
    return filter(lambda e: len(e.score_sets) > 0, experiments)


def flatmap(f: Callable[[S], Iterable[T]], items: Iterable[S]) -> Iterable[T]:
    return chain.from_iterable(map(f, items))


@script_environment.command()
@with_database_session
def export_public_data(db: Session):
    experiment_sets_query = db.scalars(
        select(ExperimentSet)
        .where(ExperimentSet.published_date.is_not(None))
        .options(
            lazyload(ExperimentSet.experiments.and_(Experiment.published_date.is_not(None))).options(
                lazyload(
                    Experiment.score_sets.and_(
                        ScoreSet.published_date.is_not(None), ScoreSet.license.has(License.short_name == "CC0")
                    )
                )
            )
        )
        .execution_options(populate_existing=True)
        .order_by(ExperimentSet.urn)
    )

    # Filter the stream of experiment sets to exclude experiments and experiment sets with no public, CC0-licensed score
    # sets.
    experiment_sets = list(filter_experiment_sets(experiment_sets_query.all()))

    # TODO To support very large data sets, we may want to use custom code for JSON-encoding an iterator.
    # Issue: https://github.com/VariantEffect/mavedb-api/issues/192
    # See, for instance, https://stackoverflow.com/questions/12670395/json-encoding-very-long-iterators.

    experiment_set_views = list(map(lambda es: ExperimentSetPublicDump.from_orm(es), experiment_sets))

    # Get a list of IDS of all the score sets included.
    score_set_ids = list(
        flatmap(lambda es: flatmap(lambda e: map(lambda ss: ss.id, e.score_sets), es.experiments), experiment_sets)
    )

    timestamp_format = "%Y%m%d%H%M%S"
    zip_file_name = f"mavedb-dump.{datetime.now().strftime(timestamp_format)}.zip"

    logger.info(f"Exporting public data set metadata to {zip_file_name}/main.json")
    json_data = {
        "title": "MaveDB public data",
        "asOf": datetime.now(timezone.utc).isoformat(),
        "experimentSets": experiment_set_views,
    }

    with ZipFile(zip_file_name, "w") as zipfile:
        # Write metadata for all data sets to a single JSON file.
        zipfile.writestr("main.json", json.dumps(jsonable_encoder(json_data)))

        # Copy the CC0 license.
        zipfile.write(os.path.join(os.path.dirname(__file__), "resources/CC0_license.txt"), "LICENSE.txt")

        # Write score and count files for each score set.
        num_score_sets = len(score_set_ids)
        for i, score_set_id in enumerate(score_set_ids):
            score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one_or_none()
            if score_set is not None and score_set.urn is not None:
                logger.info(f"{i + 1}/{num_score_sets} Exporting variants for score set {score_set.urn}")
                csv_filename_base = score_set.urn.replace(":", "-")

                csv_str = get_score_set_scores_as_csv(db, score_set)
                zipfile.writestr(f"csv/{csv_filename_base}.scores.csv", csv_str)

                count_columns = score_set.dataset_columns["count_columns"] if score_set.dataset_columns else None
                if count_columns and len(count_columns) > 0:
                    csv_str = get_score_set_counts_as_csv(db, score_set)
                    zipfile.writestr(f"csv/{csv_filename_base}.counts.csv", csv_str)


if __name__ == "__main__":
    export_public_data()
