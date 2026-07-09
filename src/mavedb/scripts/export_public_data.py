"""
Script that generates a dump of published MaveDB data.

Usage:
```
python3 -m mavedb.scripts.export_public_data
```

This generates a ZIP archive named `mavedb-dump.YYYYMMDDHHMMSS.zip` in the working directory.
See `src/mavedb/scripts/resources/README.md` for a full description of the archive contents and file formats.

Unpublished data and data sets licensed other than under the Creative Commons Zero license are not included in the dump,
and user details are limited to ORCID IDs and names of contributors to published data sets.

RELEASING A NEW VERSION: Before publishing a new version of this archive to Zenodo, add an entry describing what
changed to `src/mavedb/scripts/resources/CHANGELOG.md` (it is bundled into the archive). The full release procedure
is documented in `deployment/docs/zenodo-release.md`.
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
from sqlalchemy.orm import Session, joinedload, lazyload

from mavedb.lib.annotation.annotate import variant_highest_level_annotation
from mavedb.lib.score_sets import get_current_mapped_variants_for_annotation, get_score_set_variants_as_csv
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.license import License
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.scripts.environment import script_environment, with_database_session
from mavedb.view_models import mapped_variant as mapped_variant_vm
from mavedb.view_models.experiment_set import ExperimentSetPublicDump

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
    logger.info(f"Found {len(experiment_sets)} published experiment sets with CC0-licensed score sets.")

    # TODO To support very large data sets, we may want to use custom code for JSON-encoding an iterator.
    # Issue: https://github.com/VariantEffect/mavedb-api/issues/192
    # See, for instance, https://stackoverflow.com/questions/12670395/json-encoding-very-long-iterators.

    experiment_set_views = list(map(lambda es: ExperimentSetPublicDump.model_validate(es), experiment_sets))

    # Get a list of IDS of all the score sets included.
    score_set_ids = list(
        flatmap(lambda es: flatmap(lambda e: map(lambda ss: ss.id, e.score_sets), es.experiments), experiment_sets)
    )

    timestamp_format = "%Y%m%d%H%M%S"
    zip_file_name = f"mavedb-dump.{datetime.now().strftime(timestamp_format)}.zip"

    logger.info(f"Writing {zip_file_name} with {len(score_set_ids)} score sets.")
    json_data = {
        "title": "MaveDB public data",
        "asOf": datetime.now(timezone.utc).isoformat(),
        "experimentSets": experiment_set_views,
    }

    with ZipFile(zip_file_name, "w") as zipfile:
        # Write metadata for all data sets to a single JSON file.
        zipfile.writestr("main.json", json.dumps(jsonable_encoder(json_data)))

        # Copy the CC0 license, README, and changelog.
        resources_dir = os.path.join(os.path.dirname(__file__), "resources")
        zipfile.write(os.path.join(resources_dir, "CC0_license.txt"), "LICENSE.txt")
        zipfile.write(os.path.join(resources_dir, "README.md"), "README.md")
        zipfile.write(os.path.join(resources_dir, "CHANGELOG.md"), "CHANGELOG.md")

        # Write score and count files for each score set.
        num_score_sets = len(score_set_ids)
        for i, score_set_id in enumerate(score_set_ids):
            score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one_or_none()
            if score_set is not None and score_set.urn is not None:
                logger.info(f"[{i + 1}/{num_score_sets}] Exporting score set {score_set.urn}")
                csv_filename_base = score_set.urn.replace(":", "-")

                csv_str = get_score_set_variants_as_csv(db, score_set, ["scores"], namespaced=True)
                zipfile.writestr(f"csv/{csv_filename_base}.scores.csv", csv_str)

                # Only generate annotation files if the score set has at least one current mapped variant.
                # A score set whose mappings are all superseded (no current mapping) yields no annotations,
                # so we skip emitting empty/superseded-only annotation files for it entirely.
                has_annotations = (
                    db.scalars(
                        select(ScoreSet)
                        .where(ScoreSet.id == score_set_id)
                        .join(Variant)
                        .join(MappedVariant)
                        .where(MappedVariant.current.is_(True))
                        .limit(1)
                    ).one_or_none()
                    is not None
                )
                if has_annotations:
                    csv_str = get_score_set_variants_as_csv(
                        db,
                        score_set,
                        [
                            "vep",
                            "gnomad",
                            "clingen",
                            "clinvar.2015_02",
                            "clinvar.2016_01",
                            "clinvar.2017_01",
                            "clinvar.2018_01",
                            "clinvar.2019_01",
                            "clinvar.2020_01",
                            "clinvar.2021_01",
                            "clinvar.2022_01",
                            "clinvar.2023_01",
                            "clinvar.2024_01",
                            "clinvar.2025_01",
                            "clinvar.2026_01",
                        ],
                        include_post_mapped_hgvs=True,
                        namespaced=True,
                    )
                    zipfile.writestr(f"csv/{csv_filename_base}.annotations.csv", csv_str)

                    # Write mapped variants JSON — mirrors GET /api/v1/score-sets/{urn}/mapped-variants.
                    mapped_variants = db.scalars(
                        select(MappedVariant)
                        .join(Variant, Variant.id == MappedVariant.variant_id)
                        .options(joinedload(MappedVariant.variant))
                        .where(Variant.score_set_id == score_set_id)
                        .where(MappedVariant.current.is_(True))
                    ).all()
                    mapped_variant_views = [
                        mapped_variant_vm.MappedVariant.model_validate(mv) for mv in mapped_variants
                    ]
                    zipfile.writestr(
                        f"mapped/{csv_filename_base}.mapped-variants.json",
                        json.dumps(jsonable_encoder(mapped_variant_views)),
                    )
                    logger.info(
                        f"[{i + 1}/{num_score_sets}]   Wrote annotations + {len(mapped_variants)} mapped variants"
                    )

                    # Write VA-Spec annotations NDJSON — mirrors the GET /api/v1/score-sets/{urn}/annotated-variants/*
                    # streams, emitting one record per current mapped variant at its highest materialized VA level.
                    annotated_variants = get_current_mapped_variants_for_annotation(db, score_set)

                    va_lines = []
                    num_annotations = 0
                    for mv in annotated_variants:
                        annotation = variant_highest_level_annotation(mv)
                        if annotation is not None:
                            num_annotations += 1
                        record = {
                            "variant_urn": mv.variant.urn,
                            "annotation": annotation.model_dump(exclude_none=True) if annotation else None,
                        }
                        va_lines.append(json.dumps(record, default=str))

                    # Newline-terminate every record (including the last) to match the API NDJSON streams
                    # and keep line-based consumers happy.
                    zipfile.writestr(f"va/{csv_filename_base}.va.ndjson", "".join(line + "\n" for line in va_lines))
                    logger.info(
                        f"[{i + 1}/{num_score_sets}]   Wrote {len(va_lines)} VA-Spec records "
                        f"({num_annotations} non-null annotations)"
                    )

                # Only generate the counts CSV if count columns are present.
                count_columns = score_set.dataset_columns["count_columns"] if score_set.dataset_columns else None
                if count_columns and len(count_columns) > 0:
                    csv_str = get_score_set_variants_as_csv(db, score_set, ["counts"], namespaced=True)
                    zipfile.writestr(f"csv/{csv_filename_base}.counts.csv", csv_str)

    logger.info(f"Export complete: {zip_file_name}")


if __name__ == "__main__":
    export_public_data()
