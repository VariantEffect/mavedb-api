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
"""

import json
import logging
import os
from datetime import datetime, timezone
from itertools import chain
from typing import Callable, Iterable, Optional, TypeVar
from zipfile import ZipFile

from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload, lazyload

from mavedb.lib.annotation.annotate import variant_highest_level_annotation
from mavedb.lib.csv.namespaces import CsvNamespace
from mavedb.lib.csv.score_set import (
    available_score_set_csv_namespaces,
    get_score_set_variants_as_csv,
)
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.lib.score_sets import get_current_mapped_variants_for_annotation
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


def annotation_export_namespaces(db: Session, score_set: ScoreSet) -> list[str]:
    """The namespaces the public annotations CSV should carry for this score set.

    Asks discovery what the score set actually has rather than naming groups by hand. The previous
    hand-maintained list enumerated ClinVar releases one by one, so it emitted all-NA columns for releases
    never ingested, needed a code change for every new release, and was fragile to schema changes.

    The archive carries everything MaveDB holds about the score set, so this takes what discovery found
    and subtracts from it rather than opting groups in.

    In particular it does not filter on `selected_by_default`. That flag answers "what should a download
    dialog open on", which is a question about attention rather than about what exists, and the reasons a
    group opens unchecked are not interchangeable. An archive is about completeness, not about what a user
    should be nudged to look at first.

    Subtractions:

    - Every score and count group, and the score set's own identity: scores and counts get their own
      files, and the URN is in the filename, so repeating either would be noise.
    """
    excluded = {
        CsvNamespace.SCORES,
        CsvNamespace.SCORES_CUSTOM,
        CsvNamespace.COUNTS,
        CsvNamespace.SCORE_SET,
    }
    return [
        entry.namespace
        for entry in available_score_set_csv_namespaces(db, score_set)
        if entry.namespace not in excluded
    ]


def flatmap(f: Callable[[S], Iterable[T]], items: Iterable[S]) -> Iterable[T]:
    return chain.from_iterable(map(f, items))


def public_experiment_set(
    experiment_set_view: ExperimentSetPublicDump, visible_calibration_ids: set[int]
) -> Optional[ExperimentSetPublicDump]:
    """
    Narrow a validated experiment set to what belongs in the public dump.

    Drops calibrations an anonymous caller may not read, then experiments left with no score sets, and
    returns None for an experiment set left with no experiments. The score sets themselves need no filter:
    the loading query already restricts them to published, CC0-licensed ones.

    Narrowing the validated view rather than the ORM graph is deliberate. ``ExperimentSet.experiments`` and
    ``ScoreSet.score_calibrations`` are both mapped with ``cascade="all, delete-orphan"``, so removing a
    member from either ORM collection marks the removed row as an orphan and the next flush deletes it.
    This script can flush: ``with_database_session`` commits when invoked with ``--commit``.

    Args:
        experiment_set_view (ExperimentSetPublicDump): The validated experiment set to narrow.
        visible_calibration_ids (set[int]): Ids of the calibrations an anonymous caller may read.

    Returns:
        Optional[ExperimentSetPublicDump]: The narrowed experiment set, or None if nothing public remains.
    """
    experiments = []
    for experiment_view in experiment_set_view.experiments:
        if not experiment_view.score_sets:
            continue

        score_sets = [
            score_set_view.model_copy(
                update={
                    "score_calibrations": [
                        calibration
                        for calibration in (score_set_view.score_calibrations or [])
                        if calibration.id in visible_calibration_ids
                    ]
                }
            )
            for score_set_view in experiment_view.score_sets
        ]
        experiments.append(experiment_view.model_copy(update={"score_sets": score_sets}))

    if not experiments:
        return None

    return experiment_set_view.model_copy(update={"experiments": experiments})


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

    experiment_sets = experiment_sets_query.all()

    # The dump is built for an anonymous principal. Publishing a score set does not publish its
    # calibrations: a calibration keeps its own `private` flag and a stricter READ rule, so every artifact
    # below is scoped to what this viewer may read.
    public_principal = Principal()
    public_viewer = public_principal.viewer_for(ScoreCalibrationViewer)
    all_calibrations = [
        calibration
        for score_set_orm in flatmap(lambda es: flatmap(lambda e: e.score_sets, es.experiments), experiment_sets)
        for calibration in (score_set_orm.score_calibrations or [])
    ]

    # TODO(#372): Nullable ids.
    visible_calibration_ids: set[int] = {calibration.id for calibration in public_viewer.visible(all_calibrations)}  # type: ignore
    if len(all_calibrations) > len(visible_calibration_ids):
        logger.info(
            f"Withholding {len(all_calibrations) - len(visible_calibration_ids)} non-public score "
            "calibration(s) from the dump."
        )

    # TODO To support very large data sets, we may want to use custom code for JSON-encoding an iterator.
    # Issue: https://github.com/VariantEffect/mavedb-api/issues/192
    # See, for instance, https://stackoverflow.com/questions/12670395/json-encoding-very-long-iterators.

    experiment_set_views = [
        narrowed
        for narrowed in (
            public_experiment_set(ExperimentSetPublicDump.model_validate(es), visible_calibration_ids)
            for es in experiment_sets
        )
        if narrowed is not None
    ]
    logger.info(f"Found {len(experiment_set_views)} published experiment sets with CC0-licensed score sets.")

    score_set_urns = list(
        flatmap(
            lambda es: flatmap(lambda e: map(lambda ss: ss.urn, e.score_sets), es.experiments), experiment_set_views
        )
    )

    timestamp_format = "%Y%m%d%H%M%S"
    zip_file_name = f"mavedb-dump.{datetime.now().strftime(timestamp_format)}.zip"

    logger.info(f"Writing {zip_file_name} with {len(score_set_urns)} score sets.")
    json_data = {
        "title": "MaveDB public data",
        "asOf": datetime.now(timezone.utc).isoformat(),
        "experimentSets": experiment_set_views,
    }

    with ZipFile(zip_file_name, "w") as zipfile:
        # Write metadata for all data sets to a single JSON file.
        zipfile.writestr("main.json", json.dumps(jsonable_encoder(json_data)))

        # Copy the CC0 license and README.
        resources_dir = os.path.join(os.path.dirname(__file__), "resources")
        zipfile.write(os.path.join(resources_dir, "CC0_license.txt"), "LICENSE.txt")
        zipfile.write(os.path.join(resources_dir, "README.md"), "README.md")

        # Write score and count files for each score set.
        num_score_sets = len(score_set_urns)
        for i, score_set_urn in enumerate(score_set_urns):
            score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one_or_none()
            if score_set is not None:
                logger.info(f"[{i + 1}/{num_score_sets}] Exporting score set {score_set_urn}")
                csv_filename_base = score_set_urn.replace(":", "-")

                csv_str = get_score_set_variants_as_csv(db, score_set, ["scores"], namespaced=True)
                zipfile.writestr(f"csv/{csv_filename_base}.scores.csv", csv_str)

                # Only generate annotation files if the score set has at least one current mapped variant.
                # A score set whose mappings are all superseded (no current mapping) yields no annotations,
                # so we skip emitting empty/superseded-only annotation files for it entirely.
                has_annotations = (
                    db.scalars(
                        select(ScoreSet)
                        .where(ScoreSet.id == score_set.id)
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
                        annotation_export_namespaces(db, score_set),
                        namespaced=True,
                    )
                    zipfile.writestr(f"csv/{csv_filename_base}.annotations.csv", csv_str)

                    # Write mapped variants JSON — mirrors GET /api/v1/score-sets/{urn}/mapped-variants.
                    mapped_variants = db.scalars(
                        select(MappedVariant)
                        .join(Variant, Variant.id == MappedVariant.variant_id)
                        .options(joinedload(MappedVariant.variant))
                        .where(Variant.score_set_id == score_set.id)
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
                        annotation = variant_highest_level_annotation(mv, principal=public_principal)
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
