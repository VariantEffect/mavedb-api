import click
import logging
from datetime import datetime

from sqlalchemy import select, or_, and_
from sqlalchemy.orm import Session

from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.scripts.environment import script_environment, with_database_session
from mavedb.view_models import score_set
from mavedb.view_models import mapped_variant

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@script_environment.command()
@click.option("export_dir", "--export", type=click.Path(exists=True, writable=True, dir_okay=True, file_okay=False))
@with_database_session
def export_mapped_variant_data(db: Session, export_dir: click.Path):
    score_sets_with_mapped_variants = db.scalars(
        select(ScoreSet).where(
            or_(ScoreSet.mapping_state == MappingState.complete, ScoreSet.mapping_state == MappingState.incomplete)
        )
    ).all()

    for export_score_set in score_sets_with_mapped_variants:
        metadata = score_set.ScoreSet.model_validate(export_score_set, from_attributes=True)
        mapped_variants = db.scalars(
            select(MappedVariant)
            .join(Variant)
            .where(and_(Variant.score_set_id == export_score_set.id, MappedVariant.current == True))  # noqa: E712
        ).all()

        mapped_variant_export = [
            mapped_variant.MappedVariant.model_validate(mv).model_dump_json() for mv in mapped_variants
        ]

        with open(f"{export_dir}/{export_score_set.urn}_{datetime.now().isoformat()}.json", "w") as fp:
            fp.write(
                "{"
                + f'"metadata": {metadata.model_dump_json()}, "mapped_scores": [{", ".join(mapped_variant_export)}]'
                + "}"
            )


if __name__ == "__main__":
    export_mapped_variant_data()
