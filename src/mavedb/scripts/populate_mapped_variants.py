import logging
import click
from typing import Sequence, Optional

from sqlalchemy import select, null
from sqlalchemy.orm import Session

from mavedb.data_providers.services import vrs_mapper
from mavedb.models.score_set import ScoreSet
from mavedb.models.mapped_variant import MappedVariant

from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def variant_from_mapping(mapping: dict) -> MappedVariant:
    return MappedVariant(
        variant_id=mapping.get("mavedb_id"),
        pre_mapped=mapping.get("pre_mapped", null()),
        post_mapped=mapping.get("post_mapped", null()),
    )


@script_environment.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Populate mapped variants for every score set in MaveDB.", is_flag=True)
def populate_mapped_variant_data(db: Session, urns: Sequence[Optional[str]], all: bool):
    logger.info("Populating mapped variant data")
    if all:
        urns = db.scalars(select(ScoreSet.urn)).all()
        logger.debug(f"Populating mapped variant data for all score sets ({len(urns)}).")
    else:
        logger.debug(f"Populating mapped variant data for the provided score sets ({len(urns)}).")

    vrs = vrs_mapper()

    for idx, urn in enumerate(urns):
        logger.info(f"Populating mapped variant data for {urn}. ({idx+1}/{len(urns)}).")

        mapped_scoreset = vrs.map_score_set(urn)
        logger.debug("Done mapping score set.")

        mapped_variants = (*map(variant_from_mapping, mapped_scoreset["mapped_scores"]),)
        logger.debug(f"Done constructing {len(mapped_variants)} mapped variant objects.")

        db.bulk_save_objects(mapped_variants)
        logger.info(f"Done populating {len(mapped_variants)} mapped variants for {urn}.")

    logger.info("Done populating mapped variant data.")


if __name__ == "__main__":
    populate_mapped_variant_data()
