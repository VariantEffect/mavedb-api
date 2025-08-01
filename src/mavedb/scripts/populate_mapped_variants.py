import logging
import click
from datetime import date
from typing import Sequence, Optional

from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from mavedb.data_providers.services import vrs_mapper
from mavedb.lib.exceptions import NonexistentMappingReferenceError
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.mapping import ANNOTATION_LAYERS
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.score_set import ScoreSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant

from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def variant_from_mapping(db: Session, mapping: dict, dcd_mapping_version: str) -> MappedVariant:
    variant_urn = mapping.get("mavedb_id")
    variant = db.scalars(select(Variant).where(Variant.urn == variant_urn)).one()

    return MappedVariant(
        variant_id=variant.id,
        pre_mapped=mapping.get("pre_mapped"),
        post_mapped=mapping.get("post_mapped"),
        modification_date=date.today(),
        mapped_date=date.today(),  # since this is a one-time script, assume mapping was done today
        vrs_version=mapping.get("vrs_version"),
        mapping_api_version=dcd_mapping_version,
        error_message=mapping.get("error_message"),
        current=True,
    )


@script_environment.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Populate mapped variants for every score set in MaveDB.", is_flag=True)
def populate_mapped_variant_data(db: Session, urns: Sequence[Optional[str]], all: bool):
    score_set_ids: Sequence[Optional[int]]
    if all:
        score_set_ids = db.scalars(select(ScoreSet.id)).all()
        logger.info(
            f"Command invoked with --all. Routine will populate mapped variant data for {len(urns)} score sets."
        )
    else:
        score_set_ids = db.scalars(select(ScoreSet.id).where(ScoreSet.urn.in_(urns))).all()
        logger.info(f"Populating mapped variant data for the provided score sets ({len(urns)}).")

    vrs = vrs_mapper()

    for idx, ss_id in enumerate(score_set_ids):
        if not ss_id:
            continue

        score_set = db.scalar(select(ScoreSet).where(ScoreSet.id == ss_id))
        if not score_set:
            logger.warning(f"Could not fetch score set with id={ss_id}.")
            continue

        try:
            existing_mapped_variants = (
                db.query(MappedVariant)
                .join(Variant)
                .join(ScoreSet)
                .filter(ScoreSet.id == ss_id, MappedVariant.current.is_(True))
                .all()
            )

            for variant in existing_mapped_variants:
                variant.current = False

            assert score_set.urn
            logger.info(f"Mapping score set {score_set.urn}.")
            mapped_scoreset = vrs.map_score_set(score_set.urn)
            logger.info(f"Done mapping score set {score_set.urn}.")

            dcd_mapping_version = mapped_scoreset["dcd_mapping_version"]
            mapped_scores = mapped_scoreset.get("mapped_scores")

            if not mapped_scores:
                # if there are no mapped scores, the score set failed to map.
                score_set.mapping_state = MappingState.failed
                score_set.mapping_errors = {"error_message": mapped_scoreset.get("error_message")}
                db.commit()
                logger.info(f"No mapped variants available for {score_set.urn}.")
            else:
                reference_metadata = mapped_scoreset.get("reference_sequences")
                if not reference_metadata:
                    raise NonexistentMappingReferenceError()

                for target_gene_identifier in reference_metadata:
                    target_gene = next(
                        (
                            target_gene
                            for target_gene in score_set.target_genes
                            if target_gene.name == target_gene_identifier
                        ),
                        None,
                    )
                    if not target_gene:
                        raise ValueError(
                            f"Target gene {target_gene_identifier} not found in database for score set {score_set.urn}."
                        )
                    # allow for multiple annotation layers
                    pre_mapped_metadata = {}
                    post_mapped_metadata = {}
                    excluded_pre_mapped_keys = {"sequence"}
                    for annotation_layer in reference_metadata[target_gene_identifier]:
                        layer_premapped = reference_metadata[target_gene_identifier][annotation_layer].get(
                            "computed_reference_sequence"
                        )
                        if layer_premapped:
                            pre_mapped_metadata[ANNOTATION_LAYERS[annotation_layer]] = {
                                k: layer_premapped[k]
                                for k in set(list(layer_premapped.keys())) - excluded_pre_mapped_keys
                            }
                        layer_postmapped = reference_metadata[target_gene_identifier][annotation_layer].get(
                            "mapped_reference_sequence"
                        )
                        if layer_postmapped:
                            post_mapped_metadata[ANNOTATION_LAYERS[annotation_layer]] = layer_postmapped
                    target_gene.pre_mapped_metadata = cast(pre_mapped_metadata, JSONB)
                    target_gene.post_mapped_metadata = cast(post_mapped_metadata, JSONB)

                mapped_variants = [
                    variant_from_mapping(db=db, mapping=mapped_score, dcd_mapping_version=dcd_mapping_version)
                    for mapped_score in mapped_scores
                ]
                logger.debug(f"Done constructing {len(mapped_variants)} mapped variant objects.")

                num_successful_variants = len(
                    [variant for variant in mapped_variants if variant.post_mapped is not None]
                )
                logger.debug(
                    f"{num_successful_variants}/{len(mapped_variants)} variants generated a post-mapped VRS object."
                )

                if num_successful_variants == 0:
                    score_set.mapping_state = MappingState.failed
                    score_set.mapping_errors = {"error_message": "All variants failed to map"}
                elif num_successful_variants < len(mapped_variants):
                    score_set.mapping_state = MappingState.incomplete
                else:
                    score_set.mapping_state = MappingState.complete

                db.bulk_save_objects(mapped_variants)
                db.commit()
                logger.info(f"Done populating {len(mapped_variants)} mapped variants for {score_set.urn}.")

        except Exception as e:
            logging_context = {
                "mapped_score_sets": urns[:idx],
                "unmapped_score_sets": urns[idx:],
            }
            logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
            logger.error(f"Score set {score_set.urn} failed to map.", extra=logging_context)
            logger.info(f"Rolling back all changes for scoreset {score_set.urn}")
            db.rollback()

        logger.info(f"Done with score set {score_set.urn}. ({idx+1}/{len(urns)}).")

    logger.info("Done populating mapped variant data.")


if __name__ == "__main__":
    populate_mapped_variant_data()
