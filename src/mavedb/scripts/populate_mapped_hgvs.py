import logging
import requests
from typing import Sequence, Optional

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.clingen.allele_registry import CLINGEN_API_URL
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.variants import get_hgvs_from_post_mapped

from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant

from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_target_info(score_set: ScoreSet) -> tuple[bool, Optional[str]]:
    target_is_coding: bool
    transcript_accession: Optional[str] = None
    if len(score_set.target_genes) == 1:
        target = score_set.target_genes[0]
        if target.category == "protein_coding":
            target_is_coding = True
            # only get transcript accession if coding
            # accession-based
            if target.target_accession and target.target_accession.accession:
                # only use accession info if a transcript was specified
                if target.target_accession.accession.startswith(("NM", "ENST")):
                    transcript_accession = target.target_accession.accession
            # sequence-based
            if target.post_mapped_metadata:
                # assert that post_mapped_metadata is a dict for mypy
                assert isinstance(target.post_mapped_metadata, dict)
                if target.post_mapped_metadata.get("cdna", {}).get("sequence_accessions"):
                    if len(target.post_mapped_metadata["cdna"]["sequence_accessions"]) == 1:
                        transcript_accession = target.post_mapped_metadata["cdna"]["sequence_accessions"][0]
                    else:
                        raise ValueError(
                            f"Multiple cDNA accessions found in post-mapped metadata for target {target.name} in score set {score_set.urn}. Cannot determine which to use."
                        )
                # if sequence-based and no cDNA accession, warn that no transcript was specified
                else:
                    # for coding score sets, the mapper should have returned a cdna post mapped metadata entry. Use mane transcript from clingen for now, but warn that we are assuming transcript.
                    logger.warning(
                        f"No cDNA accession found in post-mapped metadata for target {target.name} in score set {score_set.urn}. This is expected if variants were only provided at the protein level. If variants are at the nucleotide level, will assume MANE transcript from ClinGen for coding variant."
                    )
            else:
                # for coding score sets, the mapper should have returned a cdna post mapped metadata entry. Use mane transcript from clingen for now, but warn that we are assuming transcript.
                logger.warning(
                    f"No post-mapped metadata for target {target.name} in score set {score_set.urn}. Will assume MANE transcript from ClinGen for coding variant."
                )
        else:
            target_is_coding = False
    # multi-target score sets are more complex because there is no direct link between variants and targets in the db. support later
    else:
        raise NotImplementedError("Populating mapped hgvs for multi-target score sets is not yet supported.")

    return target_is_coding, transcript_accession


@script_environment.command()
@with_database_session
@click.argument("urns", nargs=-1)
@click.option("--all", help="Populate mapped hgvs for every score set in MaveDB.", is_flag=True)
def populate_mapped_hgvs(db: Session, urns: Sequence[Optional[str]], all: bool):
    score_set_ids: Sequence[Optional[int]]
    if all:
        score_set_ids = db.scalars(select(ScoreSet.id)).all()
        logger.info(f"Command invoked with --all. Routine will populate mapped hgvs for {len(urns)} score sets.")
    else:
        score_set_ids = db.scalars(select(ScoreSet.id).where(ScoreSet.urn.in_(urns))).all()
        logger.info(f"Populating mapped hgvs for the provided score sets ({len(urns)}).")

    for idx, ss_id in enumerate(score_set_ids):
        if not ss_id:
            continue

        score_set = db.scalar(select(ScoreSet).where(ScoreSet.id == ss_id))
        if not score_set:
            logger.warning(f"Could not fetch score set with id={ss_id}.")
            continue

        try:
            target_is_coding, transcript_accession = get_target_info(score_set)

            variant_info = db.execute(
                select(Variant.urn, MappedVariant)
                .join(Variant)
                .join(ScoreSet)
                .where(ScoreSet.id == ss_id)
                .where(MappedVariant.current == True)  # noqa: E712
            )

            # for variant_urn, post_mapped, clingen_allele_id in variant_info:
            for variant_urn, mapped_variant in variant_info:
                # NOTE: get_hgvs_from_post_mapped currently does not support multi-variants
                # returns None if no post-mapped object or if multi-variant
                hgvs_assay_level = get_hgvs_from_post_mapped(mapped_variant.post_mapped)

                hgvs_g: Optional[str] = None
                hgvs_c: Optional[str] = None
                hgvs_p: Optional[str] = None

                # NOTE: if no clingen allele id, could consider searching clingen using hgvs_assay_level. for now, skipping variant if no clingen allele id in db
                # TODO skipping multi-variants for now
                if mapped_variant.clingen_allele_id and len(mapped_variant.clingen_allele_id.split(",")) == 1:
                    response = requests.get(f"{CLINGEN_API_URL}/{mapped_variant.clingen_allele_id}")
                    if response.status_code != 200:
                        logger.error(
                            f"Failed for variant {variant_urn} to query ClinGen API for {mapped_variant.clingen_allele_id}: {response.status_code}"
                        )
                        continue
                    data = response.json()
                    if mapped_variant.clingen_allele_id.startswith("CA"):
                        if data.get("genomicAlleles"):
                            for allele in data["genomicAlleles"]:
                                if allele.get("referenceGenome") == "GRCh38" and allele.get("hgvs"):
                                    hgvs_g = allele["hgvs"][0]
                                    break
                        if target_is_coding:
                            if data.get("transcriptAlleles"):
                                if transcript_accession:
                                    for allele in data["transcriptAlleles"]:
                                        if allele.get("hgvs"):
                                            for hgvs_string in allele["hgvs"]:
                                                hgvs_reference_sequence = hgvs_string.split(":")[0]
                                                if transcript_accession == hgvs_reference_sequence:
                                                    hgvs_c = hgvs_string
                                                    break
                                        if hgvs_c:
                                            if allele.get("proteinEffect"):
                                                hgvs_p = allele["proteinEffect"].get("hgvs")
                                            break
                                else:
                                    # no transcript specified, use mane if available
                                    for allele in data["transcriptAlleles"]:
                                        if allele.get("MANE"):
                                            # TODO consider prioritizing mane select over other mane statuses
                                            hgvs_c = allele["MANE"].get("nucleotide", {}).get("RefSeq", {}).get("hgvs")
                                            hgvs_p = allele["MANE"].get("protein", {}).get("RefSeq", {}).get("hgvs")
                                            break

                    elif mapped_variant.clingen_allele_id.startswith("PA"):
                        # if PA, assume that assay was performed at amino acid level, so only provide hgvs_p
                        if data.get("aminoAcidAlleles"):
                            for allele in data["aminoAcidAlleles"]:
                                if allele.get("hgvs"):
                                    hgvs_p = allele["hgvs"][0]
                                    break

                # TODO should we check that assay level hgvs mtches either g. or p.?

                mapped_variant.hgvs_assay_level = hgvs_assay_level
                mapped_variant.hgvs_g = hgvs_g
                mapped_variant.hgvs_c = hgvs_c
                mapped_variant.hgvs_p = hgvs_p
                db.add(mapped_variant)
            db.commit()

        except Exception as e:
            logging_context = {
                "processed_score_sets": urns[:idx],
                "unprocessed_score_sets": urns[idx:],
            }
            logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
            logger.error(
                f"Score set {score_set.urn} could not be processed to extract hgvs strings.", extra=logging_context
            )
            logger.info(f"Rolling back all changes for scoreset {score_set.urn}")
            db.rollback()

        # TODO if accession based and no post mapped object, use original variant hgvs if available?
        # the problem with this is that it would be c. in a lot of cases, and we want g. if possible, since we map to the genome for c./n./g. variants

        logger.info(f"Done with score set {score_set.urn}. ({idx+1}/{len(urns)}).")

    logger.info("Done populating mapped hgvs.")


if __name__ == "__main__":
    populate_mapped_hgvs()
