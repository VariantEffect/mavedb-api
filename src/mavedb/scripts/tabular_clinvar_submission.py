"""
Export variants for a single score set into a tabular file compatible with
ClinVar's Functional Data Submission template (F1.0).

This script produces a header row that exactly matches the ClinVar functional
data template column names. The output is intended to be readily usable for
ClinVar functional data submissions once you populate the required fields.

Example:
    with_mavedb_local poetry run python3 -m mavedb.scripts.tabular_clinvar_submission \
        urn:mavedb:00000001-a-1 ./out --format tsv

Notes:
    - Uses @with_database_session for DB lifecycle and dry-run/prompt/commit.
    - Does not change the database; commit/dry-run flags are provided for
        consistency with other scripts.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.parse import quote_plus

import click
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from mavedb.constants import MAVEDB_FRONTEND_URL
from mavedb.models.enums.functional_classification import FunctionalClassification
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.scripts.environment import with_database_session

logger = logging.getLogger(__name__)


SCORING_RANGE_STR = "Scoring range %d"
LABEL_FOR_SCORING_RANGE_STR = "Label for scoring range %d"

# Column documentation and names (single source of truth)
# Each tuple is (header name [exact string], description)
CLINVAR_COLUMNS: List[Tuple[str, str]] = [
    (
        "##Local ID",
        "Optional, but highly recommended. The stable unique identifier your organization uses to identify this variant. "
        "This identifier will be public so should not include protected health information.",
    ),
    (
        "Gene symbol",
        "Optional. The preferred symbol in NCBI's Gene database, which corresponds to HGNC's preferred symbol when one is available. "
        "Use only gene symbols that represent functional genes; do not include symbols for pseudogenes or regulatory regions.",
    ),
    (
        "Reference sequence",
        "Required for sequence variants unless chromosome and some combination of start and stop are provided. "
        "The reference sequence for the HGVS expression provided in the next column (e.g., NM_000492.3, NG_016465.3, or NC_000007.13).",
    ),
    (
        "HGVS",
        "Required for sequence variants unless chromosome and some combination of start and stop are provided. "
        "Provide the c. or g. portion of the nucleotide HGVS expression for the variant(s) being reported related to condition (https://varnomen.hgvs.org/).",
    ),
    (
        "Chromosome",
        "Required for structural variants as well as sequence variants not described by an HGVS expression. "
        "You will also need to provide the name of the genome assembly used for all variants in this submission when you upload this file in the ClinVar Submission Portal.",
    ),
    (
        "Start",
        "Required for sequence variants only defined by chromosome coordinates (not by an HGVS expression); also required for structural variants when absolute start is known. "
        "The start location for the reference allele in chromosome coordinates. If only start is provided, stop will be presumed to be the same coordinate.",
    ),
    (
        "Stop",
        "Required for sequence variants defined only by chromosome coordinates (not by an HGVS expression) and not submitted according to the VCF standard; "
        "also required for structural variants when absolute stop is known. If only start is provided, the submission will be processed as if the definition meets the VCF standard.",
    ),
    (
        "Reference allele",
        "Required for sequence variants if an HGVS expression is not provided and the variant is not a deletion. "
        "For an insertion, enter '-' and select 'Insertion' as 'Variant type'.",
    ),
    (
        "Alternate allele",
        "Required for sequence variants if HGVS expression is not provided. The alternate allele for the submitted variant. "
        "For a deletion, enter '-' and select 'Deletion' as 'Variant type'.",
    ),
    (
        "Variation identifiers",
        "Optional. Any of OMIM allelic variant ID, dbSNP rs number, dbVar submitter or region identifier. "
        "Report as database name:database identifier; integers without a database name cannot be processed. "
        "Examples: OMIM:611101.0001, dbSNP: rs104894321, dbVar:nsv491743, dbVar:essv12345. Separate multiple identifiers by a semicolon.",
    ),
    (
        "Location",
        "Optional. This is the variant's location within a gene. Options are exon, intron, 5'UTR, 3'UTR, promoter. "
        "Must be submitted with the reference sequence used to define the location within the gene, e.g. NM_000060.2:exon 2 or NM_000060.2:5'UTR or NM_000060.2:promoter. "
        "Separate multiple locations on different reference sequences by a semicolon.",
    ),
    (
        "Alternate designations ",
        "Optional. A set of alternate, common, or legacy names for the variant. Separate multiple names with a vertical bar (|).",
    ),
    (
        "URL",
        "Optional. A URL that points to this variant on your organization's website and that does not match the pattern of Base URL + Local ID (Variant tab).",
    ),
    (
        "Condition ID type",
        "Required if 'Preferred condition name' is not supplied. Please specify the database for the ID in the Condition ID value column. "
        "Allowed values are: OMIM, MeSH, MedGen, Orphanet, HPO, or Mondo.",
    ),
    (
        "Condition ID value",
        "Required if 'Preferred condition name' is not supplied. Please specify the identifier from the database referenced in the Condition ID type column. "
        "If there are multiple values, they all need to be from the same source, and separated by a semicolon.",
    ),
    (
        "Preferred condition name",
        "Required if 'Condition ID Type' and 'Condition ID Value' are not supplied. We strongly encourage using a database identifier; "
        "if not available, provide the disease name here (single name). If you do not wish to indicate the disease context, enter 'not provided'.",
    ),
    (
        "Explanation for multiple conditions",
        "Optional. Indicate if more than one disease context is relevant for the assay; used to capture feedback on multiple conditions.",
    ),
    (
        "Collection method",
        "Required. Allowed values will be in vivo, in vitro.",
    ),
    (
        "Assay type",
        "Required. A short general description for the type of assay, like RNA-seq, enzyme activity assay, or animal model.",
    ),
    (
        "Molecular phenotype measured",
        "Required, unless the assay type is 'animal model'. A short description of the specific molecular phenotype measured with this assay.",
    ),
    (
        "Method",
        "Required. A free text description of the method, to add detail to the general description in 'Assay type'.",
    ),
    (
        "Method citation",
        "Optional. PMID, PMC, or DOI for a citation describing this assay.",
    ),
    (
        LABEL_FOR_SCORING_RANGE_STR % 1,
        "Optional. The label for the first range of scores, e.g. functional; uncertain; non-functional. Note: you can add many additional scoring ranges if needed, just increment the number of the header.",
    ),
    (
        SCORING_RANGE_STR % 1,
        "Optional. The first range of scores (e.g., 0.5-1.0). Note: you can add many additional scoring ranges if needed, just increment the number of the header.",
    ),
    (
        LABEL_FOR_SCORING_RANGE_STR % 2,
        "Optional. The label for the second range of scores, e.g. functional; uncertain; non-functional. Note: you can add many additional scoring ranges if needed, just increment the number of the header.",
    ),
    (
        SCORING_RANGE_STR % 2,
        "Optional. The second range of scores (e.g., 0.5-1.0). Note: you can add many additional scoring ranges if needed, just increment the number of the header.",
    ),
    (
        LABEL_FOR_SCORING_RANGE_STR % 3,
        "Optional. The label for the third range of scores, e.g. functional; uncertain; non-functional. Note: you can add many additional scoring ranges if needed, just increment the number of the header.",
    ),
    (
        SCORING_RANGE_STR % 3,
        "Optional. The third range of scores (e.g., 0.5-1.0). Note: you can add many additional scoring ranges if needed, just increment the number of the header.",
    ),
    (
        "Species",
        "Required if 'Assay type' is 'animal model'. If not filled in, the default is human.",
    ),
    (
        "Strain/breed",
        "Optional, for animal models.",
    ),
    (
        "Cell line",
        "Optional. The cell line used for an in vitro assay. If patient cells were used, indicate that cell type in 'Tissue'.",
    ),
    (
        "Tissue",
        "Optional. The tissue type used for an in vitro assay. If the tissue was from a patient, indicate that, e.g., 'patient's fibroblasts'.",
    ),
    (
        "Sex",
        "Optional. The sex of the patient whose tissue was assayed, if relevant for the result.",
    ),
    (
        "Number of replicates",
        "Optional. The number of replicates for the assay.",
    ),
    (
        "Number of controls",
        "Optional. The number of controls that were assayed.",
    ),
    (
        "Functional effect",
        "Required. A high level term for the functional effect of the variant based on this assay. One of: functionally normal; functionally abnormal; function uncertain.",
    ),
    (
        "Functional consequence",
        "Required. A more specific term for the effect of the variant on function (e.g., exon splicing, reduced enzyme activity).",
    ),
    (
        "Result",
        "Required. The result of the assay for this variant (e.g., numerical measurement or brief qualitative result).",
    ),
    (
        "Comment on functional consequence",
        "Optional. A free text comment to provide more information or context for the functional effect.",
    ),
    (
        "ClinVarAccession",
        "Required for updated records and for novel records if accession numbers were reserved. Provide the SCV number for your submitted record (not the RCV number).",
    ),
    (
        "Novel or Update",
        "Optional. If you include ClinVar SCV accessions, indicate whether each record is novel (accessions reserved) or an update to an existing SCV record.",
    ),
    (
        "Replaces ClinVarAccessions ",
        "Optional. Used only if you have already submitted to ClinVar and have assigned accessions. To merge records, provide the retained SCV in ClinVarAccession and the discontinued SCVs here.",
    ),
]


SCORING_RANGE_INSERT_AFTER = CLINVAR_COLUMNS.index(
    (
        SCORING_RANGE_STR % 3,
        "Optional. The third range of scores (e.g., 0.5-1.0). Note: you can add many additional scoring ranges if needed, just increment the number of the header.",
    )
)


def _default_filename_from_urn(urn: str, fmt: str) -> str:
    base = urn.replace(":", "-")
    ext = "tsv" if fmt == "tsv" else "csv"
    return f"{base}.variants.{ext}"


def _iter_mapped_variants(db: Session, score_set: ScoreSet) -> Iterable[MappedVariant]:
    """
    Yield variants for the given score set.

    Skeleton only: replace the return type and extraction logic with whatever
    tabular columns you need. For example, you may want to pull from
    `variant.data`, variant numbers, scores, counts, annotations, etc.
    """
    return (
        db.execute(
            select(MappedVariant)
            .join(Variant)
            .options(selectinload(MappedVariant.variant))
            .where(Variant.score_set_id == score_set.id)
        ).scalars()
        or []
    )


def _write_tabular(
    db: Session,
    out_path: Path,
    variants: Iterable[MappedVariant],
    fmt: str = "tsv",
    include_header: bool = True,
) -> None:
    """
    Write variants to a tabular file.

    Skeleton only: replace the header and row building with the actual columns
    you need. Keep the writer minimal and stream-friendly.
    """
    sep = "\t" if fmt == "tsv" else ","
    out_path.parent.mkdir(parents=True, exist_ok=True)

    header = [col_name for col_name, _ in CLINVAR_COLUMNS]
    rows = []
    for v in variants:
        v_urn = v.variant.urn
        if not v_urn:
            logger.warning(f"Mapped variant ID {v.id} is missing URN; skipping")
            continue

        fully_qualified_assay_hgvs = v.hgvs_assay_level
        if not fully_qualified_assay_hgvs:
            logger.warning(f"Mapped variant URN {v_urn} is missing assay-level mapped HGVS; skipping")
            continue

        reference_sequence, hgvs_nt = fully_qualified_assay_hgvs.split(":")
        if not reference_sequence or not hgvs_nt:
            logger.warning(
                f"Mapped variant URN {v_urn} has invalid assay-level mapped HGVS '{fully_qualified_assay_hgvs}'; skipping"
            )
            continue

        variant_url = f"{MAVEDB_FRONTEND_URL}/score-sets/{quote_plus(v.variant.score_set.urn, safe='')}?variant={quote_plus(v.variant.urn, safe='')}"

        condition_id = "MedGen"  # the database used for condition IDs.
        condition_value = "C0012634"  # example condition ID for generic disease, which is what is exported via VA-Spec. Should we be more specific?

        collection_method = "in vitro"
        assay_type = "functional assay"  # see: https://ftp.ncbi.nlm.nih.gov/pub/clinvar/functional_assay_types.txt
        molecular_phenotype_measured = "molecular phenotype measured"

        # Escape the field delimiter and newlines so the naive join doesn't split this text
        method_text = v.variant.score_set.method_text or ""
        if fmt == "csv":
            # Quote and escape to protect commas and quotes; keep newlines escaped
            escaped = method_text.replace('"', '""').replace("\n", r"\n")
            method = f'"{escaped}"' if escaped else ""
        else:  # "tsv"
            method = method_text.replace("\t", r"\t").replace("\n", r"\n")
        method_citation = next(
            (
                pub.publication.identifier
                for pub in v.variant.score_set.publication_identifier_associations
                if pub.primary
            ),
            "",
        )

        # Get the primary score calibration for the score set containing this variant, if any
        score_calibration = next(
            (sc for sc in v.variant.score_set.score_calibrations if sc.primary is True),
            None,
        )
        score_calibration_rows = []
        if score_calibration:
            num_ranges = len(score_calibration.functional_classifications)
            additional_range_columns = []

            # If the score calibration has more than three ranges, we need to add
            # additional columns to the header
            if num_ranges > 3:
                for i in range(4, num_ranges + 1):
                    additional_range_columns.append((SCORING_RANGE_STR % i, ""))
                    additional_range_columns.append((LABEL_FOR_SCORING_RANGE_STR % i, ""))

                # Insert any additional scoring range columns after the third scoring range
                insert_idx = SCORING_RANGE_INSERT_AFTER
                for col in additional_range_columns:
                    if col[0] not in header:
                        header.insert(insert_idx, col[0])
                        insert_idx += 1

            # TODO: connect with clingen about categorically given ranges.
            if any(fc.range is None for fc in score_calibration.functional_classifications):
                logger.warning(
                    f"Mapped variant URN {v_urn} has score calibration ID {score_calibration.id} with "
                    "functional classifications missing ranges; skipping export of functional ranges"
                )
                break

            # Build rows for the functional classifications
            for fr in score_calibration.functional_classifications:
                score_calibration_rows.append((str(fr.label)))
                range_str = ""
                if fr.inclusive_lower_bound:
                    range_str += "["
                else:
                    range_str += "("
                range_str += str(fr.range[0] or "-Inf") + ", " + str(fr.range[1] or "Inf")  # type: ignore
                if fr.inclusive_upper_bound:
                    range_str += "]"
                else:
                    range_str += ")"
                score_calibration_rows.append(f'"{range_str}"')

        # Ensure we have six entries (three ranges) for the template
        while len(score_calibration_rows) < 6:
            score_calibration_rows.append("")

        score_calibration_functional_classification = None
        if score_calibration:
            score_calibration_functional_classification = db.execute(
                select(ScoreCalibrationFunctionalClassification).where(
                    ScoreCalibrationFunctionalClassification.calibration_id == score_calibration.id,
                    ScoreCalibrationFunctionalClassification.variants.any(Variant.id == v.variant.id),
                )
            ).scalar_one_or_none()

        _functional_effect: FunctionalClassification | None = (
            score_calibration_functional_classification.functional_classification
            if score_calibration_functional_classification
            else None
        )
        functional_effect = "function uncertain"
        if _functional_effect is FunctionalClassification.normal:
            functional_effect = "functionally normal"
        elif _functional_effect is FunctionalClassification.abnormal:
            functional_effect = "functionally abnormal"
        else:
            functional_effect = "function uncertain"
        functional_consequence = ""  # e.g., "reduced enzyme activity"?? Needs to be defined.
        result = str(v.variant.data.get("score_data", {}).get("score", "")) if v.variant.data else ""
        # comment_on_functional_consequence = ...

        # clinvar_accession = ... # If updating existing ClinVar records only. Note that we will eventually get this back, and must have a mechanism to store it.
        is_novel_or_update = "novel"  # "Novel" or "Update" if clinvar_accession is provided
        # replaces_clinvar_accessions = ...  # If merging existing ClinVar records only.

        row = [
            v_urn or "",  # ##Local ID
            "",  # Gene symbol
            str(reference_sequence) or "",  # Reference sequence
            str(hgvs_nt) or "",  # HGVS
            "",  # Chromosome
            "",  # Start
            "",  # Stop
            "",  # Reference allele
            "",  # Alternate allele
            "",  # Variation identifiers
            "",  # Location
            "",  # Alternate designations
            str(variant_url),  # URL
            str(condition_id),  # Condition ID type
            str(condition_value),  # Condition ID value
            "",  # Preferred condition name
            "",  # Explanation for multiple conditions
            str(collection_method),  # Collection method
            str(assay_type),  # Assay type
            str(molecular_phenotype_measured),  # Molecular phenotype measured
            str(method),  # Method
            str(method_citation),  # Method citation
            *score_calibration_rows,  # Scoring ranges and labels
            "",  # Species
            "",  # Strain/breed
            "",  # Cell line
            "",  # Tissue
            "",  # Sex
            "",  # Number of replicates
            "",  # Number of controls
            str(functional_effect),  # Functional effect
            str(functional_consequence),  # Functional consequence
            str(result),  # Result
            "",  # Comment on functional consequence
            "",  # ClinVarAccession
            str(is_novel_or_update),  # Novel or Update
            "",  # Replaces ClinVarAccessions
        ]
        rows.append(row)

    with out_path.open("w", encoding="utf-8", newline="") as fh:
        if include_header:
            fh.write(sep.join(header) + "\n")
        for row in rows:
            fh.write(sep.join(row) + "\n")


@click.command()
@with_database_session
@click.option(
    "--score-set-urn",
    "score_set_urn",
    type=str,
    required=True,
    help="URN of the score set to export variants from",
)
@click.option(
    "--output-dir",
    "output_dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Directory to write the output file to",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["tsv", "csv"], case_sensitive=False),
    default="tsv",
    show_default=True,
    help="Output format",
)
@click.option("--no-header", is_flag=True, default=False, help="Do not include a header row in the output")
@click.option(
    "--describe-columns",
    is_flag=True,
    default=False,
    help="Print the ClinVar header columns with descriptions and exit",
)
def main(db: Session, score_set_urn: str, output_dir: Path, fmt: str, no_header: bool, describe_columns: bool) -> None:
    """Export variants for SCORE_SET_URN to OUTPUT_DIR as a tabular file.

    The output filename will incorporate the URN (colons replaced by dashes),
    for example: `urn-mavedb-00000001-a-1.variants.tsv`.
    """
    if describe_columns:
        click.echo("ClinVar functional data header columns:\n")
        for name, desc in CLINVAR_COLUMNS:
            click.echo(f"- {name}: {desc}")
        return

    ss: Optional[ScoreSet] = db.query(ScoreSet).filter(ScoreSet.urn == score_set_urn).one_or_none()
    if ss is None:
        logger.error(f"Score set with URN '{score_set_urn}' not found")
        return

    outfile = output_dir / _default_filename_from_urn(score_set_urn, fmt)
    logger.info(f"Exporting variants for {score_set_urn} -> {outfile}")

    variants = _iter_mapped_variants(db, ss)
    _write_tabular(db, outfile, variants, fmt=fmt, include_header=not no_header)

    logger.info("Done")


if __name__ == "__main__":  # pragma: no cover
    main()
