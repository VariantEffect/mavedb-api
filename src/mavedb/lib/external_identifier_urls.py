"""Links to the external databases that records cite through :class:`ExternalIdentifier` models.

MaveDB has no other code for these databases, so their link rules live together here. A source MaveDB also
talks to, such as gnomAD or ClinGen, keeps its link with that code. This module imports nothing from mavedb, so
models can use it.
"""

import re

import idutils

# NCBI keeps RefSeq proteins (these prefixes) in its `protein` database and everything else in `nuccore`, its
# nucleotide database. A link to the wrong one finds nothing.
REFSEQ_PROTEIN_ACCESSION = re.compile(r"^(AP|NP|WP|XP|YP)_")


def doi_url(identifier: str) -> str:
    """The resolver link for a DOI."""
    return f"https://doi.org/{identifier}"


def raw_read_url(identifier: str) -> str:
    """The page for a raw read accession, chosen by its accession type (#445)."""
    if idutils.is_bioproject(identifier):
        return f"https://www.ncbi.nlm.nih.gov/bioproject/{identifier}"
    if idutils.is_geo(identifier):
        return f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={identifier}"
    if idutils.is_arrayexpress_experiment(identifier):
        return f"https://www.ebi.ac.uk/biostudies/arrayexpress/studies/{identifier}"
    if idutils.is_arrayexpress_array(identifier):
        return f"https://www.ebi.ac.uk/biostudies/arrayexpress/arrays/{identifier}"

    # SRA accessions (SRR, ERR, DRR and their study and experiment forms) are shared across the INSDC archives,
    # and ENA's browser resolves all of them. It's also where every raw read url pointed before #445, so
    # accessions that match none of the types above keep working.
    return f"https://www.ebi.ac.uk/ena/browser/view/{identifier}"


def external_gene_identifier_url(db_name: str, identifier: str) -> str:
    """The page for a target gene's identifier in Ensembl, RefSeq or UniProt."""
    if db_name == "Ensembl":
        return f"https://www.ensembl.org/id/{identifier}"
    if db_name == "UniProt":
        return f"https://www.uniprot.org/uniprotkb/{identifier}"

    database = "protein" if REFSEQ_PROTEIN_ACCESSION.match(identifier) else "nuccore"
    return f"https://www.ncbi.nlm.nih.gov/{database}/{identifier}"
