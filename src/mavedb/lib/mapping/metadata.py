"""Helpers for extracting fields from persisted post-mapped metadata blobs."""

from typing import Any, Optional


def extract_ids_from_post_mapped_metadata(post_mapped_metadata: dict[str, Any]) -> Optional[list[str]]:
    """Return the sequence accessions recorded for the protein or cdna layer.

    UniProt annotation submission only cares about protein-coding layers.
    Genomic-layer entries are intentionally ignored: they don't carry the
    transcript-level accessions UniProt expects.
    """
    if not post_mapped_metadata:
        return None

    if "protein" in post_mapped_metadata:
        return post_mapped_metadata["protein"].get("sequence_accessions")
    if "cdna" in post_mapped_metadata:
        return post_mapped_metadata["cdna"].get("sequence_accessions")

    return None
