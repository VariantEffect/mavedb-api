"""Constants used when ingesting dcd-mapping payloads."""

# ``computed_reference_sequence.sequence`` is the raw (potentially very large)
# nucleotide / amino-acid sequence; we strip it before persisting layer
# metadata to avoid bloating ``target_genes.pre_mapped_metadata``.
EXCLUDED_PREMAPPED_ANNOTATION_KEYS = {"sequence"}
