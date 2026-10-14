from enum import Enum


class MeasurementRelationship(str, Enum):
    """How a measurement relates to the queried ClinGen id, by the measurement's *measured* level."""

    direct = "direct"  # assayed at the queried allele
    protein_consequence = "protein_consequence"  # protein measurement of a CA's consequence
    nucleotide_encoding = "nucleotide_encoding"  # an nt measurement of the protein apex's encoding
