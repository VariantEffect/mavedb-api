class ExternalIdentifier:
    """An identifier in an external database that a record cites, and where it resolves.

    Subclasses store ``identifier`` on the citing record's row, supply ``db_name`` as a constant or a column, and
    compute ``url`` from them. Annotation sources (ClinVar, gnomAD) aren't citations: their rows hold fetched,
    versioned data keyed by ``db_identifier``, so they don't use this.
    """

    identifier: str
    db_name: str

    @property
    def url(self) -> str:
        raise NotImplementedError
