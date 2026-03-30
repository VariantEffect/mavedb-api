import enum


class ACMGCriterion(enum.Enum):
    """Enum for ACMG criteria codes."""

    PVS1 = "PVS1"
    PS1 = "PS1"
    PS2 = "PS2"
    PS3 = "PS3"
    PS4 = "PS4"
    PM1 = "PM1"
    PM2 = "PM2"
    PM3 = "PM3"
    PM4 = "PM4"
    PM5 = "PM5"
    PM6 = "PM6"
    PP1 = "PP1"
    PP2 = "PP2"
    PP3 = "PP3"
    PP4 = "PP4"
    PP5 = "PP5"
    BA1 = "BA1"
    BS1 = "BS1"
    BS2 = "BS2"
    BS3 = "BS3"
    BS4 = "BS4"
    BP1 = "BP1"
    BP2 = "BP2"
    BP3 = "BP3"
    BP4 = "BP4"
    BP5 = "BP5"
    BP6 = "BP6"
    BP7 = "BP7"

    @property
    def is_pathogenic(self) -> bool:
        """Return True if the criterion is pathogenic, False if benign."""
        return self.name.startswith("P")  # PVS, PS, PM, PP are pathogenic criteria

    @property
    def is_benign(self) -> bool:
        """Return True if the criterion is benign, False if pathogenic."""
        return self.name.startswith("B")  # BA, BS, BP are benign criteria
