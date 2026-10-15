# ruff: noqa: E402
import pytest

pytest.importorskip("arq")

from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.worker.lib.projection import MeasuredHgvs, project_measured_hgvs


class FakeCoordinates:
    """Answers the projections in ``answers``; anything else raises, like hgvs on unknown input."""

    def __init__(self, answers: dict[tuple[str, str], str]):
        self.answers = answers

    def _answer(self, op: str, hgvs: str) -> str:
        if (op, hgvs) not in self.answers:
            raise ValueError(f"no {op} for {hgvs}")
        return self.answers[(op, hgvs)]

    def g_to_c(self, g_hgvs: str, transcript: str) -> str:
        return self._answer("g_to_c", g_hgvs)

    def normalize(self, hgvs_string: str) -> str:
        return hgvs_string

    def c_to_g(self, c_hgvs: str) -> str:
        return self._answer("c_to_g", c_hgvs)

    def c_to_p(self, c_hgvs: str) -> str:
        return self._answer("c_to_p", c_hgvs)


MULTI_CODON_G = "NC_000008.11:g.95025143_95025145delinsAAC"
MULTI_CODON_C = "NM_152416.4:c.135_137delinsAAC"
INTRONIC_G = "NC_000002.12:g.214809442A>G"
INTRONIC_C = "NM_000465.4:c.123+5A>G"


@pytest.mark.unit
class TestProjectMeasuredHgvs:
    def test_genomic_measurement_projects_to_coding_and_protein(self):
        coordinates = FakeCoordinates(
            {
                ("g_to_c", MULTI_CODON_G): MULTI_CODON_C,
                ("c_to_p", MULTI_CODON_C): "NP_689629.2:p.(Ser45_Val46delinsArgThr)",
            }
        )

        measured, errors = project_measured_hgvs(
            SequenceLevel.genomic, MULTI_CODON_G, transcript="NM_152416.4", coordinates=coordinates
        )

        # A multi-codon change reverse translation cannot reach still gets its coding coordinate, and the
        # prediction parens are stripped like reverse translation's.
        assert measured == MeasuredHgvs(
            hgvs_c=MULTI_CODON_C, hgvs_g=MULTI_CODON_G, hgvs_p="NP_689629.2:p.Ser45_Val46delinsArgThr"
        )
        assert errors == []

    def test_intronic_change_keeps_its_coding_coordinate_without_a_protein(self):
        coordinates = FakeCoordinates({("g_to_c", INTRONIC_G): INTRONIC_C, ("c_to_p", INTRONIC_C): "NP_000456.2:p.?"})

        measured, errors = project_measured_hgvs(
            SequenceLevel.genomic, INTRONIC_G, transcript="NM_000465.4", coordinates=coordinates
        )

        assert measured == MeasuredHgvs(hgvs_c=INTRONIC_C, hgvs_g=INTRONIC_G, hgvs_p=None)
        assert errors == []

    def test_coding_measurement_projects_to_genomic_and_protein(self):
        coordinates = FakeCoordinates(
            {
                ("c_to_g", MULTI_CODON_C): MULTI_CODON_G,
                ("c_to_p", MULTI_CODON_C): "NP_689629.2:p.(Ser45_Val46delinsArgThr)",
            }
        )

        measured, _ = project_measured_hgvs(SequenceLevel.cdna, MULTI_CODON_C, transcript=None, coordinates=coordinates)

        assert measured.hgvs_g == MULTI_CODON_G
        assert measured.hgvs_c == MULTI_CODON_C

    def test_genomic_measurement_without_a_transcript_has_only_its_genomic_hgvs(self):
        measured, errors = project_measured_hgvs(
            SequenceLevel.genomic, MULTI_CODON_G, transcript=None, coordinates=FakeCoordinates({})
        )

        assert measured == MeasuredHgvs(hgvs_g=MULTI_CODON_G)
        assert errors == []

    def test_failed_protein_keeps_the_coding_projection(self):
        coordinates = FakeCoordinates({("g_to_c", MULTI_CODON_G): MULTI_CODON_C})

        measured, errors = project_measured_hgvs(
            SequenceLevel.genomic, MULTI_CODON_G, transcript="NM_152416.4", coordinates=coordinates
        )

        assert measured == MeasuredHgvs(hgvs_c=MULTI_CODON_C, hgvs_g=MULTI_CODON_G)
        assert len(errors) == 1 and errors[0].startswith("c_to_p")

    def test_protein_measurement_is_its_own_description(self):
        measured, errors = project_measured_hgvs(
            SequenceLevel.protein, "NP_689629.2:p.Val46Ala", transcript=None, coordinates=FakeCoordinates({})
        )

        assert measured == MeasuredHgvs(hgvs_p="NP_689629.2:p.Val46Ala")
        assert errors == []

    def test_genomic_measurement_is_normalized_before_it_is_projected(self):
        """An untrimmed delins at an exon boundary projects to the coding deletion it is, not to an
        intron-spanning delins with no protein consequence."""
        raw = "NC_000010.11:g.87965286_87965289delinsG"
        trimmed = "NC_000010.11:g.87965287_87965289del"

        class Normalizing(FakeCoordinates):
            def normalize(self, hgvs_string: str) -> str:
                return trimmed if hgvs_string == raw else hgvs_string

        coordinates = Normalizing(
            {
                ("g_to_c", trimmed): "NM_000314.8:c.1027_1029del",
                ("c_to_p", "NM_000314.8:c.1027_1029del"): "NP_000305.3:p.(Val343del)",
            }
        )

        measured, errors = project_measured_hgvs(
            SequenceLevel.genomic, raw, transcript="NM_000314.8", coordinates=coordinates
        )

        assert measured == MeasuredHgvs(
            hgvs_c="NM_000314.8:c.1027_1029del", hgvs_g=trimmed, hgvs_p="NP_000305.3:p.Val343del"
        )
        assert errors == []

    def test_an_inversion_is_projected_as_the_equivalent_delins(self):
        """VRS cannot represent ``inv``; hgvs's normalizing mapper writes one, so the coding projection is
        taken from the unnormalized mapper instead."""
        genomic = "NC_000014.9:g.90403898_90403899delinsTG"

        class Literal(FakeCoordinates):
            def g_to_c_literal(self, g_hgvs: str, transcript: str) -> str:
                return "NM_006888.6:c.215_216delinsCA"

        coordinates = Literal(
            {
                ("g_to_c", genomic): "NM_006888.6:c.215_216inv",
                ("c_to_p", "NM_006888.6:c.215_216delinsCA"): "NP_008819.1:p.(Met72Thr)",
            }
        )

        measured, errors = project_measured_hgvs(
            SequenceLevel.genomic, genomic, transcript="NM_006888.6", coordinates=coordinates
        )

        assert measured.hgvs_c == "NM_006888.6:c.215_216delinsCA"
        assert measured.hgvs_p == "NP_008819.1:p.Met72Thr"
        assert errors == []

    def test_a_reference_identity_is_projected_as_written(self):
        """Normalizing ``g.100CCT=`` would collapse its three-base span to one position."""
        identity = "NC_000007.14:g.144548593_144548595="

        class Collapsing(FakeCoordinates):
            def normalize(self, hgvs_string: str) -> str:
                return "NC_000007.14:g.144548593="

        coordinates = Collapsing({("g_to_c", identity): "NM_022445.4:c.610_612="})

        measured, _ = project_measured_hgvs(
            SequenceLevel.genomic, identity, transcript="NM_022445.4", coordinates=coordinates
        )

        assert measured.hgvs_g == identity
        assert measured.hgvs_c == "NM_022445.4:c.610_612="

    def test_a_spanless_multi_base_identity_is_given_its_span(self):
        """Legacy ``g.100CCT=`` names one position with a three-base reference, which hgvs rejects."""
        coordinates = FakeCoordinates(
            {
                ("g_to_c", "NC_000007.14:g.144548593_144548595="): "NM_022445.4:c.509_511=",
                ("c_to_p", "NM_022445.4:c.509_511="): "NP_071890.2:p.(His170=)",
            }
        )

        measured, errors = project_measured_hgvs(
            SequenceLevel.genomic, "NC_000007.14:g.144548593CCT=", transcript="NM_022445.4", coordinates=coordinates
        )

        assert measured == MeasuredHgvs(
            hgvs_c="NM_022445.4:c.509_511=",
            hgvs_g="NC_000007.14:g.144548593_144548595=",
            hgvs_p="NP_071890.2:p.His170=",
        )
        assert errors == []

    def test_a_synonymous_stop_is_written_with_equals(self):
        coordinates = FakeCoordinates(
            {
                ("g_to_c", "NC_000002.12:g.214728677C>T"): "NM_000465.4:c.2333G>A",
                ("c_to_p", "NM_000465.4:c.2333G>A"): "NP_000456.2:p.(Ter778Ter)",
            }
        )

        measured, _ = project_measured_hgvs(
            SequenceLevel.genomic, "NC_000002.12:g.214728677C>T", transcript="NM_000465.4", coordinates=coordinates
        )

        assert measured.hgvs_p == "NP_000456.2:p.Ter778="
