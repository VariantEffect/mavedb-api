# ruff: noqa: E402
"""Query-count canary for every GET route with path parameters.

Each route is called against two seeded score sets, one small and one four times larger, and must run no
more queries against the larger. A route whose count grows with its data is an N+1, which is invisible at
fixture sizes until it reaches a score set in production.

Routes are found in the OpenAPI schema, so a new route is covered without a test of its own. Path
parameters are resolved by ``(resource segment, parameter)``: the segment before the parameter in the
path, since ``{urn}`` alone names a dozen different resources. A route under a resource the map doesn't
know fails, naming the pair to add to ``_RESOLVERS``, or a reason to add to ``_EXEMPT``.
"""

import base64
import hashlib
import re
from dataclasses import dataclass
from unittest.mock import patch
from urllib.parse import quote

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from sqlalchemy import select

from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant as VariantDbModel
from mavedb.server_main import app
from tests.helpers.constants import (
    TEST_BIORXIV_IDENTIFIER,
    TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_PUBMED_IDENTIFIER,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
)
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record
from tests.helpers.util.common import deepcamelize
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.query_plan import captured_statements
from tests.helpers.util.score_calibration import create_publish_and_promote_score_calibration
from tests.helpers.util.score_set import create_seq_score_set, publish_score_set
from tests.helpers.util.variant import mock_worker_variant_insertion

# Resource segments whose routes don't read the score set's variant data, or reach a service the test
# environment doesn't run. Each needs a reason.
_EXEMPT = {
    "hgvs": "resolves through cdot, an external service",
    "refget": "reads SeqRepo, not the variant data",
    "seqrepo": "reads SeqRepo, not the variant data",
    "orcid": "calls ORCID",
    "publication-identifiers": "calls the publication registries",
    "genes": "reads the gene normalizer, not the variant data",
    "taxonomies": "reference data, not variant data",
    "licenses": "reference data, not variant data",
    "target-genes": "one target gene; not variant data",
    "controlled-keywords": "reference data, not variant data",
    "users": "one user; not variant data",
    "collections": "one collection's metadata; not variant data",
    "statistics": "site-wide counts, tracked in api#900",
    "user-is-permitted": "one permission check; not variant data",
    "job-runs": "one job run; not variant data",
    "pipelines": "one pipeline; not variant data",
}


@dataclass(frozen=True)
class _World:
    """The identifiers of one seeded score set and the records hanging off it."""

    experiment_set_urn: str
    experiment_urn: str
    score_set_urn: str
    variant_urn: str
    measured_digest: str
    protein_clingen_allele_id: str
    calibration_urn: str
    classification_id: int


_RESOLVERS = {
    ("experiment-sets", "urn"): lambda world: world.experiment_set_urn,
    ("experiments", "urn"): lambda world: world.experiment_urn,
    ("score-sets", "urn"): lambda world: world.score_set_urn,
    ("score-set", "score_set_urn"): lambda world: world.score_set_urn,
    ("variants", "urn"): lambda world: world.variant_urn,
    ("mapped-variants", "urn"): lambda world: world.variant_urn,
    ("vrs", "identifier"): lambda world: world.measured_digest,
    ("alleles", "identifier"): lambda world: world.measured_digest,
    # The protein consequence every variant in the world shares, so its measurements grow with the world.
    ("clingen-alleles", "clingen_allele_id"): lambda world: world.protein_clingen_allele_id,
    ("score-calibrations", "urn"): lambda world: world.calibration_urn,
    ("functional-classifications", "classification_id"): lambda world: world.classification_id,
}

# Routes that answer every request with a fixed status by design, so there is nothing to scale.
_RETIRED = {
    "/api/v1/score-sets/{urn}/mapped-variants",
}

_PARAMETER = re.compile(r"{([^}]+)}")

_PUBLICATION_FETCH = [
    {"dbName": "PubMed", "identifier": TEST_PUBMED_IDENTIFIER},
    {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
]


def _parameterized_get_routes() -> list[str]:
    return sorted(
        path
        for path, operations in app.openapi()["paths"].items()
        if "get" in operations and _PARAMETER.search(path) and path not in _RETIRED
    )


def _resolve(path: str, world: _World) -> str:
    """The concrete URL for ``path`` in ``world``. Raises ``KeyError`` naming the unresolved pair."""
    segments = path.split("/")

    def substitute(match: re.Match) -> str:
        parameter = match.group(1)
        index = segments.index(match.group(0))
        resolver = _RESOLVERS[(segments[index - 1], parameter)]
        return quote(str(resolver(world)), safe=":")

    return _PARAMETER.sub(substitute, path)


def _exemption(path: str) -> str | None:
    return next((reason for segment, reason in _EXEMPT.items() if f"/{segment}/" in path), None)


def _digest(text: str) -> str:
    """A well-formed GA4GH VRS identifier, unique to ``text``."""
    return "ga4gh:VA." + base64.urlsafe_b64encode(hashlib.sha512(text.encode()).digest()[:24]).decode()


def _seed_world(client, session, data_provider, scores_csv, label: str) -> _World:
    """A published score set whose variants each carry a coding measurement with VEP, gnomAD and ClinVar
    records, its genomic projection, a convergent encoding and a protein consequence they all share, under a
    published primary calibration that classifies every variant."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set(client, experiment["urn"])
    mock_worker_variant_insertion(client, session, data_provider, score_set, scores_csv)
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        score_set = publish_score_set(client, score_set["urn"])
    calibration = create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )

    variants = session.scalars(
        select(VariantDbModel)
        .join(ScoreSetDbModel)
        .where(ScoreSetDbModel.urn == score_set["urn"])
        .order_by(VariantDbModel.id)
    ).all()
    vrs = TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X
    for index, variant in enumerate(variants):
        seed_mapping_record(
            session,
            variant,
            hgvs_assay_level=f"NM_000546.6:c.{1000 + index}G>A",
            alleles=[
                AlleleSpec(
                    digest=_digest(f"{label}-measured-{index}"),
                    is_authoritative=True,
                    clingen_allele_id=f"CA{label}{index}",
                    projection_group=0,
                    vep_consequence="missense_variant",
                    clinvar_control_ids=(1,),
                    gnomad_variant_ids=(1,),
                    post_mapped=vrs,
                ),
                AlleleSpec(
                    digest=_digest(f"{label}-genomic-{index}"), level="genomic", projection_group=0, post_mapped=vrs
                ),
                AlleleSpec(digest=_digest(f"{label}-convergent-{index}"), projection_group=1, post_mapped=vrs),
                AlleleSpec(
                    digest=_digest(f"{label}-protein"), level="protein", clingen_allele_id=f"PA{label}", post_mapped=vrs
                ),
            ],
        )

    return _World(
        experiment_set_urn=experiment["experimentSetUrn"],
        experiment_urn=experiment["urn"],
        score_set_urn=score_set["urn"],
        variant_urn=variants[0].urn,
        measured_digest=_digest(f"{label}-measured-0"),
        protein_clingen_allele_id=f"PA{label}",
        calibration_urn=calibration["urn"],
        classification_id=calibration["functionalClassifications"][0]["id"],
    )


def _scores_csv(tmp_path, count: int):
    """A scores file of ``count`` single-base substitutions against the minimal target (``ACGTTT``), scored
    in the calibration's abnormal range so each variant carries pathogenicity evidence."""
    reference = "ACGTTT"
    substitutions = [(position, base, alt) for position, base in enumerate(reference) for alt in "ACGT" if alt != base][
        :count
    ]
    rows = [
        f"n.{position + 1}{base}>{alt},{-2.0 - index / 10}" for index, (position, base, alt) in enumerate(substitutions)
    ]
    path = tmp_path / f"scores_{count}.csv"
    path.write_text("hgvs_nt,score\n" + "\n".join(rows) + "\n")
    return path


@pytest.fixture
def worlds(client, session, data_provider, setup_router_db, mock_publication_fetch, tmp_path):
    small = _seed_world(client, session, data_provider, _scores_csv(tmp_path, 3), "small")
    large = _seed_world(client, session, data_provider, _scores_csv(tmp_path, 12), "large")
    return small, large


def _query_count(client, session, url: str) -> tuple[int, int]:
    with captured_statements(session) as statements:
        response = client.get(url)
    return response.status_code, len(statements)


def test_every_parameterized_get_route_is_resolved_or_exempt():
    """A new route under an unknown resource needs a resolver or an exemption reason, not a silent skip."""
    unresolved = set()
    for path in _parameterized_get_routes():
        if _exemption(path) is not None:
            continue
        segments = path.split("/")
        for match in _PARAMETER.finditer(path):
            key = (segments[segments.index(match.group(0)) - 1], match.group(1))
            if key not in _RESOLVERS:
                unresolved.add((path, key))

    assert not unresolved, f"Add a resolver to _RESOLVERS or a reason to _EXEMPT for: {sorted(unresolved)}"


@pytest.mark.parametrize("mock_publication_fetch", [_PUBLICATION_FETCH], indirect=True)
def test_no_route_runs_more_queries_on_a_larger_score_set(client, session, worlds):
    small, large = worlds
    failures = []
    for path in _parameterized_get_routes():
        if _exemption(path) is not None:
            continue

        small_status, small_count = _query_count(client, session, _resolve(path, small))
        large_status, large_count = _query_count(client, session, _resolve(path, large))
        if small_status >= 400 or large_status >= 400:
            failures.append(f"{path}: status {small_status}/{large_status}, so its query count isn't comparable")
        elif large_count > small_count:
            failures.append(f"{path}: {small_count} queries on the small score set, {large_count} on the large one")

    assert not failures, "\n".join(failures)
