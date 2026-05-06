# ruff: noqa: E402
"""Structural / declarative smoke tests for PIPELINE_DEFINITIONS.

These tests treat the pipeline definitions as a specification and verify
that the data satisfies its own contracts without touching the database or
any external services.
"""

import pytest

pytest.importorskip("arq")

from collections import defaultdict

from mavedb.lib.workflow.definitions import PIPELINE_DEFINITIONS
from mavedb.models.enums.job_pipeline import JobType
from mavedb.worker.jobs.registry import BACKGROUND_FUNCTIONS

# --------------------------------------------------------------------- helpers

_REGISTERED_FUNCTION_NAMES: frozenset[str] = frozenset(fn.__name__ for fn in BACKGROUND_FUNCTIONS)


def _adjacency(job_defs: list) -> dict[str, list[str]]:
    """Build a dependency adjacency map: key → list of keys it depends on."""
    return {job["key"]: [dep_key for dep_key, _ in job["dependencies"]] for job in job_defs}


def _has_cycle(adj: dict[str, list[str]]) -> list[str] | None:
    """Return the first cycle found as an ordered key list, or None if the graph is acyclic."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = defaultdict(int)
    path: list[str] = []

    def dfs(node: str) -> bool:
        color[node] = GRAY
        path.append(node)
        for neighbor in adj.get(node, []):
            if color[neighbor] == GRAY:
                path.append(neighbor)
                return True
            if color[neighbor] == WHITE and dfs(neighbor):
                return True
        path.pop()
        color[node] = BLACK
        return False

    for node in adj:
        if color[node] == WHITE and dfs(node):
            return path
    return None


# ---------------------------------------------------------------- per-pipeline


@pytest.mark.unit
class TestPipelineDefinitionsStructure:
    """Verify that every pipeline definition is internally self-consistent."""

    # --- metadata ------------------------------------------------------------

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_pipeline_has_description(self, pipeline_name: str) -> None:
        """Every pipeline must have a non-empty description."""
        description = PIPELINE_DEFINITIONS[pipeline_name]["description"]
        assert description and description.strip(), f"Pipeline '{pipeline_name}' is missing a description."

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_pipeline_has_at_least_one_job(self, pipeline_name: str) -> None:
        """Every pipeline must define at least one job."""
        assert PIPELINE_DEFINITIONS[pipeline_name][
            "job_definitions"
        ], f"Pipeline '{pipeline_name}' has no job definitions."

    # --- job field validity --------------------------------------------------

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_all_job_keys_are_non_empty_strings(self, pipeline_name: str) -> None:
        """Every job key must be a non-empty string."""
        bad = [job["key"] for job in PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"] if not job.get("key")]
        assert not bad, f"Pipeline '{pipeline_name}' has jobs with empty/missing keys."

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_all_job_functions_are_non_empty_strings(self, pipeline_name: str) -> None:
        """Every job function must be a non-empty string."""
        bad = [job["key"] for job in PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"] if not job.get("function")]
        assert not bad, f"Pipeline '{pipeline_name}' has jobs with empty/missing function names: {bad}"

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_all_job_types_are_valid_job_type_enum_values(self, pipeline_name: str) -> None:
        """Every job type must be a member of the JobType enum."""
        valid = set(JobType)
        bad = [
            (job["key"], job["type"])
            for job in PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
            if job["type"] not in valid
        ]
        assert not bad, f"Pipeline '{pipeline_name}' has jobs with invalid type values: " + ", ".join(
            f"'{k}' → '{t}'" for k, t in bad
        )

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_retry_delay_seconds_is_positive_when_present(self, pipeline_name: str) -> None:
        """retry_delay_seconds, when specified, must be a positive integer."""
        bad = [
            job["key"]
            for job in PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
            if "retry_delay_seconds" in job
            and (not isinstance(job["retry_delay_seconds"], int) or job["retry_delay_seconds"] <= 0)
        ]
        assert not bad, f"Pipeline '{pipeline_name}' has jobs with invalid retry_delay_seconds: {bad}"

    # --- param conventions ---------------------------------------------------

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_every_job_has_correlation_id_as_required_param(self, pipeline_name: str) -> None:
        """Every job must declare correlation_id as a None-valued (required) param.

        The PipelineFactory always supplies correlation_id via pipeline_params; a job
        that omits it would have the param silently ignored rather than filled in.
        """
        bad = [
            job["key"]
            for job in PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
            if job["params"].get("correlation_id", "MISSING") != None  # noqa: E711 — intentional None check
        ]
        assert not bad, f"Pipeline '{pipeline_name}' has jobs missing 'correlation_id: None' in params: {bad}"

    # --- dependency graph correctness ----------------------------------------

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_all_job_keys_are_unique_within_pipeline(self, pipeline_name: str) -> None:
        """Each job key must appear at most once within a pipeline."""
        job_defs = PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
        seen: set[str] = set()
        duplicates: list[str] = []
        for job in job_defs:
            if job["key"] in seen:
                duplicates.append(job["key"])
            seen.add(job["key"])
        assert not duplicates, f"Pipeline '{pipeline_name}' contains duplicate job keys: {duplicates}"

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_all_dependency_keys_exist_in_pipeline(self, pipeline_name: str) -> None:
        """Every dependency key referenced by a job must exist as another job's key in the same pipeline."""
        job_defs = PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
        known_keys = {job["key"] for job in job_defs}
        dangling = [
            (job["key"], dep_key) for job in job_defs for dep_key, _ in job["dependencies"] if dep_key not in known_keys
        ]
        assert not dangling, f"Pipeline '{pipeline_name}' has dangling dependency keys:\n" + "\n".join(
            f"  job '{jk}' depends on unknown '{dk}'" for jk, dk in dangling
        )

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_no_job_depends_on_itself(self, pipeline_name: str) -> None:
        """A job must not list its own key as a dependency."""
        bad = [
            job["key"]
            for job in PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
            if job["key"] in {dep_key for dep_key, _ in job["dependencies"]}
        ]
        assert not bad, f"Pipeline '{pipeline_name}' has jobs with self-dependencies: {bad}"

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_no_duplicate_dependency_declarations_per_job(self, pipeline_name: str) -> None:
        """A job must not list the same dependency key more than once."""
        bad: list[tuple[str, str]] = []
        for job in PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]:
            dep_keys = [dep_key for dep_key, _ in job["dependencies"]]
            seen: set[str] = set()
            for dk in dep_keys:
                if dk in seen:
                    bad.append((job["key"], dk))
                seen.add(dk)
        assert not bad, f"Pipeline '{pipeline_name}' has jobs with duplicate dependency declarations: " + ", ".join(
            f"'{jk}' duplicates dep '{dk}'" for jk, dk in bad
        )

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_no_cycles_in_dependency_graph(self, pipeline_name: str) -> None:
        """The dependency graph within a pipeline must be acyclic (a valid DAG)."""
        job_defs = PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
        cycle = _has_cycle(_adjacency(job_defs))
        assert cycle is None, f"Pipeline '{pipeline_name}' has a dependency cycle: {' → '.join(cycle)}"

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_dependencies_declared_before_dependent_job(self, pipeline_name: str) -> None:
        """All jobs a job depends on must appear earlier in the job list (topological order convention).

        The factory doesn't enforce ordering, but maintaining topological order in the
        source list keeps definitions readable and makes the execution sequence obvious.
        """
        job_defs = PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
        position = {job["key"]: i for i, job in enumerate(job_defs)}
        bad: list[tuple[str, str]] = []
        for job in job_defs:
            for dep_key, _ in job["dependencies"]:
                if dep_key in position and position[dep_key] >= position[job["key"]]:
                    bad.append((job["key"], dep_key))
        assert not bad, (
            f"Pipeline '{pipeline_name}' has jobs whose dependencies appear later in the list:\n"
            + "\n".join(f"  '{jk}' depends on '{dk}' which is defined after it" for jk, dk in bad)
        )

    # --- cross-system consistency --------------------------------------------

    @pytest.mark.parametrize("pipeline_name", list(PIPELINE_DEFINITIONS))
    def test_all_job_functions_are_registered_in_arq_worker(self, pipeline_name: str) -> None:
        """Every function name used in a pipeline must be registered in BACKGROUND_FUNCTIONS.

        An unregistered function would be silently ignored by ARQ and the job would
        never execute.
        """
        unregistered = [
            job["key"]
            for job in PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
            if job["function"] not in _REGISTERED_FUNCTION_NAMES
        ]
        assert not unregistered, (
            f"Pipeline '{pipeline_name}' references functions not registered in BACKGROUND_FUNCTIONS: "
            + ", ".join(
                f"job '{k}' → function '{f}'"
                for k, f in (
                    (job["key"], job["function"])
                    for job in PIPELINE_DEFINITIONS[pipeline_name]["job_definitions"]
                    if job["function"] not in _REGISTERED_FUNCTION_NAMES
                )
            )
        )


# ---------------------------------------------------------------- cross-pipeline


@pytest.mark.unit
class TestPipelineDefinitionsGlobal:
    """Verify invariants that span the full set of pipeline definitions."""

    def test_no_two_pipelines_have_identical_job_key_sets(self) -> None:
        """No two pipelines should define the exact same set of job keys.

        Identical job sets with different names suggest one pipeline is a
        duplicate of another rather than a meaningfully distinct workflow.
        """
        key_sets: dict[frozenset[str], str] = {}
        duplicates: list[tuple[str, str]] = []
        for name, defn in PIPELINE_DEFINITIONS.items():
            key_set = frozenset(job["key"] for job in defn["job_definitions"])
            if key_set in key_sets:
                duplicates.append((key_sets[key_set], name))
            else:
                key_sets[key_set] = name
        assert not duplicates, "Pipelines with identical job key sets found: " + ", ".join(
            f"'{a}' and '{b}'" for a, b in duplicates
        )
