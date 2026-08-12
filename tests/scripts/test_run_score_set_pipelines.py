# ruff: noqa: E402

from datetime import date, datetime, timezone

import pytest

pytest.importorskip("arq")

from mavedb.lib.workflow.definitions import PIPELINE_DEFINITIONS
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.scripts.run_score_set_pipelines import (
    build_cohort_items,
    classify_status,
    effective_pipeline_name,
    grouping_key,
    in_flight_pipelines,
    is_current,
    is_failure,
    normalize_gene,
    order_cohort,
    pipelines_by_score_set,
    plan_enqueue,
    resolve_cohort,
    resolve_job_subset,
)


def _make_pipeline(session, **overrides) -> Pipeline:
    defaults = {
        "name": "test_pipeline",
        "description": "test pipeline description",
        "status": PipelineStatus.RUNNING,
        "correlation_id": "corr-1",
    }
    defaults.update(overrides)
    pipeline = Pipeline(**defaults)
    session.add(pipeline)
    session.commit()
    session.refresh(pipeline)
    return pipeline


def _make_job_run(session, pipeline_id=None, score_set_id=None, **overrides) -> JobRun:
    defaults = {
        "job_type": "mapped_variant_annotation",
        "job_function": "submit_score_set_mappings_to_car",
        "status": JobStatus.PENDING,
        "pipeline_id": pipeline_id,
        "correlation_id": "corr-1",
        "max_retries": 3,
        "retry_count": 0,
        "job_params": {"score_set_id": score_set_id} if score_set_id is not None else {},
    }
    defaults.update(overrides)
    job_run = JobRun(**defaults)
    session.add(job_run)
    session.commit()
    session.refresh(job_run)
    return job_run


####################################################################################################
# Pure functions
####################################################################################################


@pytest.mark.unit
class TestNormalizeGene:
    def test_strips_and_lowercases(self):
        assert normalize_gene("  BRCA1 ") == "brca1"

    def test_empty_string(self):
        assert normalize_gene("") == ""
        assert normalize_gene("   ") == ""


@pytest.mark.unit
class TestOrderCohort:
    class _FakeScoreSet:
        def __init__(self, urn):
            self.urn = urn

    def test_groups_same_key_adjacently_and_sorts_by_urn(self):
        a = (self._FakeScoreSet("urn:2"), ["brca1"])
        b = (self._FakeScoreSet("urn:1"), ["brca1"])
        c = (self._FakeScoreSet("urn:3"), ["tp53"])
        ordered = order_cohort([c, a, b])
        assert [item[0].urn for item in ordered] == ["urn:1", "urn:2", "urn:3"]

    def test_mixed_case_groups_together(self):
        # order_cohort itself doesn't normalize; callers pass pre-normalized names via
        # build_cohort_items(normalize_gene(...)). Grouping only works if genes arrive normalized.
        a = (self._FakeScoreSet("urn:1"), [normalize_gene("BRCA1")])
        b = (self._FakeScoreSet("urn:2"), [normalize_gene("brca1")])
        ordered = order_cohort([a, b])
        assert grouping_key(ordered[0][1]) == grouping_key(ordered[1][1])

    def test_no_gene_sentinel_sorts_first(self):
        with_gene = (self._FakeScoreSet("urn:2"), ["aaa"])
        without_gene = (self._FakeScoreSet("urn:1"), [])
        ordered = order_cohort([with_gene, without_gene])
        assert ordered[0][0].urn == "urn:1"

    def test_urn_tiebreak_within_shared_key(self):
        a = (self._FakeScoreSet("urn:b"), ["brca1", "tp53"])
        b = (self._FakeScoreSet("urn:a"), ["brca1"])
        ordered = order_cohort([a, b])
        assert [item[0].urn for item in ordered] == ["urn:a", "urn:b"]


@pytest.mark.unit
class TestClassifyStatus:
    @pytest.mark.parametrize(
        "status,expected",
        [
            (PipelineStatus.SUCCEEDED, "terminal"),
            (PipelineStatus.FAILED, "terminal"),
            (PipelineStatus.PARTIAL, "terminal"),
            (PipelineStatus.CANCELLED, "terminal"),
            (PipelineStatus.CREATED, "in_flight"),
            (PipelineStatus.RUNNING, "in_flight"),
            (PipelineStatus.PAUSED, "in_flight"),
        ],
    )
    def test_classifies_all_seven_statuses(self, status, expected):
        assert classify_status(status) == expected

    def test_all_members_covered_exhaustively(self):
        for status in PipelineStatus:
            # Should not raise for any real PipelineStatus member.
            classify_status(status)


@pytest.mark.unit
class TestIsFailure:
    @pytest.mark.parametrize(
        "status,expected",
        [
            (PipelineStatus.FAILED, True),
            (PipelineStatus.PARTIAL, True),
            (PipelineStatus.CANCELLED, False),
            (PipelineStatus.SUCCEEDED, False),
            (PipelineStatus.CREATED, False),
            (PipelineStatus.RUNNING, False),
            (PipelineStatus.PAUSED, False),
        ],
    )
    def test_only_failed_and_partial_are_failures(self, status, expected):
        assert is_failure(status) == expected


@pytest.mark.unit
class TestIsCurrent:
    def test_current_since_none_disables_skip_if_current(self):
        assert is_current(PipelineStatus.SUCCEEDED, datetime.now(timezone.utc), None) is False

    def test_succeeded_before_cutoff_is_not_current(self):
        finished = datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert is_current(PipelineStatus.SUCCEEDED, finished, date(2024, 1, 2)) is False

    def test_succeeded_on_or_after_cutoff_is_current(self):
        finished = datetime(2024, 1, 2, tzinfo=timezone.utc)
        assert is_current(PipelineStatus.SUCCEEDED, finished, date(2024, 1, 2)) is True

    @pytest.mark.parametrize("status", [PipelineStatus.CANCELLED, PipelineStatus.FAILED, PipelineStatus.PARTIAL])
    def test_non_succeeded_never_current_regardless_of_finished_at(self, status):
        finished = datetime(2099, 1, 1, tzinfo=timezone.utc)
        assert is_current(status, finished, date(2024, 1, 1)) is False

    def test_succeeded_with_no_finished_at_is_not_current(self):
        assert is_current(PipelineStatus.SUCCEEDED, None, date(2024, 1, 1)) is False


@pytest.mark.unit
class TestPlanEnqueue:
    class _FakeScoreSet:
        def __init__(self, id_, urn):
            self.id = id_
            self.urn = urn

    def _cohort(self, n):
        return [(self._FakeScoreSet(i, f"urn:{i}"), []) for i in range(1, n + 1)]

    def test_zero_slots_skips_everything_as_cap(self):
        plan = plan_enqueue(
            self._cohort(2), in_flight_score_set_ids=set(), current_score_set_ids=set(), slots=0, limit=None
        )
        assert [decision for _ss, _key, decision in plan] == ["skip_cap", "skip_cap"]

    def test_in_flight_skips_without_consuming_slot(self):
        cohort = self._cohort(2)
        plan = plan_enqueue(cohort, in_flight_score_set_ids={1}, current_score_set_ids=set(), slots=1, limit=None)
        decisions = {ss.id: decision for ss, _key, decision in plan}
        assert decisions[1] == "skip_in_flight"
        assert decisions[2] == "enqueue"

    def test_current_skips_without_consuming_slot(self):
        cohort = self._cohort(2)
        plan = plan_enqueue(cohort, in_flight_score_set_ids=set(), current_score_set_ids={1}, slots=1, limit=None)
        decisions = {ss.id: decision for ss, _key, decision in plan}
        assert decisions[1] == "skip_current"
        assert decisions[2] == "enqueue"

    def test_limit_caps_below_slots(self):
        plan = plan_enqueue(
            self._cohort(3), in_flight_score_set_ids=set(), current_score_set_ids=set(), slots=3, limit=1
        )
        decisions = [decision for _ss, _key, decision in plan]
        assert decisions == ["enqueue", "skip_cap", "skip_cap"]

    def test_later_entry_enqueues_after_earlier_skip(self):
        cohort = self._cohort(2)
        plan = plan_enqueue(cohort, in_flight_score_set_ids={1}, current_score_set_ids=set(), slots=1, limit=None)
        decisions = {ss.id: decision for ss, _key, decision in plan}
        assert decisions[1] == "skip_in_flight"
        assert decisions[2] == "enqueue"


@pytest.mark.unit
class TestResolveJobSubset:
    def test_caid_leaf_resolves_against_map_annotate_score_set(self):
        jobs = PIPELINE_DEFINITIONS["map_annotate_score_set"]["job_definitions"]
        subset = resolve_job_subset(jobs, frozenset({"submit_score_set_mappings_to_car"}))
        assert {j["key"] for j in subset} == {"map_variants_for_score_set", "submit_score_set_mappings_to_car"}

    def test_fast_annotate_leaf_resolves_against_map_annotate_score_set(self):
        jobs = PIPELINE_DEFINITIONS["map_annotate_score_set"]["job_definitions"]
        leaf = frozenset(
            {
                "link_gnomad_variants",
                "refresh_clinvar_controls",
                "populate_hgvs_for_score_set",
                "populate_variant_translations_for_score_set",
                "submit_uniprot_mapping_jobs_for_score_set",
                "poll_uniprot_mapping_jobs_for_score_set",
            }
        )
        subset = resolve_job_subset(jobs, leaf)
        assert {j["key"] for j in subset} == {
            "map_variants_for_score_set",
            "submit_score_set_mappings_to_car",
            "warm_clingen_cache",
            "link_gnomad_variants",
            "refresh_clinvar_controls",
            "populate_hgvs_for_score_set",
            "populate_variant_translations_for_score_set",
            "submit_uniprot_mapping_jobs_for_score_set",
            "poll_uniprot_mapping_jobs_for_score_set",
        }

    # TODO(#772)
    @pytest.mark.skip(reason="vep currently disabled")
    def test_vep_leaf_resolves_against_map_annotate_score_set(self):
        jobs = PIPELINE_DEFINITIONS["map_annotate_score_set"]["job_definitions"]
        subset = resolve_job_subset(jobs, frozenset({"populate_vep_for_score_set"}))
        assert {j["key"] for j in subset} == {
            "map_variants_for_score_set",
            "submit_score_set_mappings_to_car",
            "populate_vep_for_score_set",
        }

    @pytest.mark.parametrize(
        "leaf",
        [
            frozenset({"submit_score_set_mappings_to_car"}),
            frozenset(
                {
                    "link_gnomad_variants",
                    "refresh_clinvar_controls",
                    "populate_hgvs_for_score_set",
                    "populate_variant_translations_for_score_set",
                    "submit_uniprot_mapping_jobs_for_score_set",
                    "poll_uniprot_mapping_jobs_for_score_set",
                }
            ),
            # TODO(#772)
            # frozenset({"populate_vep_for_score_set"}),
        ],
    )
    def test_presets_against_annotate_score_set_exclude_mapping_job(self, leaf):
        jobs = PIPELINE_DEFINITIONS["annotate_score_set"]["job_definitions"]
        subset = resolve_job_subset(jobs, leaf)
        assert "map_variants_for_score_set" not in {j["key"] for j in subset}

    def test_missing_leaf_key_raises(self):
        jobs = PIPELINE_DEFINITIONS["publish_score_set"]["job_definitions"]
        with pytest.raises(ValueError):
            resolve_job_subset(jobs, frozenset({"populate_vep_for_score_set"}))

    # TODO(#772)
    @pytest.mark.skip(reason="vep currently disabled")
    def test_preserves_base_pipeline_order(self):
        jobs = PIPELINE_DEFINITIONS["map_annotate_score_set"]["job_definitions"]
        subset = resolve_job_subset(jobs, frozenset({"populate_vep_for_score_set"}))
        base_order = [j["key"] for j in jobs]
        subset_keys = [j["key"] for j in subset]
        assert subset_keys == [k for k in base_order if k in subset_keys]


@pytest.mark.unit
class TestEffectivePipelineName:
    def test_no_phase_returns_pipeline_name_unchanged(self):
        assert effective_pipeline_name("map_annotate_score_set", None) == "map_annotate_score_set"

    def test_phase_appends_suffix(self):
        assert effective_pipeline_name("map_annotate_score_set", "caid") == "map_annotate_score_set:caid"


####################################################################################################
# DB-backed
####################################################################################################


@pytest.mark.integration
class TestCurrentSinceQuerying:
    def test_succeeded_before_current_since_not_current(self, session, make_score_set):
        score_set = make_score_set()
        pipeline = _make_pipeline(
            session, status=PipelineStatus.SUCCEEDED, finished_at=datetime(2024, 1, 1, tzinfo=timezone.utc)
        )
        _make_job_run(session, pipeline_id=pipeline.id, score_set_id=score_set.id)

        result = pipelines_by_score_set(
            session, tracked_name="test_pipeline", score_set_ids=[score_set.id], statuses=[PipelineStatus.SUCCEEDED]
        )
        assert not any(is_current(p.status, p.finished_at, date(2024, 1, 2)) for p in result.get(score_set.id, []))

    def test_succeeded_on_or_after_current_since_is_current(self, session, make_score_set):
        score_set = make_score_set()
        pipeline = _make_pipeline(
            session, status=PipelineStatus.SUCCEEDED, finished_at=datetime(2024, 1, 2, tzinfo=timezone.utc)
        )
        _make_job_run(session, pipeline_id=pipeline.id, score_set_id=score_set.id)

        result = pipelines_by_score_set(
            session, tracked_name="test_pipeline", score_set_ids=[score_set.id], statuses=[PipelineStatus.SUCCEEDED]
        )
        assert any(is_current(p.status, p.finished_at, date(2024, 1, 2)) for p in result.get(score_set.id, []))


@pytest.mark.integration
class TestInFlightAndDedup:
    def test_running_pipeline_with_no_succeeded_history_is_in_flight(self, session, make_score_set):
        score_set = make_score_set()
        pipeline = _make_pipeline(session, status=PipelineStatus.RUNNING)
        _make_job_run(session, pipeline_id=pipeline.id, score_set_id=score_set.id)

        rows = in_flight_pipelines(session, tracked_name="test_pipeline")
        assert (pipeline.id, score_set.id) in {(p.id, ss_id) for p, ss_id in rows}

    def test_one_pipeline_two_job_runs_same_score_set_returned_once(self, session, make_score_set):
        score_set = make_score_set()
        pipeline = _make_pipeline(session, status=PipelineStatus.RUNNING)
        _make_job_run(
            session, pipeline_id=pipeline.id, score_set_id=score_set.id, job_function="submit_score_set_mappings_to_car"
        )
        _make_job_run(session, pipeline_id=pipeline.id, score_set_id=score_set.id, job_function="warm_clingen_cache")

        rows = in_flight_pipelines(session, tracked_name="test_pipeline")
        matching = [r for r in rows if r[0].id == pipeline.id]
        assert len(matching) == 1

    def test_start_pipeline_job_run_does_not_produce_none_score_set_id(self, session, make_score_set):
        """Every real Pipeline has a start_pipeline JobRun with job_params={}, so
        job_params["score_set_id"] is SQL NULL for that row. Dedup-by-pipeline.id must not
        nondeterministically pick that row over the one that actually carries score_set_id."""
        score_set = make_score_set()
        pipeline = _make_pipeline(session, status=PipelineStatus.RUNNING)
        _make_job_run(session, pipeline_id=pipeline.id, job_params={}, job_function="start_pipeline")
        _make_job_run(
            session, pipeline_id=pipeline.id, score_set_id=score_set.id, job_function="submit_score_set_mappings_to_car"
        )

        rows = in_flight_pipelines(session, tracked_name="test_pipeline")
        matching = [(p.id, ss_id) for p, ss_id in rows if p.id == pipeline.id]
        assert matching == [(pipeline.id, score_set.id)]

    def test_score_set_id_exact_match_not_prefix(self, session, make_score_set):
        score_set_1 = make_score_set()
        score_set_12_pipeline = _make_pipeline(session, name="other_pipeline", status=PipelineStatus.RUNNING)
        _make_job_run(session, pipeline_id=score_set_12_pipeline.id, score_set_id=12)

        result = pipelines_by_score_set(session, tracked_name="other_pipeline", score_set_ids=[score_set_1.id])
        assert score_set_1.id not in result

    def test_cancelled_pipeline_not_current_and_not_a_failure(self, session, make_score_set):
        score_set = make_score_set()
        pipeline = _make_pipeline(
            session, status=PipelineStatus.CANCELLED, finished_at=datetime(2099, 1, 1, tzinfo=timezone.utc)
        )
        _make_job_run(session, pipeline_id=pipeline.id, score_set_id=score_set.id)

        assert is_current(pipeline.status, pipeline.finished_at, date(2024, 1, 1)) is False
        assert is_failure(pipeline.status) is False

    def test_partial_pipeline_not_current_but_is_a_failure(self, session, make_score_set):
        score_set = make_score_set()
        pipeline = _make_pipeline(
            session, status=PipelineStatus.PARTIAL, finished_at=datetime(2099, 1, 1, tzinfo=timezone.utc)
        )
        _make_job_run(session, pipeline_id=pipeline.id, score_set_id=score_set.id)

        assert is_current(pipeline.status, pipeline.finished_at, date(2024, 1, 1)) is False
        assert is_failure(pipeline.status) is True

    def test_phase_and_base_pipeline_tracked_independently(self, session, make_score_set):
        score_set = make_score_set()
        caid_pipeline = _make_pipeline(
            session,
            name="map_annotate_score_set:caid",
            status=PipelineStatus.SUCCEEDED,
            finished_at=datetime.now(timezone.utc),
        )
        _make_job_run(session, pipeline_id=caid_pipeline.id, score_set_id=score_set.id)

        succeeded = pipelines_by_score_set(
            session,
            tracked_name="map_annotate_score_set",
            score_set_ids=[score_set.id],
            statuses=[PipelineStatus.SUCCEEDED],
        )
        assert score_set.id not in succeeded


@pytest.mark.integration
class TestResolveCohort:
    def test_published_only_and_taxonomy_both_required(self, session, make_score_set, make_taxonomy):
        taxonomy = make_taxonomy(code=9606)
        matches_both = make_score_set(gene_names=["G1"], taxonomies=[taxonomy], published=True)
        make_score_set(gene_names=["G2"], taxonomies=[taxonomy], published=False)  # taxonomy only

        result = resolve_cohort(
            session,
            explicit_urns=None,
            collection_urn=None,
            published_only=True,
            taxonomy_id=9606,
            organism=None,
        )
        assert [ss.urn for ss in result] == [matches_both.urn]

    def test_explicit_urns_do_not_bypass_other_filters(self, session, make_score_set, capsys):
        unpublished = make_score_set(published=False)

        result = resolve_cohort(
            session,
            explicit_urns=[unpublished.urn],
            collection_urn=None,
            published_only=True,
            taxonomy_id=None,
            organism=None,
        )
        assert result == []

    def test_taxonomy_join_dedups_score_set_with_two_matching_genes(self, session, make_score_set, make_taxonomy):
        taxonomy = make_taxonomy(code=9606)
        score_set = make_score_set(gene_names=["G1", "G2"], taxonomies=[taxonomy, taxonomy])

        result = resolve_cohort(
            session,
            explicit_urns=None,
            collection_urn=None,
            published_only=False,
            taxonomy_id=9606,
            organism=None,
        )
        assert [ss.urn for ss in result].count(score_set.urn) == 1

    def test_accession_based_target_excluded_from_taxonomy_filter(self, session, make_score_set, make_taxonomy):
        make_taxonomy(code=9606)
        make_score_set(gene_names=[], taxonomies=[], accession_gene_names=["G1"])

        result = resolve_cohort(
            session,
            explicit_urns=None,
            collection_urn=None,
            published_only=False,
            taxonomy_id=9606,
            organism=None,
        )
        assert result == []

    def test_collection_urn_and_taxonomy_and_semantics(self, session, make_score_set, make_taxonomy, make_collection):
        taxonomy = make_taxonomy(code=9606)
        in_collection_and_taxon = make_score_set(gene_names=["G1"], taxonomies=[taxonomy])
        in_collection_only = make_score_set(gene_names=["G2"], taxonomies=[make_taxonomy(code=10090)])
        collection = make_collection(score_sets=[in_collection_and_taxon, in_collection_only])

        result = resolve_cohort(
            session,
            explicit_urns=None,
            collection_urn=collection.urn,
            published_only=False,
            taxonomy_id=9606,
            organism=None,
        )
        assert [ss.urn for ss in result] == [in_collection_and_taxon.urn]


####################################################################################################
# build_cohort_items / normalize_gene integration
####################################################################################################


@pytest.mark.integration
def test_build_cohort_items_normalizes_gene_names(session, make_score_set):
    score_set = make_score_set(gene_names=["  BRCA1 "])
    items = build_cohort_items([score_set])
    assert items == [(score_set, ["brca1"])]
