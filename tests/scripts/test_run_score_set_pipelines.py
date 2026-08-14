# ruff: noqa: E402

from datetime import date, datetime, timezone

import pytest

pytest.importorskip("arq")

from mavedb.lib.workflow.definitions import PIPELINE_DEFINITIONS
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.scripts.run_score_set_pipelines import (
    CLUSTER_KEY_UNKNOWN,
    _IN_FLIGHT_STATUSES,
    cluster_cohort,
    cohort_filename,
    effective_pipeline_name,
    extract_symbol,
    filter_by_gene,
    group_clusters,
    in_flight_pipelines,
    is_current,
    is_failure,
    pipelines_by_score_set,
    plan_enqueue,
    render_cluster_table,
    resolve_cohort,
    resolve_job_subset,
    score_set_symbols,
    write_cohort_files,
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
class TestExtractSymbol:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("BRCA1", "brca1"),
            ("  BRCA1 ", "brca1"),
            ("TP53 (P72R)", "tp53"),
            ("BRCA1 RING domain", "brca1"),
            ("TERT promoter", "tert"),
            ("MSH2 exon 7", "msh2"),
            ("alpha-synuclein", "alpha-synuclein"),
        ],
    )
    def test_first_word_is_the_symbol(self, name, expected):
        assert extract_symbol(name) == expected

    def test_empty_name(self):
        assert extract_symbol("") == ""
        assert extract_symbol("   ") == ""


@pytest.mark.unit
class TestClusterCohort:
    class _FakeTargetGene:
        def __init__(self, name):
            self.name = name
            self.mapped_hgnc_name = None

    class _FakeScoreSet:
        def __init__(self, urn, gene_names=()):
            self.urn = urn
            self.target_genes = [TestClusterCohort._FakeTargetGene(name) for name in gene_names]

    def _entry_map(self, entries):
        return {entry.score_set.urn: entry.cluster_key for entry in entries}

    def test_groups_same_symbol_adjacently_and_sorts_by_urn(self):
        entries = cluster_cohort(
            [
                self._FakeScoreSet("urn:3", ["TP53"]),
                self._FakeScoreSet("urn:2", ["BRCA1"]),
                self._FakeScoreSet("urn:1", ["BRCA1 RING domain"]),
            ]
        )
        assert [entry.score_set.urn for entry in entries] == ["urn:1", "urn:2", "urn:3"]

    def test_multi_target_score_set_files_under_its_first_symbol(self):
        entries = cluster_cohort([self._FakeScoreSet("urn:1", ["BRCA1", "BARD1"])])
        assert entries[0].cluster_key == "bard1"

    def test_unrelated_genes_stay_separate(self):
        entries = cluster_cohort(
            [
                self._FakeScoreSet("urn:1", ["BRCA1"]),
                self._FakeScoreSet("urn:2", ["BRCA2"]),
            ]
        )
        by_urn = self._entry_map(entries)
        assert by_urn["urn:1"] != by_urn["urn:2"]

    def test_no_symbol_gets_unknown_key_and_sorts_last(self):
        """Unknown-gene entries are fill-in work, so plan_enqueue must reach them last."""
        entries = cluster_cohort(
            [
                self._FakeScoreSet("urn:1", ["   "]),
                self._FakeScoreSet("urn:2", ["ZZZ"]),
            ]
        )
        assert entries[-1].score_set.urn == "urn:1"
        assert entries[-1].cluster_key == CLUSTER_KEY_UNKNOWN


@pytest.mark.unit
class TestFilterByGene:
    class _FakeScoreSet:
        def __init__(self, urn, gene_names):
            self.urn = urn
            self.target_genes = [TestClusterCohort._FakeTargetGene(name) for name in gene_names]

    def _cohort(self):
        return cluster_cohort(
            [
                self._FakeScoreSet("urn:1", ["BRCA1"]),
                self._FakeScoreSet("urn:2", ["BRCA1 RING domain"]),
                self._FakeScoreSet("urn:3", ["TP53"]),
                self._FakeScoreSet("urn:4", ["BARD1", "BRCA1"]),
            ]
        )

    def _urns(self, entries):
        return {entry.score_set.urn for entry in entries}

    def test_matches_decorated_names_too(self):
        assert self._urns(filter_by_gene(self._cohort(), ["BRCA1"])) == {"urn:1", "urn:2", "urn:4"}

    def test_input_is_case_insensitive(self):
        cohort = self._cohort()
        assert self._urns(filter_by_gene(cohort, ["brca1"])) == self._urns(filter_by_gene(cohort, ["BRCA1"]))

    def test_matches_a_secondary_symbol_not_just_the_cluster_key(self):
        """urn:4 clusters under bard1, but --gene BRCA1 should still find it."""
        cohort = self._cohort()
        by_urn = {entry.score_set.urn: entry.cluster_key for entry in cohort}
        assert by_urn["urn:4"] == "bard1"
        assert "urn:4" in self._urns(filter_by_gene(cohort, ["BRCA1"]))

    def test_multiple_genes_union(self):
        assert self._urns(filter_by_gene(self._cohort(), ["TP53", "BARD1"])) == {"urn:3", "urn:4"}

    def test_unknown_gene_yields_empty(self):
        assert filter_by_gene(self._cohort(), ["NOTAGENE"]) == []

    def test_blank_gene_matches_nothing(self):
        assert filter_by_gene(self._cohort(), ["   "]) == []


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
class TestInFlightStatuses:
    def test_every_pipeline_status_is_deliberately_classified(self):
        """An unlisted status silently reads as "not in flight", so it would stop counting
        against --concurrency and let a campaign over-enqueue. Adding a PipelineStatus has
        to be a decision here, not an omission."""
        terminal = {
            PipelineStatus.SUCCEEDED,
            PipelineStatus.FAILED,
            PipelineStatus.PARTIAL,
            PipelineStatus.CANCELLED,
        }
        assert _IN_FLIGHT_STATUSES | terminal == set(PipelineStatus)
        assert not _IN_FLIGHT_STATUSES & terminal


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
        def __init__(self, id_, urn, gene_name):
            self.id = id_
            self.urn = urn
            self.target_genes = [TestClusterCohort._FakeTargetGene(gene_name)]

    def _cohort(self, n, genes_by_id=None):
        genes_by_id = genes_by_id or {}
        return cluster_cohort([self._FakeScoreSet(i, f"urn:{i}", genes_by_id.get(i, "   ")) for i in range(1, n + 1)])

    def _decisions(self, plan):
        return {ss.id: decision for ss, _key, decision in plan}

    def test_zero_slots_skips_everything_as_cap(self):
        plan = plan_enqueue(
            self._cohort(2), in_flight_score_set_ids=set(), current_score_set_ids=set(), slots=0, limit=None
        )
        assert [decision for _ss, _key, decision in plan] == ["skip_cap", "skip_cap"]

    def test_in_flight_skips_without_consuming_slot(self):
        plan = plan_enqueue(
            self._cohort(2), in_flight_score_set_ids={1}, current_score_set_ids=set(), slots=1, limit=None
        )
        decisions = self._decisions(plan)
        assert decisions[1] == "skip_in_flight"
        assert decisions[2] == "enqueue"

    def test_current_skips_without_consuming_slot(self):
        plan = plan_enqueue(
            self._cohort(2), in_flight_score_set_ids=set(), current_score_set_ids={1}, slots=1, limit=None
        )
        decisions = self._decisions(plan)
        assert decisions[1] == "skip_current"
        assert decisions[2] == "enqueue"

    def test_limit_caps_below_slots(self):
        plan = plan_enqueue(
            self._cohort(3), in_flight_score_set_ids=set(), current_score_set_ids=set(), slots=3, limit=1
        )
        assert sorted(self._decisions(plan).values()) == ["enqueue", "skip_cap", "skip_cap"]

    def test_slots_fill_one_cluster_before_moving_to_the_next(self):
        """Two slots against two clusters must both land on the same gene, otherwise each
        running pipeline warms a ClinGen cache the other never reads."""
        cohort = self._cohort(4, {1: "BRCA1", 2: "BRCA1", 3: "TP53", 4: "TP53"})
        plan = plan_enqueue(cohort, in_flight_score_set_ids=set(), current_score_set_ids=set(), slots=2, limit=None)
        enqueued = {key for _ss, key, decision in plan if decision == "enqueue"}
        assert len(enqueued) == 1

    def test_unknown_cluster_yields_to_real_clusters(self):
        cohort = self._cohort(3, {3: "TP53"})
        plan = plan_enqueue(cohort, in_flight_score_set_ids=set(), current_score_set_ids=set(), slots=1, limit=None)
        assert self._decisions(plan)[3] == "enqueue"

    def test_unknown_cluster_still_fills_leftover_slots(self):
        cohort = self._cohort(3, {3: "TP53"})
        plan = plan_enqueue(cohort, in_flight_score_set_ids=set(), current_score_set_ids=set(), slots=3, limit=None)
        assert set(self._decisions(plan).values()) == {"enqueue"}


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
    def test_published_only_filters_unpublished(self, session, make_score_set):
        published = make_score_set(gene_names=["G1"], published=True)
        make_score_set(gene_names=["G2"], published=False)

        result = resolve_cohort(session, explicit_urns=None, collection_urn=None, published_only=True)
        assert [ss.urn for ss in result] == [published.urn]

    def test_explicit_urns_do_not_bypass_other_filters(self, session, make_score_set):
        unpublished = make_score_set(published=False)

        result = resolve_cohort(session, explicit_urns=[unpublished.urn], collection_urn=None, published_only=True)
        assert result == []

    def test_score_set_with_two_genes_returned_once(self, session, make_score_set):
        score_set = make_score_set(gene_names=["G1", "G2"], published=True)

        result = resolve_cohort(session, explicit_urns=None, collection_urn=None, published_only=True)
        assert [ss.urn for ss in result].count(score_set.urn) == 1

    def test_accession_based_target_is_included(self, session, make_score_set):
        """Regression: the taxonomy filter joined through TargetSequence, so every
        accession-based score set was silently dropped from the cohort."""
        accession_only = make_score_set(gene_names=[], accession_gene_names=["G1"], published=True)

        result = resolve_cohort(session, explicit_urns=None, collection_urn=None, published_only=True)
        assert [ss.urn for ss in result] == [accession_only.urn]

    def test_collection_urn_and_published_only_and_semantics(self, session, make_score_set, make_collection):
        in_collection_published = make_score_set(gene_names=["G1"], published=True)
        in_collection_unpublished = make_score_set(gene_names=["G2"], published=False)
        collection = make_collection(score_sets=[in_collection_published, in_collection_unpublished])

        result = resolve_cohort(session, explicit_urns=None, collection_urn=collection.urn, published_only=True)
        assert [ss.urn for ss in result] == [in_collection_published.urn]


####################################################################################################
# Symbol extraction / clustering against real score sets
####################################################################################################


@pytest.mark.integration
class TestScoreSetSymbols:
    def test_first_word_of_target_name(self, session, make_score_set):
        score_set = make_score_set(gene_names=["  BRCA1 RING domain "])
        assert score_set_symbols(score_set) == {"brca1"}

    def test_mapped_hgnc_name_wins_over_target_name(self, session, make_score_set):
        score_set = make_score_set(gene_names=["Ras"], mapped_hgnc_names=["HRAS"])
        assert score_set_symbols(score_set) == {"hras"}

    def test_vague_names_do_not_bridge_distinct_hgnc_symbols(self, session, make_score_set):
        """Both are curated as "Ras"; using the name alongside the HGNC symbol would put
        HRAS and KRAS in one cluster that shares no alleles."""
        hras = make_score_set(gene_names=["Ras"], mapped_hgnc_names=["HRAS"])
        kras = make_score_set(gene_names=["Ras"], mapped_hgnc_names=["KRAS"])

        entries = cluster_cohort([hras, kras])
        keys = {entry.score_set.urn: entry.cluster_key for entry in entries}
        assert keys[hras.urn] != keys[kras.urn]

    def test_accession_target_contributes_its_name(self, session, make_score_set):
        score_set = make_score_set(gene_names=[], accession_gene_names=["TP53 (P72R)"])
        assert score_set_symbols(score_set) == {"tp53"}

    def test_decorated_and_bare_names_cluster_together(self, session, make_score_set):
        bare = make_score_set(gene_names=["BRCA1"])
        decorated = make_score_set(gene_names=["BRCA1 exon 11"])

        entries = cluster_cohort([bare, decorated])
        assert len({entry.cluster_key for entry in entries}) == 1

    def test_blank_target_name_yields_no_symbol(self, session, make_score_set):
        score_set = make_score_set(gene_names=["   "])
        assert score_set_symbols(score_set) == frozenset()


####################################################################################################
# Cohort builder
####################################################################################################


@pytest.mark.unit
class TestCohortFilename:
    def test_unknown_cluster_gets_a_stable_name(self):
        assert cohort_filename("") == "_unknown.urns"

    def test_symbol_becomes_filename(self):
        assert cohort_filename("brca1") == "brca1.urns"

    def test_unsafe_characters_flattened(self):
        assert cohort_filename("kir2.1") == "kir2.1.urns"
        assert cohort_filename("5'foo") == "5_foo.urns"


@pytest.mark.integration
class TestCohortBuilder:
    def test_groups_urns_per_cluster(self, session, make_score_set):
        brca1_a = make_score_set(gene_names=["BRCA1"])
        brca1_b = make_score_set(gene_names=["BRCA1 RING domain"])
        tp53 = make_score_set(gene_names=["TP53"])

        clusters = dict(group_clusters(cluster_cohort([brca1_a, brca1_b, tp53])))

        assert {entry.score_set.urn for entry in clusters["brca1"]} == {brca1_a.urn, brca1_b.urn}
        assert [entry.score_set.urn for entry in clusters["tp53"]] == [tp53.urn]

    def test_largest_cluster_ranks_first(self, session, make_score_set):
        make_score_set(gene_names=["TP53"])
        make_score_set(gene_names=["BRCA1"])
        make_score_set(gene_names=["BRCA1 exon 11"])

        cohort = cluster_cohort(resolve_cohort(session, explicit_urns=None, collection_urn=None, published_only=False))
        assert group_clusters(cohort)[0][0] == "brca1"

    def test_unknown_cluster_ranks_last(self, session, make_score_set):
        unknown = make_score_set(gene_names=["   "])
        known = make_score_set(gene_names=["BRCA1"])

        clusters = group_clusters(cluster_cohort([unknown, known]))
        assert clusters[-1][0] == CLUSTER_KEY_UNKNOWN

    def test_table_reports_set_and_variant_counts_per_cluster(self, session, make_score_set):
        make_score_set(gene_names=["BRCA1"], num_variants=10)
        make_score_set(gene_names=["BRCA1 exon 11"], num_variants=5)

        cohort = cluster_cohort(resolve_cohort(session, explicit_urns=None, collection_urn=None, published_only=False))
        table = render_cluster_table(group_clusters(cohort))

        assert "1 gene cluster(s)" in table
        # One cluster row: 2 score sets on brca1, 15 variants between them.
        assert table.splitlines()[-1].split() == ["brca1", "2", "15", "brca1"]

    def test_writes_one_urn_file_per_cluster(self, session, make_score_set, tmp_path):
        brca1 = make_score_set(gene_names=["BRCA1"])
        tp53 = make_score_set(gene_names=["TP53"])

        clusters = group_clusters(cluster_cohort([brca1, tp53]))
        written, stale = write_cohort_files(clusters, str(tmp_path))

        assert {path.rsplit("/", 1)[-1] for path in written} == {"brca1.urns", "tp53.urns"}
        assert (tmp_path / "brca1.urns").read_text() == f"{brca1.urn}\n"
        assert stale == []

    def test_reports_urns_files_left_by_an_earlier_plan(self, session, make_score_set, tmp_path):
        """A shrunk cohort leaves files behind, and --urns-file would consume one."""
        (tmp_path / "gone.urns").write_text("urn:mavedb:00000001-a-1\n")

        clusters = group_clusters(cluster_cohort([make_score_set(gene_names=["BRCA1"])]))
        _written, stale = write_cohort_files(clusters, str(tmp_path))

        assert [path.rsplit("/", 1)[-1] for path in stale] == ["gone.urns"]

    def test_colliding_cluster_filenames_raise_rather_than_clobber(self, session, make_score_set, tmp_path):
        clusters = group_clusters(
            cluster_cohort([make_score_set(gene_names=["5'foo"]), make_score_set(gene_names=['5"foo'])])
        )

        assert len(clusters) == 2, "distinct cluster keys that sanitize to one filename"
        with pytest.raises(ValueError, match="share the cohort filename"):
            write_cohort_files(clusters, str(tmp_path))
