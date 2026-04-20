# ruff: noqa: E402

from datetime import datetime, timezone

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from tests.helpers.dependency_overrider import DependencyOverrider


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


def _make_job_run(session, pipeline_id=None, **overrides) -> JobRun:
    defaults = {
        "job_type": "variant_mapping",
        "job_function": "map_variants_for_score_set",
        "status": JobStatus.PENDING,
        "pipeline_id": pipeline_id,
        "correlation_id": "corr-1",
        "max_retries": 3,
        "retry_count": 0,
    }
    defaults.update(overrides)
    job_run = JobRun(**defaults)
    session.add(job_run)
    session.commit()
    session.refresh(job_run)
    return job_run


####################################################################################################
# /api/v1/pipelines
####################################################################################################


def test_cannot_list_pipelines_as_anonymous_user(client, setup_router_db, anonymous_app_overrides):
    with DependencyOverrider(anonymous_app_overrides):
        response = client.get("/api/v1/pipelines/")

    assert response.status_code == 401


def test_cannot_list_pipelines_as_normal_user(client, setup_router_db):
    response = client.get("/api/v1/pipelines/")
    assert response.status_code == 403


def test_can_list_pipelines_as_admin(client, session, setup_router_db, admin_app_overrides):
    _make_pipeline(session, name="p1", status=PipelineStatus.RUNNING)
    _make_pipeline(session, name="p2", status=PipelineStatus.SUCCEEDED)

    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/pipelines/")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    names = {row["name"] for row in body}
    assert names == {"p1", "p2"}


def test_list_pipelines_filters_by_status(client, session, setup_router_db, admin_app_overrides):
    _make_pipeline(session, name="p_running", status=PipelineStatus.RUNNING)
    _make_pipeline(session, name="p_done", status=PipelineStatus.SUCCEEDED)

    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/pipelines/?status=running")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["name"] == "p_running"


def test_list_pipelines_filters_by_correlation_id(client, session, setup_router_db, admin_app_overrides):
    _make_pipeline(session, name="p_a", correlation_id="corr-a")
    _make_pipeline(session, name="p_b", correlation_id="corr-b")

    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/pipelines/?correlation_id=corr-a")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["name"] == "p_a"


def test_list_pipelines_respects_limit(client, session, setup_router_db, admin_app_overrides):
    for i in range(5):
        _make_pipeline(session, name=f"p{i}")

    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/pipelines/?limit=3")

    assert response.status_code == 200
    assert len(response.json()) == 3


def test_cannot_show_pipeline_as_normal_user(client, session, setup_router_db):
    pipeline = _make_pipeline(session)
    response = client.get(f"/api/v1/pipelines/{pipeline.urn}")
    assert response.status_code == 403


def test_show_pipeline_returns_404_for_unknown_urn(client, setup_router_db, admin_app_overrides):
    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/pipelines/urn:mavedb-pipeline:does-not-exist")

    assert response.status_code == 404


def test_show_pipeline_returns_progress(client, session, setup_router_db, admin_app_overrides):
    pipeline = _make_pipeline(session)
    _make_job_run(session, pipeline_id=pipeline.id, status=JobStatus.SUCCEEDED)
    _make_job_run(session, pipeline_id=pipeline.id, status=JobStatus.FAILED)
    _make_job_run(session, pipeline_id=pipeline.id, status=JobStatus.PENDING)

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/pipelines/{pipeline.urn}")

    assert response.status_code == 200
    body = response.json()
    assert body["urn"] == pipeline.urn
    assert body["name"] == pipeline.name
    # Progress aggregation is delegated to PipelineManager.get_pipeline_progress().
    progress = body["progress"]
    assert progress["totalJobs"] == 3
    assert progress["successfulJobs"] == 1
    assert progress["failedJobs"] == 1
    assert progress["pendingJobs"] == 1
    # completion = succeeded + failed + skipped + cancelled = 2 / 3
    assert progress["completedJobs"] == 2
    assert 66.0 < progress["completionPercentage"] < 67.0


def test_show_pipeline_renders_metadata_key(client, session, setup_router_db, admin_app_overrides):
    pipeline = _make_pipeline(session, metadata_={"foo": "bar"})

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/pipelines/{pipeline.urn}")

    assert response.status_code == 200
    body = response.json()
    # `metadata_` on the ORM model surfaces as JSON key `metadata`.
    assert body["metadata"] == {"foo": "bar"}


def test_show_pipeline_with_no_jobs_reports_empty_progress(client, session, setup_router_db, admin_app_overrides):
    pipeline = _make_pipeline(session)

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/pipelines/{pipeline.urn}")

    assert response.status_code == 200
    progress = response.json()["progress"]
    assert progress["totalJobs"] == 0
    assert progress["completionPercentage"] == 100.0


def test_list_pipelines_orders_by_created_desc(client, session, setup_router_db, admin_app_overrides):
    older = _make_pipeline(session, name="older")
    # Force created_at ordering deterministically.
    older.created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    session.commit()
    newer = _make_pipeline(session, name="newer")
    newer.created_at = datetime(2025, 1, 1, tzinfo=timezone.utc)
    session.commit()

    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/pipelines/")

    assert response.status_code == 200
    names = [row["name"] for row in response.json()]
    assert names == ["newer", "older"]
