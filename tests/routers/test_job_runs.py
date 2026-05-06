# ruff: noqa: E402

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
# /api/v1/job-runs
####################################################################################################


def test_cannot_list_job_runs_as_anonymous_user(client, setup_router_db, anonymous_app_overrides):
    with DependencyOverrider(anonymous_app_overrides):
        response = client.get("/api/v1/job-runs/")

    assert response.status_code == 401


def test_cannot_list_job_runs_as_normal_user(client, setup_router_db):
    response = client.get("/api/v1/job-runs/")
    assert response.status_code == 403


def test_can_list_job_runs_as_admin(client, session, setup_router_db, admin_app_overrides):
    _make_job_run(session, status=JobStatus.PENDING)
    _make_job_run(session, status=JobStatus.SUCCEEDED)

    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/job-runs/")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2


def test_list_job_runs_filters_by_status(client, session, setup_router_db, admin_app_overrides):
    _make_job_run(session, status=JobStatus.FAILED, error_message="boom")
    _make_job_run(session, status=JobStatus.SUCCEEDED)

    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/job-runs/?status=failed")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["status"] == "failed"
    assert body[0]["errorMessage"] == "boom"


def test_list_job_runs_filters_by_job_type(client, session, setup_router_db, admin_app_overrides):
    _make_job_run(session, job_type="variant_mapping", job_function="map_variants_for_score_set")
    _make_job_run(session, job_type="variant_creation", job_function="create_variants_for_score_set")

    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/job-runs/?job_type=variant_creation")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["jobType"] == "variant_creation"


def test_list_job_runs_filters_by_pipeline_id(client, session, setup_router_db, admin_app_overrides):
    pipeline = _make_pipeline(session)
    _make_job_run(session, pipeline_id=pipeline.id)
    _make_job_run(session, pipeline_id=None)

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/job-runs/?pipeline_id={pipeline.id}")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["pipelineId"] == pipeline.id


def test_list_job_runs_respects_limit(client, session, setup_router_db, admin_app_overrides):
    for _ in range(4):
        _make_job_run(session)

    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/job-runs/?limit=2")

    assert response.status_code == 200
    assert len(response.json()) == 2


def test_cannot_show_job_run_as_normal_user(client, session, setup_router_db):
    job_run = _make_job_run(session)
    response = client.get(f"/api/v1/job-runs/{job_run.urn}")
    assert response.status_code == 403


def test_show_job_run_returns_404_for_unknown_urn(client, setup_router_db, admin_app_overrides):
    with DependencyOverrider(admin_app_overrides):
        response = client.get("/api/v1/job-runs/urn:mavedb-job:does-not-exist")

    assert response.status_code == 404


def test_show_job_run_returns_detail_with_traceback(client, session, setup_router_db, admin_app_overrides):
    job_run = _make_job_run(
        session,
        status=JobStatus.FAILED,
        error_message="kaboom",
        error_traceback="Traceback (most recent call last):\n  File 'x.py'",
        failure_category="system_error",
    )

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/job-runs/{job_run.urn}")

    assert response.status_code == 200
    body = response.json()
    assert body["urn"] == job_run.urn
    assert body["status"] == "failed"
    assert body["errorMessage"] == "kaboom"
    # The detail response is the only place a full traceback is returned to operators.
    assert body["errorTraceback"].startswith("Traceback")
    assert body["failureCategory"] == "system_error"


def test_show_job_run_renders_metadata_key(client, session, setup_router_db, admin_app_overrides):
    job_run = _make_job_run(session, metadata_={"k": "v"})

    with DependencyOverrider(admin_app_overrides):
        response = client.get(f"/api/v1/job-runs/{job_run.urn}")

    assert response.status_code == 200
    body = response.json()
    # `metadata_` on the ORM model surfaces as JSON key `metadata`.
    assert body["metadata"] == {"k": "v"}
