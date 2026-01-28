import pytest
from sqlalchemy import select

from mavedb.lib.workflow.pipeline_factory import PipelineFactory
from mavedb.models.job_run import JobRun


@pytest.mark.unit
class TestPipelineFactoryUnit:
    """Unit tests for the PipelineFactory class."""

    def test_create_pipeline_raises_if_pipeline_not_found(self, session, test_user):
        """Test that creating a pipeline with an unknown name raises a KeyError."""
        pipeline_factory = PipelineFactory(session=session)

        with pytest.raises(KeyError) as exc_info:
            pipeline_factory.create_pipeline(
                pipeline_name="unknown_pipeline",
                creating_user=test_user,
                pipeline_params={},
            )

        assert "unknown_pipeline" in str(exc_info.value)

    def test_create_pipeline_prioritizes_correlation_id_from_params(
        self,
        session,
        with_test_pipeline_definition_ctx,
        pipeline_factory,
        sample_independent_pipeline_definition,
        test_user,
    ):
        """Test that the correlation_id from pipeline_params is used when creating a pipeline."""
        pipeline_name = sample_independent_pipeline_definition["name"]
        test_correlation_id = "test-correlation-id-123"

        pipeline, job_run = pipeline_factory.create_pipeline(
            pipeline_name=pipeline_name,
            creating_user=test_user,
            pipeline_params={"correlation_id": test_correlation_id, "required_param": "some_value"},
        )

        assert job_run.correlation_id == test_correlation_id

    def test_create_pipeline_creates_start_pipeline_job(
        self,
        session,
        with_test_pipeline_definition_ctx,
        pipeline_factory,
        sample_independent_pipeline_definition,
        test_user,
    ):
        """Test that creating a pipeline results in a JobRun of type 'start_pipeline'."""
        pipeline_name = sample_independent_pipeline_definition["name"]

        pipeline, job_run = pipeline_factory.create_pipeline(
            pipeline_name=pipeline_name,
            creating_user=test_user,
            pipeline_params={"required_param": "some_value"},
        )

        stmt = select(JobRun).where(JobRun.pipeline_id == pipeline.id)
        job_runs = session.execute(stmt).scalars().all()

        start_pipeline_jobs = [jr for jr in job_runs if jr.job_function == "start_pipeline"]
        assert len(start_pipeline_jobs) == 1
        assert start_pipeline_jobs[0].id == job_run.id

    def test_create_pipeline_creates_job_runs(
        self,
        session,
        with_test_pipeline_definition_ctx,
        pipeline_factory,
        sample_independent_pipeline_definition,
        test_user,
    ):
        """Test that creating a pipeline results in the correct number of JobRun instances."""
        pipeline_name = sample_independent_pipeline_definition["name"]
        expected_job_count = len(sample_independent_pipeline_definition["job_definitions"])

        pipeline, job_run = pipeline_factory.create_pipeline(
            pipeline_name=pipeline_name,
            creating_user=test_user,
            pipeline_params={"required_param": "some_value"},
        )

        stmt = select(JobRun).where(JobRun.pipeline_id == pipeline.id)
        job_runs = session.execute(stmt).scalars().all()

        # One additional job run for the start_pipeline job
        assert len(job_runs) == expected_job_count + 1

    def test_create_pipeline_creates_job_dependencies(
        self,
        session,
        with_test_pipeline_definition_ctx,
        pipeline_factory,
        sample_dependent_pipeline_definition,
        test_user,
    ):
        """Test that creating a pipeline with job dependencies results in correct JobDependency records."""
        pipeline_name = sample_dependent_pipeline_definition["name"]
        jobs = sample_dependent_pipeline_definition["job_definitions"]

        pipeline, job_run = pipeline_factory.create_pipeline(
            pipeline_name=pipeline_name,
            creating_user=test_user,
            pipeline_params={"paramA": "valueA", "paramB": "valueB", "required_param": "some_value"},
        )

        stmt = select(JobRun).where(JobRun.pipeline_id == pipeline.id)
        job_runs = session.execute(stmt).scalars().all()
        job_run_dict = {jr.job_function: jr for jr in job_runs}

        # Verify dependencies
        for job_def in jobs:
            job_deps = job_def["dependencies"]
            job_run = job_run_dict[job_def["function"]]

            # For each dependency, check that a JobDependency record exists
            # and verify its properties
            for dep_key, dependency_type in job_deps:
                dep_job_run = job_run_dict[[jd for jd in jobs if jd["key"] == dep_key][0]["function"]]

                assert len(job_run.job_dependencies) == 1
                for jd in job_run.job_dependencies:
                    assert jd.depends_on_job_id == dep_job_run.id
                    assert jd.dependency_type == dependency_type

    def test_create_pipeline_creates_pipeline(
        self,
        session,
        with_test_pipeline_definition_ctx,
        pipeline_factory,
        sample_independent_pipeline_definition,
        test_user,
    ):
        """Test that creating a pipeline results in a Pipeline record in the database."""
        pipeline_name = sample_independent_pipeline_definition["name"]

        pipeline, job_run = pipeline_factory.create_pipeline(
            pipeline_name=pipeline_name,
            creating_user=test_user,
            pipeline_params={"required_param": "some_value"},
        )

        stmt = select(pipeline.__class__).where(pipeline.__class__.id == pipeline.id)
        retrieved_pipeline = session.execute(stmt).scalars().first()

        assert retrieved_pipeline is not None
        assert retrieved_pipeline.id == pipeline.id


@pytest.mark.integration
class TestPipelineFactoryIntegration:
    """Integration tests for the PipelineFactory class."""

    def test_create_pipeline_independent(
        self,
        session,
        with_test_pipeline_definition_ctx,
        pipeline_factory,
        sample_independent_pipeline_definition,
        test_user,
    ):
        """Integration test for creating an independent pipeline."""
        pipeline_name = sample_independent_pipeline_definition["name"]

        pipeline, job_run = pipeline_factory.create_pipeline(
            pipeline_name=pipeline_name,
            creating_user=test_user,
            pipeline_params={"required_param": "some_value"},
        )

        assert pipeline.name == pipeline_name
        assert job_run.job_function == "start_pipeline"

        for job_def in sample_independent_pipeline_definition["job_definitions"]:
            stmt = select(JobRun).where(
                JobRun.pipeline_id == pipeline.id,
                JobRun.job_function == job_def["function"],
            )
            job_run = session.execute(stmt).scalars().first()
            assert job_run is not None
            assert job_run.job_params["param1"] == "value1"
            assert job_run.job_params["param2"] == "value2"
            assert job_run.pipeline_id == pipeline.id
            assert job_run.job_dependencies == []

    def test_create_pipeline_dependent(
        self,
        session,
        with_test_pipeline_definition_ctx,
        pipeline_factory,
        sample_dependent_pipeline_definition,
        test_user,
    ):
        """Integration test for creating a dependent pipeline."""
        pipeline_name = sample_dependent_pipeline_definition["name"]

        passed_params = {"paramA": "valueA", "paramB": "valueB", "required_param": "some_value"}
        pipeline, job_run = pipeline_factory.create_pipeline(
            pipeline_name=pipeline_name,
            creating_user=test_user,
            pipeline_params=passed_params,
        )

        assert pipeline.name == pipeline_name
        assert job_run.job_function == "start_pipeline"

        job_runs = {}
        for job_def in sample_dependent_pipeline_definition["job_definitions"]:
            stmt = select(JobRun).where(
                JobRun.pipeline_id == pipeline.id,
                JobRun.job_function == job_def["function"],
            )
            jr = session.execute(stmt).scalars().first()
            assert jr is not None
            assert jr.pipeline_id == pipeline.id
            for param_key, param_value in job_def["params"].items():
                if param_value is not None:
                    assert jr.job_params[param_key] == param_value
                else:
                    assert jr.job_params[param_key] == passed_params[param_key]

            job_runs[job_def["key"]] = jr

        # Verify dependencies
        for job_def in sample_dependent_pipeline_definition["job_definitions"]:
            job_deps = job_def["dependencies"]
            job_run = job_runs[job_def["key"]]
            for dep_key, dependency_type in job_deps:
                dep_job_run = job_runs[dep_key]

                assert len(job_run.job_dependencies) == 1
                for jd in job_run.job_dependencies:
                    assert jd.depends_on_job_id == dep_job_run.id
                    assert jd.dependency_type == dependency_type
