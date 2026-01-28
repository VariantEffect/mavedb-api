from unittest.mock import patch

import pytest

from mavedb.models.pipeline import Pipeline


@pytest.mark.unit
class TestJobFactoryUnit:
    """Unit tests for the JobFactory class."""

    def test_create_job_run_persists_preset_params_from_definition(self, job_factory, sample_job_definition):
        existing_params = {"param1": "new_value1", "param2": "new_value2", "required_param": "required_value"}
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="test-correlation-id",
            pipeline_params=existing_params,
            pipeline_id=1,
        )

        assert job_run.job_params["param1"] == "value1"
        assert job_run.job_params["param2"] == "value2"

    def test_create_job_run_raises_error_for_missing_params(self, job_factory, sample_job_definition):
        incomplete_params = {"param1": "new_value1"}  # Missing param2

        with pytest.raises(ValueError) as exc_info:
            job_factory.create_job_run(
                job_def=sample_job_definition,
                correlation_id="test-correlation-id",
                pipeline_params=incomplete_params,
                pipeline_id=1,
            )

        assert "Missing required param: required_param" in str(exc_info.value)

    def test_create_job_run_fills_in_required_params(self, job_factory, sample_job_definition):
        pipeline_params = {"required_param": "required_value"}
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="test-correlation-id",
            pipeline_params=pipeline_params,
            pipeline_id=1,
        )

        assert job_run.job_params["param1"] == "value1"
        assert job_run.job_params["param2"] == "value2"
        assert job_run.job_params["required_param"] == "required_value"

    def test_create_job_run_persists_correlation_id(self, job_factory, sample_job_definition):
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="test-correlation-id",
            pipeline_params={"param1": "value1", "param2": "value2", "required_param": "required_value"},
            pipeline_id=1,
        )

        assert job_run.correlation_id == "test-correlation-id"

    def test_create_job_run_persists_mavedb_version(self, job_factory, sample_job_definition):
        with patch("mavedb.lib.workflow.job_factory.mavedb_version", "1.2.3"):
            job_run = job_factory.create_job_run(
                job_def=sample_job_definition,
                correlation_id="test-correlation-id",
                pipeline_params={"param1": "value1", "param2": "value2", "required_param": "required_value"},
                pipeline_id=1,
            )

        assert job_run.mavedb_version == "1.2.3"

    def test_create_job_run_persists_job_type_and_function(self, job_factory, sample_job_definition):
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="test-correlation-id",
            pipeline_params={"param1": "value1", "param2": "value2", "required_param": "required_value"},
            pipeline_id=1,
        )

        assert job_run.job_type == sample_job_definition["type"]
        assert job_run.job_function == sample_job_definition["function"]

    def test_create_job_run_ignores_extra_pipeline_params(self, job_factory, sample_job_definition):
        pipeline_params = {
            "param1": "new_value1",
            "param2": "new_value2",
            "required_param": "required_value",
            "extra_param": "should_be_ignored",
        }
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="test-correlation-id",
            pipeline_params=pipeline_params,
            pipeline_id=1,
        )

        assert "extra_param" not in job_run.job_params

    def test_create_job_run_with_no_pipeline_id(self, job_factory, sample_job_definition):
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="test-correlation-id",
            pipeline_params={"param1": "value1", "param2": "value2", "required_param": "required_value"},
        )

        assert job_run.pipeline_id is None

    def test_create_job_run_associates_with_pipeline(self, job_factory, sample_job_definition):
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="test-correlation-id",
            pipeline_params={"param1": "value1", "param2": "value2", "required_param": "required_value"},
            pipeline_id=42,
        )

        assert job_run.pipeline_id == 42

    def test_create_job_run_adds_to_session(self, job_factory, sample_job_definition):
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="test-correlation-id",
            pipeline_params={"param1": "value1", "param2": "value2", "required_param": "required_value"},
            pipeline_id=1,
        )

        assert job_run in job_factory.session.new


@pytest.mark.integration
class TestJobFactoryIntegration:
    """Integration tests for the JobFactory class within pipeline execution."""

    def test_create_job_run_independent(self, job_factory, sample_job_definition):
        pipeline_params = {"required_param": "required_value"}
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="integration-correlation-id",
            pipeline_params=pipeline_params,
        )
        job_factory.session.commit()

        retrieved_job_run = job_factory.session.get(type(job_run), job_run.id)

        assert retrieved_job_run is not None
        assert retrieved_job_run.job_type == sample_job_definition["type"]
        assert retrieved_job_run.job_function == sample_job_definition["function"]
        assert retrieved_job_run.job_params["param1"] == "value1"
        assert retrieved_job_run.job_params["param2"] == "value2"
        assert retrieved_job_run.job_params["required_param"] == "required_value"
        assert retrieved_job_run.correlation_id == "integration-correlation-id"
        assert retrieved_job_run.pipeline_id is None

    def test_create_job_run_with_pipeline(self, job_factory, sample_job_definition):
        pipeline = Pipeline(
            name="Test Pipeline",
            description="A pipeline for testing JobFactory integration.",
        )
        job_factory.session.add(pipeline)
        job_factory.session.flush()

        pipeline_params = {"required_param": "required_value"}
        job_run = job_factory.create_job_run(
            job_def=sample_job_definition,
            correlation_id="integration-correlation-id",
            pipeline_params=pipeline_params,
            pipeline_id=pipeline.id,
        )
        job_factory.session.commit()

        retrieved_job_run = job_factory.session.get(type(job_run), job_run.id)

        assert retrieved_job_run is not None
        assert retrieved_job_run.job_type == sample_job_definition["type"]
        assert retrieved_job_run.job_function == sample_job_definition["function"]
        assert retrieved_job_run.job_params["param1"] == "value1"
        assert retrieved_job_run.job_params["param2"] == "value2"
        assert retrieved_job_run.job_params["required_param"] == "required_value"
        assert retrieved_job_run.correlation_id == "integration-correlation-id"
        assert retrieved_job_run.pipeline_id == pipeline.id

    def test_create_job_run_missing_params_raises_error(self, job_factory, sample_job_definition):
        incomplete_params = {"param1": "new_value1"}  # Missing required_param

        with pytest.raises(ValueError) as exc_info:
            job_factory.create_job_run(
                job_def=sample_job_definition,
                correlation_id="integration-correlation-id",
                pipeline_params=incomplete_params,
                pipeline_id=100,
            )

        assert "Missing required param: required_param" in str(exc_info.value)
