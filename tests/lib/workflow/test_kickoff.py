# ruff: noqa: E402
import pytest

pytest.importorskip("arq")

from unittest.mock import AsyncMock, Mock, patch

from arq.jobs import Job as ArqJob
from sqlalchemy import select

from mavedb.lib.workflow.kickoff import enqueue_pipeline_for_score_set
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.models.user import User
from tests.helpers.constants import TEST_USER


def _mock_entrypoint(*, id=42, job_function="start_pipeline", urn="urn:mavedb:job-fixed", retry_count=0):
    return Mock(id=id, job_function=job_function, urn=urn, retry_count=retry_count)


@pytest.mark.unit
@pytest.mark.asyncio
class TestEnqueuePipelineForScoreSetUnit:
    """Unit tests with PipelineFactory and ARQ mocked out."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.db = Mock()
        self.redis = AsyncMock()
        self.score_set = Mock(id=99, urn="urn:mavedb:00000001-a-1")
        self.user = Mock(id=5)
        self.pipeline = Mock(spec=Pipeline)
        self.entrypoint = _mock_entrypoint()

        factory_patch = patch("mavedb.lib.workflow.kickoff.PipelineFactory")
        self.factory_cls = factory_patch.start()
        self.factory = self.factory_cls.return_value
        self.factory.create_pipeline.return_value = (self.pipeline, self.entrypoint)
        yield
        factory_patch.stop()

    async def _call(self, **kwargs):
        return await enqueue_pipeline_for_score_set(
            self.db, self.redis, pipeline_name="test_pipeline", score_set=self.score_set, user=self.user, **kwargs
        )

    async def test_returns_pipeline_and_job_when_enqueued(self):
        job = Mock(spec=ArqJob)
        self.redis.enqueue_job.return_value = job

        result = await self._call()

        assert result == (self.pipeline, job)
        self.redis.enqueue_job.assert_awaited_once_with(
            self.entrypoint.job_function,
            self.entrypoint.id,
            _job_id=f"{self.entrypoint.urn}#{self.entrypoint.retry_count}",
        )

    async def test_returns_none_job_when_arq_deduplicates(self):
        self.redis.enqueue_job.return_value = None

        result = await self._call()

        assert result == (self.pipeline, None)

    async def test_propagates_key_error_for_unknown_pipeline_name(self):
        self.factory.create_pipeline.side_effect = KeyError("unknown_pipeline")

        with pytest.raises(KeyError):
            await self._call()

    async def test_discards_pipeline_and_reraises_when_enqueue_raises(self):
        self.redis.enqueue_job.side_effect = ConnectionError("redis is unreachable")

        with pytest.raises(ConnectionError):
            await self._call()

        self.factory.discard_pipeline.assert_called_once_with(self.pipeline)

    async def test_does_not_discard_pipeline_when_enqueue_succeeds(self):
        self.redis.enqueue_job.return_value = Mock(spec=ArqJob)

        await self._call()

        self.factory.discard_pipeline.assert_not_called()

    async def test_does_not_discard_pipeline_when_arq_deduplicates(self):
        self.redis.enqueue_job.return_value = None

        await self._call()

        self.factory.discard_pipeline.assert_not_called()

    async def test_default_correlation_id_built_from_pipeline_name_score_set_and_user(self):
        with patch("mavedb.lib.workflow.kickoff.correlation_id_for_context", return_value=None):
            await self._call()

        params = self.factory.create_pipeline.call_args.kwargs["pipeline_params"]
        assert params["correlation_id"].startswith(f"test_pipeline_{self.score_set.urn}_{self.user.id}_")

    async def test_correlation_id_from_context_used_when_present(self):
        with patch("mavedb.lib.workflow.kickoff.correlation_id_for_context", return_value="context-correlation-id"):
            await self._call()

        params = self.factory.create_pipeline.call_args.kwargs["pipeline_params"]
        assert params["correlation_id"] == "context-correlation-id"

    async def test_extra_params_correlation_id_overrides_context(self):
        with patch("mavedb.lib.workflow.kickoff.correlation_id_for_context", return_value="context-correlation-id"):
            await self._call(extra_params={"correlation_id": "explicit-correlation-id"})

        params = self.factory.create_pipeline.call_args.kwargs["pipeline_params"]
        assert params["correlation_id"] == "explicit-correlation-id"

    async def test_extra_params_merged_alongside_base_params(self):
        await self._call(extra_params={"some_extra_param": "some_value"})

        params = self.factory.create_pipeline.call_args.kwargs["pipeline_params"]
        assert params["some_extra_param"] == "some_value"
        assert params["score_set_id"] == self.score_set.id
        assert params["updater_id"] == self.user.id

    async def test_create_pipeline_called_with_pipeline_name_and_user(self):
        await self._call()

        call_kwargs = self.factory.create_pipeline.call_args.kwargs
        assert call_kwargs["pipeline_name"] == "test_pipeline"
        assert call_kwargs["creating_user"] is self.user


@pytest.mark.integration
@pytest.mark.asyncio
class TestEnqueuePipelineForScoreSetIntegration:
    """Integration tests exercising the real PipelineFactory and a fakeredis-backed ARQ instance."""

    @pytest.fixture(autouse=True)
    def setup(self, session, setup_lib_db_with_score_set):
        self.score_set = setup_lib_db_with_score_set
        self.user = session.query(User).filter(User.username == TEST_USER["username"]).first()

    async def test_creates_pipeline_and_job_run_records_and_enqueues_job(
        self, session, arq_redis, with_test_pipeline_definition_ctx, sample_independent_pipeline_definition
    ):
        pipeline, job = await enqueue_pipeline_for_score_set(
            session,
            arq_redis,
            pipeline_name=sample_independent_pipeline_definition["name"],
            score_set=self.score_set,
            user=self.user,
            extra_params={"required_param": "some_value"},
        )

        assert job is not None
        assert session.execute(select(Pipeline).where(Pipeline.id == pipeline.id)).scalar_one() is pipeline

        job_run = session.execute(
            select(JobRun).where(JobRun.pipeline_id == pipeline.id, JobRun.job_function == "process_data")
        ).scalar_one()
        assert job_run.job_params["required_param"] == "some_value"

    async def test_correlation_id_defaults_to_generated_value_without_request_context(
        self, session, arq_redis, with_test_pipeline_definition_ctx, sample_independent_pipeline_definition
    ):
        pipeline, _ = await enqueue_pipeline_for_score_set(
            session,
            arq_redis,
            pipeline_name=sample_independent_pipeline_definition["name"],
            score_set=self.score_set,
            user=self.user,
            extra_params={"required_param": "some_value"},
        )

        assert pipeline.correlation_id.startswith(
            f"{sample_independent_pipeline_definition['name']}_{self.score_set.urn}_{self.user.id}_"
        )

    async def test_independent_calls_create_independent_pipelines_and_jobs(
        self, session, arq_redis, with_test_pipeline_definition_ctx, sample_independent_pipeline_definition
    ):
        pipeline_one, job_one = await enqueue_pipeline_for_score_set(
            session,
            arq_redis,
            pipeline_name=sample_independent_pipeline_definition["name"],
            score_set=self.score_set,
            user=self.user,
            extra_params={"required_param": "some_value"},
        )
        pipeline_two, job_two = await enqueue_pipeline_for_score_set(
            session,
            arq_redis,
            pipeline_name=sample_independent_pipeline_definition["name"],
            score_set=self.score_set,
            user=self.user,
            extra_params={"required_param": "some_value"},
        )

        assert pipeline_one.id != pipeline_two.id
        assert job_one is not None
        assert job_two is not None

    async def test_returns_none_job_when_arq_job_id_collides(
        self, session, arq_redis, with_test_pipeline_definition_ctx, sample_independent_pipeline_definition
    ):
        with patch("mavedb.lib.workflow.kickoff.arq_job_id", return_value="fixed-dedupe-id"):
            pipeline_one, job_one = await enqueue_pipeline_for_score_set(
                session,
                arq_redis,
                pipeline_name=sample_independent_pipeline_definition["name"],
                score_set=self.score_set,
                user=self.user,
                extra_params={"required_param": "some_value"},
            )
            pipeline_two, job_two = await enqueue_pipeline_for_score_set(
                session,
                arq_redis,
                pipeline_name=sample_independent_pipeline_definition["name"],
                score_set=self.score_set,
                user=self.user,
                extra_params={"required_param": "some_value"},
            )

        # ARQ deduplicates the second enqueue on the collided job id, but pipeline records are
        # still created for both calls -- kickoff does not roll back on dedup.
        assert job_one is not None
        assert job_two is None
        assert pipeline_one.id != pipeline_two.id

    async def test_unknown_pipeline_name_raises_key_error(self, session, arq_redis):
        with pytest.raises(KeyError):
            await enqueue_pipeline_for_score_set(
                session,
                arq_redis,
                pipeline_name="does_not_exist_pipeline",
                score_set=self.score_set,
                user=self.user,
            )

    async def test_discards_pipeline_records_when_enqueue_raises(
        self, session, arq_redis, with_test_pipeline_definition_ctx, sample_independent_pipeline_definition
    ):
        with (
            patch.object(arq_redis, "enqueue_job", AsyncMock(side_effect=ConnectionError("redis is unreachable"))),
            pytest.raises(ConnectionError),
        ):
            await enqueue_pipeline_for_score_set(
                session,
                arq_redis,
                pipeline_name=sample_independent_pipeline_definition["name"],
                score_set=self.score_set,
                user=self.user,
                extra_params={"required_param": "some_value"},
            )

        # The pipeline (and its job runs) created before the failed enqueue must not be left behind.
        assert session.execute(select(Pipeline)).scalars().all() == []
        assert session.execute(select(JobRun)).scalars().all() == []
