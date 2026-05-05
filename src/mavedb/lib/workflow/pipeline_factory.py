import logging
from typing import Optional

from sqlalchemy import delete, or_, select
from sqlalchemy.orm import Session

from mavedb import __version__ as mavedb_version
from mavedb.lib.logging.context import correlation_id_for_context, logging_context
from mavedb.lib.workflow.definitions import PIPELINE_DEFINITIONS
from mavedb.lib.workflow.job_factory import JobFactory
from mavedb.models.enums.job_pipeline import JobType
from mavedb.models.job_dependency import JobDependency
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.models.user import User

logger = logging.getLogger(__name__)


class PipelineFactory:
    """
    PipelineFactory is responsible for creating Pipeline instances and their associated JobRun and JobDependency records in the database.

    Attributes:
        session (Session): The SQLAlchemy session used for database operations.

    Methods:
        __init__(session: Session):
            Initializes the PipelineFactory with a database session.

        create_pipeline(
            pipeline_name: str,
            pipeline_description: Optional[str],
            creating_user: User,
            pipeline_params: dict
        ) -> Pipeline:
            Creates a new Pipeline along with its JobRun and JobDependency records,
            commits them to the database, and returns the created Pipeline object.
    """

    def __init__(self, session: Session):
        self.session = session

    def create_pipeline(
        self, pipeline_name: str, creating_user: User, pipeline_params: dict
    ) -> tuple[Pipeline, JobRun]:
        """
        Creates a new Pipeline instance along with its associated JobRun and JobDependency records.

        Args:
            pipeline_name (str): The name of the pipeline to create.
            pipeline_description (Optional[str]): A description for the pipeline.
            creating_user (User): The user object representing the user creating the pipeline.
            pipeline_params (dict): Additional parameters for pipeline creation, such as correlation_id.

        Returns:
            Pipeline: The created Pipeline object.
            JobRun: The JobRun object representing the start of the pipeline.

        Raises:
            KeyError: If the specified pipeline_name is not found in PIPELINE_DEFINITIONS.
            Exception: If there is an error during database operations.

        Side Effects:
            - Adds and commits new Pipeline, JobRun, and JobDependency records to the database session.
        """
        pipeline_def = PIPELINE_DEFINITIONS[pipeline_name]
        jobs = pipeline_def["job_definitions"]
        job_runs: dict[str, JobRun] = {}

        correlation_id = pipeline_params.get("correlation_id", correlation_id_for_context())

        pipeline = Pipeline(
            name=pipeline_name,
            description=pipeline_def["description"],
            correlation_id=correlation_id,
            created_by_user_id=creating_user.id,
            mavedb_version=mavedb_version,
        )  # type: ignore[call-arg]
        self.session.add(pipeline)
        self.session.flush()  # To get pipeline.id

        logger.info(
            msg=f"Creating pipeline '{pipeline_name}' with ID {pipeline.id} and correlation ID {correlation_id}.",
            extra=logging_context(),
        )

        start_pipeline_job = JobRun(
            job_type=JobType.PIPELINE_MANAGEMENT,
            job_function="start_pipeline",
            job_params={},
            pipeline_id=pipeline.id,
            mavedb_version=mavedb_version,
            correlation_id=correlation_id,
        )  # type: ignore[call-arg]
        self.session.add(start_pipeline_job)
        self.session.flush()  # to get start_pipeline_job.id

        logger.debug(
            msg=f"Created start_pipeline JobRun with ID {start_pipeline_job.id} for pipeline ID {pipeline.id}.",
            extra=logging_context(),
        )

        job_factory = JobFactory(self.session)
        for job_def in jobs:
            job_run = job_factory.create_job_run(
                job_def=job_def,
                pipeline_id=pipeline.id,
                correlation_id=correlation_id,
                pipeline_params=pipeline_params,
            )
            job_runs[job_def["key"]] = job_run
            logger.debug(
                msg=f"Created JobRun with ID {job_run.id} for job '{job_def['key']}' in pipeline ID {pipeline.id}.",
                extra=logging_context(),
            )

        self.session.flush()  # to get job_run IDs

        for job_def in jobs:
            job_deps = job_def["dependencies"]

            job_run = job_runs[job_def["key"]]
            for dep_key, dependency_type in job_deps:
                dep_job_run = job_runs[dep_key]

                dep_job = JobDependency(
                    id=job_run.id,
                    depends_on_job_id=dep_job_run.id,
                    dependency_type=dependency_type,
                )  # type: ignore[call-arg]

                self.session.add(dep_job)
                logger.debug(
                    msg=f"Created JobDependency for job '{job_def['key']}' (ID {job_run.id}) depending on job '{dep_key}' (ID {dep_job_run.id}) with dependency type '{dependency_type.name}'.",
                    extra=logging_context(),
                )

        self.session.commit()
        logger.info(
            msg=f"Successfully created pipeline '{pipeline_name}' with ID {pipeline.id} and {len(job_runs)} JobRun records.",
            extra=logging_context(),
        )

        return pipeline, start_pipeline_job

    def discard_pipeline(self, pipeline: Optional[Pipeline]) -> None:
        """Delete a pipeline and all its associated JobRun and JobDependency records.

        Used to clean up committed pipeline records when the initial ARQ enqueue fails,
        so orphaned pipeline records don't accumulate in the database.
        """
        if pipeline is None:
            logger.warning(
                msg="No pipeline to discard, pipeline is None.",
                extra=logging_context(),
            )
            return

        try:
            pipeline_id = pipeline.id
            job_run_ids = self.session.scalars(select(JobRun.id).where(JobRun.pipeline_id == pipeline_id)).all()

            if job_run_ids:
                self.session.execute(
                    delete(JobDependency).where(
                        or_(JobDependency.id.in_(job_run_ids), JobDependency.depends_on_job_id.in_(job_run_ids))
                    )
                )
                self.session.execute(delete(JobRun).where(JobRun.id.in_(job_run_ids)))
                logger.debug(
                    msg=f"Deleted {len(job_run_ids)} JobRun records and their associated JobDependency records for pipeline ID {pipeline_id}.",
                    extra=logging_context(),
                )

            self.session.execute(delete(Pipeline).where(Pipeline.id == pipeline_id))
            self.session.commit()

            logger.info(
                msg=f"Successfully discarded pipeline with ID {pipeline_id} and all associated records.",
                extra=logging_context(),
            )

        except Exception:
            self.session.rollback()
            logger.error(
                msg="Failed to discard pipeline.",
                extra=logging_context(),
                exc_info=True,
            )
