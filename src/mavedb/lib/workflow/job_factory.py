from copy import deepcopy
from typing import Optional

from sqlalchemy.orm import Session

from mavedb import __version__ as mavedb_version
from mavedb.lib.types.workflow import JobDefinition
from mavedb.models.job_run import JobRun


class JobFactory:
    """
    JobFactory is responsible for creating and persisting JobRun instances based on
    provided job definitions and pipeline parameters.

    Attributes:
        session (Session): The SQLAlchemy session used for database operations.

    Methods:
        create_job_run(job_def: JobDefinition, pipeline_id: Optional[int], user_id: int, correlation_id: str, pipeline_params: dict) -> JobRun:"""

    def __init__(self, session: Session):
        self.session = session

    def create_job_run(
        self, job_def: JobDefinition, correlation_id: str, pipeline_params: dict, pipeline_id: Optional[int] = None
    ) -> JobRun:
        """
        Creates and persists a new JobRun instance based on the provided job definition and pipeline parameters.

        Args:
            job_def (JobDefinition): The job definition containing job type, function, and parameter template.
            pipeline_id (Optional[int]): The ID of the pipeline this job run is associated with.
            correlation_id (str): A unique identifier for correlating this job run with external systems or logs.
            pipeline_params (dict): A dictionary of parameters to fill in required job parameters and allow for extensibility.

        Returns:
            JobRun: The newly created JobRun instance (not yet committed to the database).

        Raises:
            ValueError: If any required parameter defined in the job definition is missing from pipeline_params.
        """
        job_params = deepcopy(job_def["params"])

        # Fill in required params from pipeline_params
        for key in job_params:
            if job_params[key] is None:
                if key not in pipeline_params:
                    raise ValueError(f"Missing required param: {key}")
                job_params[key] = pipeline_params[key]

        job_run = JobRun(
            job_type=job_def["type"],
            job_function=job_def["function"],
            job_params=job_params,
            pipeline_id=pipeline_id,
            mavedb_version=mavedb_version,
            correlation_id=correlation_id,
        )  # type: ignore[call-arg]

        self.session.add(job_run)
        return job_run
