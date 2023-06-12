from celery import Celery
from celery.app.task import Task as CeleryTask
import io
import json
from kombu.exceptions import (
    OperationalError,
    ConnectionLimitExceeded,
    ConnectionError,
    LimitExceeded,
    KombuError,
    ContentDisallowed,
    SerializationError,
    DecodeError,
    EncodeError,
)
import logging
import pandas as pd
import traceback

# Create the celery src and get the logger
from mavedb.deps import get_db
from mavedb.models.failed_task import FailedTask

celery_app = Celery("tasks", broker="pyamqp://guest@rabbit//")
logger = logging.getLogger(__name__)


KOMBU_ERRORS = (
    ConnectionError,
    ConnectionLimitExceeded,
    ContentDisallowed,
    DecodeError,
    EncodeError,
    KombuError,
    LimitExceeded,
    OperationalError,
    SerializationError,
)


def dump_df(df, orient="records"):
    handle = io.StringIO()
    df.to_json(handle, orient=orient)
    handle.seek(0)
    return handle.read()


def record_failed_task(celery_task_id, name, exception, args, kwargs, trace, user_id=None):
    db = next(get_db())
    failed_task = FailedTask(
        celery_task_id=celery_task_id,
        name=name.split(".")[-1],
        full_name=name,
        exception_class=exception.__class__.__name__,
        exception_message=str(exception).strip(),
        kwargs=kwargs,
        trace=trace,
        user_id=user_id,
    )
    if args:
        args = [dump_df(i) if isinstance(i, pd.DataFrame) else i for i in args]
        failed_task.args = json.dumps(list(args))
    if kwargs:
        for key, item in kwargs.items():
            if isinstance(item, pd.DataFrame):
                kwargs[key] = dump_df(item)
        failed_task.kwargs = json.dumps(kwargs, sort_keys=True)
    if not find_existing_failed_task(db, celery_task_id):
        db.add(failed_task)
        db.commit()
        db.refresh(failed_task)
    return failed_task


def find_existing_failed_task(db, celery_task_id):
    if celery_task_id and celery_task_id != "-1":
        query = db.query(FailedTask).filter(FailedTask.celery_task_id == celery_task_id)
        return query.first()
    else:
        return None


class Task(CeleryTask):
    """
    Base task that will save the task to the database and log the error.
    """

    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def apply_async(
        self, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, shadow=None, **options
    ):
        logger.info(f"Applying async celery function '{self}'")
        return super().apply_async(
            args=args,
            kwargs=kwargs,
            task_id=task_id,
            producer=producer,
            link=link,
            link_error=link_error,
            shadow=shadow,
            **options,
        )

    def delay(self, *args, **kwargs):
        """
        Star argument version of :meth:`apply_async`.

        Does not support the extra options enabled by :meth:`apply_async`.

        Arguments:
            *args (Any): Positional arguments passed on to the task.
            **kwargs (Any): Keyword arguments passed on to the task.
        Returns:
            celery.result.AsyncResult: Future promise.
        """
        logger.info(f"Applying delayed celery function '{self}'")
        return self.apply_async(args, kwargs)

    @staticmethod
    def get_user(user):
        # if isinstance(user, User):
        #    pass
        # elif isinstance(user, int):
        #    if User.objects.filter(pk=user).count():
        #        user = User.objects.get(pk=user)
        #    else:
        #        user = None
        # elif isinstance(user, str):
        #    if User.objects.filter(username=user).count():
        #        user = User.objects.get(username=user)
        #    else:
        #        user = None
        # else:
        #    user = None
        #
        # return user
        return None

    def on_failure(self, exc, task_id, args, kwargs, einfo, user=None):
        """
        Error handler.

        This is run by the worker when the task fails.

        Parameters:
        ----------
        exc : `Exception`
            The exception raised by the task.
        task_id : `str`
            Unique id of the failed task.
        args : `Tuple`
            Original arguments for the task that failed.
        kwargs : `Dict`
            Original keyword arguments for the task that failed.
        einfo : `~billiard.einfo.ExceptionInfo`
            Exception information.
        user : `User`, `int` or `str`.
            User that called the task. Applicable to `publish_score_set`
            and `create_variants` tasks. Will search User model if user
            is an int pk or str username.

        Returns
        -------
        `None`
            The return value of this handler is ignored.
        """
        user = self.get_user(user)
        # The kwargs can be potentially big in dataset tasks so truncate
        # the variants key before logging.
        kwargs_str = kwargs.copy()

        # TODO Revisit this.
        variants = str(kwargs_str.get("variants", {}))[0:250]
        if variants in kwargs_str:
            kwargs_str["variants"] = variants

        logger.exception(
            f"{self.name} with id {task_id} called with args={2}, kwargs={kwargs_str}"
            f" raised:\n'{exc}' with traceback:\n{einfo}"
        )
        self.save_failed_task(exc, task_id, args, kwargs, einfo, user)
        super(BaseTask, self).on_failure(exc, task_id, args, kwargs, einfo)

    def save_failed_task(self, exc, task_id, args, kwargs, trace, user=None):
        """
        Save a failed task. If it exists, update the modification_date and failure counter.
        """
        task, _ = FailedTask.update_or_create(
            name=self.name.split(".")[-1],
            full_name=self.name,
            exc=exc,
            task_id=task_id,
            args=args,
            kwargs=kwargs,
            traceback=str(trace).strip(),  # einfo
            user=user,
        )
        return task

    def submit_task(self, args=None, kwargs=None, async_options=None, request=None, countdown=10):
        """
        Calls `task.apply_async` and handles any connection errors by
        logging the error to the `django` default log and saving the
        failed task. If a request object is passed in a warning message will be
        shown to the user using the `messages` contrib module and the task
        will be initialised with the authenticated user as a foreign key.

        Parameters
        ----------
        args : tuple, optional, Default: None
            Un-named task arguments.
        kwargs : dict, optional. Default: None
            Task keyword arguments.
        async_options : dict, optional, Default: None
            Additional kwargs that `apply_async` accepts.
        request : Request, optional. Default: None
            Request object from the view calling this function.
        countdown : int
            Delay before executing celery task.

        Returns
        -------
        tuple[bool, Union[FailedTask, Any]]
            Boolean indicating success or failure, FailedTask or task result.
        """
        if not async_options:
            async_options = {}
        try:
            return (True, self.apply_async(args=args, kwargs=kwargs, countdown=countdown, **async_options))
        except KOMBU_ERRORS as ex:
            logger.exception(
                f"Submitting task {self.name} raised a {ex.__class__.__name__} error. " "Failed task has been saved."
            )
            # TODO If this is triggered by an API request, respond with a warning. Here is how this
            # used to be done in Django:
            # if request:
            #     messages.warning(request, network_message)
            failed_task = record_failed_task(
                celery_task_id="-1",
                name=self.name,
                exception=ex,
                args=args,
                kwargs=kwargs,
                trace=traceback.format_exc(),
                user_id=None,  # None if not request else request.user TODO
            )
            return False, failed_task
