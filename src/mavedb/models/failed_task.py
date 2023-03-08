from sqlalchemy import Column, Integer, String, SmallInteger, TIMESTAMP

from mavedb.db.base import Base


# TODO Unused
"""
class FailedTask(Base):
    # __tablename__ = 'core_failedtask'
    __tablename__ = 'failed_tasks'

    id = Column(Integer, autoincrement=True, primary_key=True)
    # creation_timestamp = Column('creation_date', TIMESTAMP(timezone=True), nullable=False)  # TODO Defaults
    # modification_timestamp = Column('modification_date', TIMESTAMP(timezone=True), nullable=True)  # TODO Defaults
    creation_timestamp = Column(TIMESTAMP(timezone=True), nullable=False)  # TODO Defaults
    modification_timestamp = Column(TIMESTAMP(timezone=True), nullable=True)  # TODO Defaults
    celery_task_id = Column(String(36), nullable=False)
    name = Column(String(125), nullable=False)
    full_name = Column(String, nullable=False)
    args = Column(String, nullable=True)
    kwargs = Column(String, nullable=True)
    exception_class = Column(String, nullable=False)
    # exception_message = Column('exception_msg', String, nullable=False)
    exception_message = Column(String, nullable=False)
    # trace = Column('traceback', String, nullable=True)
    trace = Column(String, nullable=True)
    num_failures = Column(SmallInteger, nullable=False)
    user_id = Column(Integer, nullable=True)  # TODO
"""
