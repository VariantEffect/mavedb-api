from datetime import date
from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, backref, Mapped
from sqlalchemy.ext.associationproxy import association_proxy, AssociationProxy
from sqlalchemy.schema import Table
from sqlalchemy.dialects.postgresql import JSONB

from typing import List, TYPE_CHECKING, Optional

from mavedb.db.base import Base
from mavedb.models.enums.logs import LogType, Method, Source

class LogRecord(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True)

    log_type = Column(
        Enum(LogType, create_constraint=True, length=16, native_enum=False, validate_strings=True),
        nullable=True,
    )
    source = Column(
        Enum(Source, create_constraint=True, length=16, native_enum=False, validate_strings=True),
        nullable=True,
    )
    user_logged_in = Column(Boolean, nullable=True)
    time_ns = Column(Integer, nullable=True)
    duration_ns = Column(Integer, nullable=True)

    # Fields specific to API calls
    path = Column(String, nullable=False)
    method = Column(
        Enum(Method, create_constraint=True, length=8, native_enum=False, validate_strings=True),
        nullable=True,
    )
    response_code = Column(Integer, nullable=True)

