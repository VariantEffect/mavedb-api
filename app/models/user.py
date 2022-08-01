from sqlalchemy import Boolean, Column, DateTime, Integer, String

from app.db.base import Base


class User(Base):
    # __tablename__ = 'auth_user'
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String, index=True, nullable=False)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    email = Column(String, nullable=True)
    is_superuser = Column(Boolean, nullable=False, default=False)
    is_staff = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False)
    date_joined = Column(DateTime, nullable=True)
    email = Column(String, nullable=True)
    # password = Column(String, default='abcd')  # TODO Remove when the database is rebuilt from scratch.
