"""
Utilities for managing views via SQLAlchemy.
"""

from functools import partial

import sqlalchemy as sa
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement, MetaData
from sqlalchemy.orm import Session

from mavedb.db.base import Base

# See: https://github.com/sqlalchemy/sqlalchemy/wiki/Views, https://github.com/jeffwidman/sqlalchemy-postgresql-materialized-views?tab=readme-ov-file


class CreateView(DDLElement):
    def __init__(self, name: str, selectable: sa.Select, materialized: bool):
        self.name = name
        self.selectable = selectable
        self.materialized = materialized


class DropView(DDLElement):
    def __init__(self, name: str, materialized: bool):
        self.name = name
        self.materialized = materialized


class MaterializedView(Base):
    __abstract__ = True

    @classmethod
    def refresh(cls, connection, concurrently=True):
        """Refresh this materialized view."""
        refresh_mat_view(connection, cls.__table__.fullname, concurrently)


@compiler.compiles(CreateView)
def _create_view(element: CreateView, compiler, **kw):
    return "CREATE %s %s AS %s" % (
        "MATERIALIZED VIEW" if element.materialized else "VIEW",
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


@compiler.compiles(DropView)
def _drop_view(element: DropView, compiler, **kw):
    return "DROP %s %s" % ("MATERIALIZED VIEW" if element.materialized else "VIEW", element.name)


def view_exists(ddl: CreateView, target, connection: sa.Connection, materialized: bool, **kw):
    inspector = sa.inspect(connection)
    if inspector is None:
        return False

    view_names = inspector.get_materialized_view_names() if ddl.materialized else inspector.get_view_names()
    return ddl.name in view_names


def view_doesnt_exist(ddl: CreateView, target, connection: sa.Connection, materialized: bool, **kw):
    return not view_exists(ddl, target, connection, materialized, **kw)


def view(name: str, selectable: sa.Select, metadata: MetaData = Base.metadata, materialized: bool = False):
    """
    Register a view or materialized view to SQLAlchemy. Use this function to define a view on some arbitrary
    model class.

    ```
    class MyView(Base):
        __table__ = view(
            "my_view",
            select(
                MyModel.id.label("id"),
                MyModel.name.label("name"),
            ),
            materialized=False,
        )
    ```

    When registered in this manner, SQLAlchemy will create and destroy the view along with other tables. You can
    then query this view as if it were an ORM object.

    ```
    results = db.query(select(MyView.col1).where(MyView.col2)).all()
    ```
    """
    t = sa.table(
        name,
        *(sa.Column(c.name, c.type, primary_key=c.primary_key) for c in selectable.selected_columns),
    )
    t.primary_key.update(c for c in t.c if c.primary_key)  # type: ignore

    # TODO: Figure out indices.
    if materialized:
        sa.event.listen(
            metadata,
            "after_create",
            CreateView(name, selectable, True).execute_if(callable_=partial(view_doesnt_exist, materialized=True)),
        )
        sa.event.listen(
            metadata,
            "before_drop",
            DropView(name, True).execute_if(callable_=partial(view_exists, materialized=True)),
        )

    else:
        sa.event.listen(
            metadata,
            "after_create",
            CreateView(name, selectable, False).execute_if(callable_=partial(view_doesnt_exist, materialized=False)),
        )
        sa.event.listen(
            metadata,
            "before_drop",
            DropView(name, False).execute_if(callable_=partial(view_exists, materialized=False)),
        )

    return t


def refresh_mat_view(session: Session, name: str, concurrently=True):
    """
    Refreshes a single materialized view, given by `name`.
    """
    # since session.execute() bypasses autoflush, must manually flush in order
    # to include newly-created/modified objects in the refresh
    session.flush()
    _con = "CONCURRENTLY " if concurrently else ""
    session.execute(sa.text("REFRESH MATERIALIZED VIEW " + _con + name))


def refresh_all_mat_views(session: Session, concurrently=True):
    """
    Refreshes all materialized views. Views are refreshed in non-deterministic order,
    so view definitions can't depend on each other.
    """
    inspector = sa.inspect(session.connection())

    if not inspector:
        return

    for mv in inspector.get_materialized_view_names():
        refresh_mat_view(session, mv, concurrently)
