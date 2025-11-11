"""
Utilities for managing views via SQLAlchemy.
"""

from functools import partial

import sqlalchemy as sa
from sqlalchemy.ext import compiler
from sqlalchemy.orm import Session
from sqlalchemy.schema import DDLElement, MetaData

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
        """
        Refresh the underlying materialized view for this ORM-mapped class.

        This class method delegates to `refresh_mat_view` to issue a database
        REFRESH MATERIALIZED VIEW (optionally CONCURRENTLY) statement for the
        materialized view backing the current model (`cls.__table__.fullname`).

        Parameters
        ---------
        connection : sqlalchemy.engine.Connection | sqlalchemy.orm.Session
            An active SQLAlchemy connection or session bound to the target database.
        concurrently : bool, default True
            If True, performs a concurrent refresh (REFRESH MATERIALIZED VIEW CONCURRENTLY),
            allowing reads during the refresh when the database backend supports it.
            If False, performs a blocking refresh.

        Returns
        -------
        None

        Raises
        ------
        sqlalchemy.exc.DBAPIError
            If the database reports an error while refreshing the materialized view.
        sqlalchemy.exc.OperationalError
            For operational issues such as locks or insufficient privileges.
        ValueError
            If the connection provided is not a valid SQLAlchemy connection/session.

        Notes
        -----
        - A concurrent refresh typically requires the materialized view to have a unique
          index matching all rows; otherwise the database may reject the operation.
        - This operation does not return a value; it is executed for its side effect.
        - Ensure the connection/session is in a clean transactional state if you rely on
          consistent snapshot semantics.
        - This function commits no changes; it is the caller's responsibility to
          commit the session if needed.

        Examples
        --------
        # Refresh with concurrent mode (default)
        MyMaterializedView.refresh(connection)

        # Perform a blocking refresh
        MyMaterializedView.refresh(connection, concurrently=False)
        """
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
    Refresh a PostgreSQL materialized view within the current SQLAlchemy session.

    This helper issues a REFRESH MATERIALIZED VIEW statement for the specified
    materialized view name. It first explicitly flushes the session because
    session.execute() bypasses SQLAlchemy's autoflush mechanism; without the flush,
    pending changes (e.g., newly inserted/updated rows that the view depends on)
    might not be reflected in the refreshed view.

    Parameters
    ----------
    session : sqlalchemy.orm.Session
        An active SQLAlchemy session bound to a PostgreSQL database.
    name : str
        The exact name (optionally schema-qualified) of the materialized view to refresh.
    concurrently : bool, default True
        If True, uses REFRESH MATERIALIZED VIEW CONCURRENTLY allowing reads during
        the refresh and requiring a unique index on the materialized view. If False,
        performs a blocking refresh.

    Raises
    ------
    sqlalchemy.exc.SQLAlchemyError
        Propagates any database errors encountered during execution (e.g.,
        insufficient privileges, missing view, lack of required unique index for
        CONCURRENTLY).

    Notes
    -----
    - Using CONCURRENTLY requires the materialized view to have at least one
      unique index; otherwise PostgreSQL will raise an error.
    - The operation does not return a value; it is executed for its side effect.
    - Ensure the session is in a clean transactional state if you rely on
      consistent snapshot semantics.
    - This function commits no changes; it is the caller's responsibility to
      commit the session if needed.

    Examples
    --------
    refresh_mat_view(session, "public.my_materialized_view")
    refresh_mat_view(session, "reports.daily_stats", concurrently=False)
    """
    # since session.execute() bypasses autoflush, must manually flush in order
    # to include newly-created/modified objects in the refresh
    session.flush()

    _con = "CONCURRENTLY " if concurrently else ""
    session.execute(sa.text("REFRESH MATERIALIZED VIEW " + _con + name))


def refresh_all_mat_views(session: Session, concurrently=True):
    """
    Refreshes all PostgreSQL materialized views visible to the given SQLAlchemy session.

    The function inspects the current database connection for registered materialized
    views and issues a REFRESH MATERIALIZED VIEW command for each one using the helper
    function `refresh_mat_view`. After all refresh operations complete, the session
    is committed to persist any transactional side effects of the refresh statements.

    Parameters
    ----------
    session : sqlalchemy.orm.Session
        An active SQLAlchemy session bound to a PostgreSQL connection.
    concurrently : bool, default True
        If True, each materialized view is refreshed using the CONCURRENTLY option
        (only supported when the view has a unique index that satisfies PostgreSQL
        requirements). If False, a standard blocking refresh is performed.

    Behavior
    --------
    - If inspection of the connection fails or returns no inspector, the function
      exits without performing any work.
    - Each materialized view name returned by the inspector is passed to
      `refresh_mat_view(session, name, concurrently)`.

    Notes
    -----
    - Using CONCURRENTLY allows reads during refresh at the cost of requiring an
      appropriate unique index and potentially being slower.
    - Exceptions raised during individual refresh operations will propagate unless
      `refresh_mat_view` handles them internally; in such a case the commit will
      not be reached.
    - Ensure the session is in a clean transactional state if you rely on
      consistent snapshot semantics.
    - This function commits no changes; it is the caller's responsibility to
      commit the session if needed.
    """
    inspector = sa.inspect(session.connection())

    if not inspector:
        return

    for mv in inspector.get_materialized_view_names():
        refresh_mat_view(session, mv, concurrently)
