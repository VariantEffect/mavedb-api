# From https://improveandrepeat.com/2021/09/python-friday-87-handling-pre-existing-tables-with-alembic-and-sqlalchemy/
# Based on https://github.com/talkpython/data-driven-web-apps-with-flask

from alembic import op
from sqlalchemy import engine_from_config
from sqlalchemy.engine import reflection


def table_does_not_exist(table, schema=None):
    config = op.get_context().config
    engine = engine_from_config(config.get_section(config.config_ini_section), prefix="sqlalchemy.")
    insp = reflection.Inspector.from_engine(engine)
    return insp.has_table(table, schema) is False


def table_has_column(table, column):
    config = op.get_context().config
    engine = engine_from_config(config.get_section(config.config_ini_section), prefix="sqlalchemy.")
    insp = reflection.Inspector.from_engine(engine)
    has_column = False
    for col in insp.get_columns(table):
        if column not in col["name"]:
            continue
        has_column = True
    return has_column
