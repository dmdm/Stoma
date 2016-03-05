import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

import stoma.models
from pym.rc import Rc

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = stoma.models.DbBase.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

PYM_ENV = config.get_main_option("environment")
if not PYM_ENV:
    raise KeyError('Missing key "environment" in config.')
# The directory of the config file is our root_dir
rc = Rc(environment=PYM_ENV,
    root_dir=os.path.normpath(
        os.path.join(os.path.dirname(__file__), '..', '..', '..')
    )
)
rc.load()


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = rc.g('db.stoma.sa.url')
    context.configure(
        url=url,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True
    )

    with context.begin_transaction():
        context.run_migrations(rc=rc)


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    engine = engine_from_config(
        rc.data,
        prefix='db.pym.sa.',
        poolclass=pool.NullPool
    )

    connection = engine.connect()
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True
    )

    try:
        with context.begin_transaction():
            context.run_migrations(rc=rc)
    finally:
        connection.close()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
