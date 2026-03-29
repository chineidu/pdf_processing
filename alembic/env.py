# type: ignore
import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine.base import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context
from src.config import app_settings
from src.db.models import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config
# =============================================================
# ==================== Add DB Config ==========================
# =============================================================
config.set_main_option("sqlalchemy.url", app_settings.database_url)

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
# ============ Add metadata ============
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    """
    Execute migrations in a synchronous context within an async connection.

    This function acts as a synchronous bridge for SQLAlchemy's AsyncEngine.
    It is invoked via `connection.run_sync` to allow Alembic's inherently
    synchronous migration context to execute commands over an asynchronous
    DBAPI (asyncpg).

    Parameters
    ----------
        connection: Connection
            An active asyncpg connection wrapped by SQLAlchemy's AsyncEngine.
    """
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


# ============ For Async Migrations ============
async def arun_migrations_online() -> None:
    """Run migrations in 'online' mode using AsyncEngine."""

    # Create the configuration for the async engine
    configuration = config.get_section(config.config_ini_section)
    if configuration is None:
        configuration = {}

    # We use async_engine_from_config instead of engine_from_config
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        # We must use run_sync to bridge the gap between
        # Alembic's sync requirements and our async connection
        await connection.run_sync(_do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    # Use asyncio.run to execute the async entry point
    asyncio.run(arun_migrations_online())
