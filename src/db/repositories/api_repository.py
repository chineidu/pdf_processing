"""
Crud operations for the api_key repository.

(Using SQLAlchemy ORM v2.x)
"""

from datetime import datetime
from typing import Any

from dateutil.parser import parse  # Very fast, handles ISO formats well
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)
from sqlalchemy.orm import selectinload

from src import create_logger
from src.db.models import DBAPIKey, DBClient, aget_db
from src.schemas.db.models import APIKeySchema

logger = create_logger(__name__)


class APIKeyRepository:
    """CRUD operations for the api_key repository."""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # ----- Read operations -----
    async def aget_api_key_by_id(self, id: int) -> DBAPIKey | None:
        """Get a api_key by its ID with eager loading of client and roles."""
        try:
            stmt = (
                select(DBAPIKey)
                .where(DBAPIKey.id == id)
                .options(selectinload(DBAPIKey.client).selectinload(DBClient.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching api_key by id '{id}': {e}")
            return None

    async def aget_api_key_by_prefix(self, key_prefix: str) -> DBAPIKey | None:
        """Get a api_key by its key prefix with eager loading of client and roles."""
        try:
            stmt = (
                select(DBAPIKey)
                .where(DBAPIKey.key_prefix == key_prefix)
                # Eager load client, then client's roles to avoid N+1 queries
                .options(selectinload(DBAPIKey.client).selectinload(DBClient.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching api_key by key prefix '{key_prefix}': {e}")
            return None

    async def aget_api_key_by_creation_time(
        self, client_id: int, created_after: str, created_before: str
    ) -> list[DBAPIKey]:
        """Get api_key created within a specific time range. Uses database-level comparison.

        Parameters
        ----------
        client_id : int
            The ID of the client whose API keys are being queried.
        created_after : str
            The start timestamp (inclusive). e.g. "2023-01-01T00:00:00"
        created_before : str
            The end timestamp (inclusive). e.g. "2023-01-31T23:59:59"

        Returns
        -------
        list[DBAPIKey]
            List of api_key created within the specified time range.
        """
        # Internal check: ensures the strings are at least valid dates
        # before hitting the DB
        try:
            start: datetime = parse(created_after)
            end: datetime = parse(created_before)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid date format passed to query: {e}")
            raise ValueError("Timestamps must be valid ISO 8601 strings.") from e

        stmt = select(DBAPIKey).where(
            DBAPIKey.client_id == client_id,
            DBAPIKey.created_at >= start,
            DBAPIKey.created_at <= end,
        )
        result = await self.db.scalars(stmt)
        return list(result.all())

    async def aget_api_key_by_last_used_time(
        self, client_id: int, last_used_after: str, last_used_before: str
    ) -> list[DBAPIKey]:
        """Get api_key last used within a certain time period. Uses database-level comparison.

        Parameters
        ----------
        client_id : int
            The ID of the client whose API keys are being queried.
        last_used_after : str
            The start timestamp (inclusive). e.g. "2023-01-01T00:00:00"
        last_used_before : str
            The end timestamp (inclusive). e.g. "2023-01-31T23:59:59"

        Returns
        -------
        list[DBAPIKey]
            List of api_key last_used within the specified time range.
        """
        # Internal check: ensures the strings are at least valid dates
        # before hitting the DB
        try:
            start: datetime = parse(last_used_after)
            end: datetime = parse(last_used_before)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid date format passed to query: {e}")
            raise ValueError("Timestamps must be valid ISO 8601 strings.") from e

        stmt = select(DBAPIKey).where(
            DBAPIKey.client_id == client_id,
            DBAPIKey.last_used_at >= start,
            DBAPIKey.last_used_at <= end,
        )
        result = await self.db.scalars(stmt)
        return list(result.all())

    async def aget_keys_by_owner(self, owner_id: int) -> list[DBAPIKey]:
        """Get all API keys belonging to a client."""
        try:
            stmt = (
                select(DBAPIKey)
                .where(DBAPIKey.client_id == owner_id)
                .order_by(DBAPIKey.created_at.desc())
            )
            result = await self.db.scalars(stmt)
            return list(result.all())
        except Exception as e:
            logger.error(f"Error fetching api_keys for owner_id '{owner_id}': {e}")
            return []

    # ----- Create operations -----
    async def acreate_api_key(self, api_key_obj: APIKeySchema) -> int:
        """Create api_key in the database."""
        try:
            data = api_key_obj.model_dump(exclude={"id", "created_at", "last_used_at"})
        except Exception as e:
            logger.error(f"Error preparing api_key for creation: {e}")
            raise

        db_obj = DBAPIKey(**data)

        try:
            self.db.add(db_obj)
            await self.db.commit()

            # Refresh to get the auto-generated ID
            await self.db.refresh(db_obj)
            logger.info(
                f"Successfully created api_key with {db_obj.id} in the database."
            )
            return db_obj.id

        except IntegrityError as e:
            logger.error(f"Integrity error creating api_key: {e}")
            await self.db.rollback()
            raise
        except Exception as e:
            logger.error(f"Error creating api_key: {e}")
            await self.db.rollback()
            raise e

    # ----- Update operations -----
    async def aupdate_api_key(
        self, key_id: int, client_id: int, update_data: dict[str, Any]
    ) -> DBAPIKey | None:
        """Update a api_key in the database in a single round trip.

        Note
        ----
        - Only allows updating certain fields to prevent unauthorized changes.
        - Allowed fields: name, requests_per_minute, expires_at, is_active
        """

        # Fetch the existing api_key
        stmt = (
            select(DBAPIKey)
            .where(DBAPIKey.id == key_id, DBAPIKey.client_id == client_id)
            # Lock the row (prevents race conditions)
            .with_for_update()
        )
        result = await self.db.execute(stmt)
        db_api_key: DBAPIKey | None = result.scalar_one_or_none()

        if not db_api_key:
            logger.warning(f"API Key {key_id} not found for client {client_id}")
            return None

        # Update the data
        ALLOWED_FIELDS = {"name", "requests_per_minute", "expires_at", "is_active"}
        has_changes = False

        for field, value in update_data.items():
            if field not in ALLOWED_FIELDS:
                logger.warning(
                    f"Attempt to update disallowed field '{field}' on API Key {key_id}"
                )
                continue

            # If the current field value is different, update it
            current_value = getattr(db_api_key, field)
            if current_value != value:
                setattr(db_api_key, field, value)
                has_changes = True

        if not has_changes:
            logger.info(f"No changes detected for API Key {key_id}. Skipping update.")
            return db_api_key

        try:
            await self.db.commit()
            logger.info(f"Successfully updated API Key {key_id} for client {client_id}")
            return db_api_key

        except Exception as e:
            logger.error(f"Error updating API Key {key_id} for client {client_id}: {e}")
            await self.db.rollback()
            raise

    # ----- Delete operations -----
    async def adelete_owned_key(self, key_id: int, owner_id: int) -> bool:
        """
        Delete a key only if it belongs to the specific client.
        Returns True if deleted, False if not found/not owned.
        """
        try:
            stmt = (
                delete(DBAPIKey)
                .where(DBAPIKey.id == key_id)
                .where(DBAPIKey.client_id == owner_id)  # <--- CRITICAL SECURITY CHECK
            )
            result = await self.db.execute(stmt)
            await self.db.commit()
            return result.rowcount > 0  # type: ignore

        except Exception as e:
            logger.error(
                f"Error deleting api_key id {key_id} for owner {owner_id}: {e}"
            )
            await self.db.rollback()
            return False

    # ----- Conversion operations -----
    def convert_DBAPIKey_to_schema(self, db_api_key: DBAPIKey) -> APIKeySchema | None:  # noqa: N802
        """Convert a DBAPIKey ORM object directly to a Pydantic response schema."""
        try:
            return APIKeySchema.model_validate(db_api_key)
        except Exception as e:
            logger.error(f"Error converting DBAPIKey to ApiKeySchema: {e}")
            return None


# ----- Custom functions -----
async def aupdate_last_used_at(key_id: int) -> None:
    """Update the last_used_at timestamp for an API key to the current time."""

    async for session in aget_db():
        try:
            stmt = (
                update(DBAPIKey)
                .where(DBAPIKey.id == key_id)
                .values(last_used_at=func.now())
            )
            await session.execute(stmt)
            await session.commit()
            logger.info(f"Updated last_used_at for key {key_id}")
        except Exception as e:
            logger.error(f"Failed to update timestamp for key {key_id}: {e}")
        break
