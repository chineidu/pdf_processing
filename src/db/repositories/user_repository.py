"""
Crud operations for the user repository.

(Using SQLAlchemy ORM v2.x)
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

from dateutil.parser import parse  # Very fast, handles ISO formats well
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)
from sqlalchemy.orm import selectinload

from src import create_logger
from src.db.models import DBUser
from src.schemas.db.models import BaseUserSchema, UserSchema
from src.schemas.types import RoleTypeEnum, UserStatusEnum

logger = create_logger(__name__)


class UserRepository:
    """CRUD operations for the User repository."""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # ----- Read operations -----
    async def aget_user_by_id(self, id: int) -> DBUser | None:
        """Get a user by its ID with eager loading."""
        try:
            stmt = (
                select(DBUser)
                .where(DBUser.id == id)
                .options(selectinload(DBUser.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching user by id '{id}': {e}")
            return None

    async def aget_user_by_external_id(self, external_id: str) -> DBUser | None:
        """Get a user by its external ID with eager loading."""
        try:
            stmt = (
                select(DBUser)
                .where(DBUser.external_id == external_id)
                .options(selectinload(DBUser.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching user by external_id '{external_id}': {e}")
            return None

    async def aget_users_by_external_ids(self, external_ids: list[str]) -> list[DBUser]:
        """Get users by their external IDs with eager loading."""
        try:
            stmt = (
                select(DBUser)
                .where(DBUser.external_id.in_(external_ids))
                .options(selectinload(DBUser.roles))
            )
            result = await self.db.scalars(stmt)
            return list(result.all())
        except Exception as e:
            logger.error(f"Error fetching users by ids {external_ids}: {e}")
            return []

    async def aget_user_by_name(self, name: str) -> DBUser | None:
        """Get a user by its name with eager loading."""
        try:
            stmt = (
                select(DBUser)
                .where(DBUser.name == name)
                .options(selectinload(DBUser.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching user by name '{name}': {e}")
            return None

    async def aget_user_by_email(self, email: str) -> DBUser | None:
        """Get a user by its email with eager loading."""
        try:
            stmt = (
                select(DBUser)
                .where(DBUser.email == email)
                .options(selectinload(DBUser.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching user by email '{email}': {e}")
            return None

    async def aget_users_cursor(
        self, limit: int = 20, last_seen_id: int | None = None
    ) -> tuple[list[DBUser], int | None]:
        """
        Fetch users using cursor-based pagination (Seek Method) with eager loading.

        Parameters
        ----------
        limit : int, optional
            Number of users to fetch, by default 20
        last_seen_id : int | None, optional
            The ID of the last seen user from the previous page, by default None

        Returns
        -------
        tuple[list[DBUser], int | None]
            A tuple containing the list of users and the next cursor (last user's ID) or
            None if no more records.
        """
        try:
            query = (
                select(DBUser)
                .order_by(DBUser.id.asc())
                .limit(limit)
                .options(selectinload(DBUser.roles))
            )

            # If we have a cursor, seek to the next record
            if last_seen_id is not None:
                query = query.where(DBUser.id > last_seen_id)

            result = await self.db.scalars(query)
            users = list(result.all())

            # Calculate the next cursor
            next_cursor = users[-1].id if users else None

            return (users, next_cursor)

        except Exception as e:
            logger.error(f"Error fetching users with cursor {last_seen_id}: {e}")
            return [], None

    async def aget_users_by_creation_time(
        self, created_after: str, created_before: str
    ) -> list[DBUser]:
        """Get users created within a specific time range. Uses database-level comparison.

        Parameters
        ----------
        created_after : str
            The start timestamp (inclusive). e.g. "2023-01-01T00:00:00"
        created_before : str
            The end timestamp (inclusive). e.g. "2023-01-31T23:59:59"

        Returns
        -------
        list[DBUser]
            List of users created within the specified time range.
        """
        # Internal check: ensures the strings are at least valid dates
        # before hitting the DB
        try:
            start: datetime = parse(created_after)
            end: datetime = parse(created_before)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid date format passed to query: {e}")
            raise ValueError("Timestamps must be valid ISO 8601 strings.") from e

        stmt = (
            select(DBUser)
            .where(
                DBUser.created_at >= start,
                DBUser.created_at <= end,
            )
            .options(selectinload(DBUser.roles))
        )
        result = await self.db.scalars(stmt)
        return list(result.all())

    # ----- Create operations -----
    async def acreate_user(self, user: UserSchema) -> int:
        """Create user in the database."""
        try:
            db_user = DBUser(
                **user.model_dump(exclude={"id", "created_at", "updated_at"})
            )
        except Exception as e:
            logger.error(f"Error preparing users for creation: {e}")
            raise e

        try:
            self.db.add(db_user)
            await self.db.commit()

            # Refresh to get the auto-generated ID
            await self.db.refresh(db_user)

            logger.info(
                f"Successfully created user with ID {db_user.id} in the database."
            )
            return db_user.id

        except IntegrityError as e:
            logger.error(f"Integrity error creating user: {e}")
            await self.db.rollback()
            raise e

        except Exception as e:
            logger.error(f"Error creating user: {e}")
            await self.db.rollback()
            raise e

    # ----- Update operations -----
    async def aupdate_user(
        self, user_id: int, update_data: dict[str, Any]
    ) -> DBUser | None:
        """Update a user in the database in a single round trip.

        Note
        ----
        - Only allows updating certain fields to prevent unauthorized changes.
        - Allowed fields: tier, status, credits, is_active
        """

        # Fetch the existing user
        stmt = (
            select(DBUser)
            .where(DBUser.id == user_id)
            # Lock the row (prevents race conditions)
            .with_for_update()
        )
        result = await self.db.execute(stmt)
        db_user: DBUser | None = result.scalar_one_or_none()

        if not db_user:
            logger.warning(f"User id {user_id} not found!")
            return None

        # Update the data
        ALLOWED_FIELDS = {"tier", "status", "credits", "is_active"}
        has_changes = False

        for field, value in update_data.items():
            if field not in ALLOWED_FIELDS:
                logger.warning(
                    f"Attempt to update disallowed field '{field}' on user {user_id}"
                )
                continue

            # If the current field value is different, update it
            current_value = getattr(db_user, field)
            if current_value != value:
                setattr(db_user, field, value)
                has_changes = True

        if not has_changes:
            logger.info(f"No changes detected for user {user_id}. Skipping update.")
            return db_user

        try:
            await self.db.commit()
            logger.info(f"Successfully updated user {user_id}")
            return db_user

        except Exception as e:
            logger.error(f"Error updating user {user_id}: {e}")
            await self.db.rollback()
            raise

    async def aupdate_credits(self, user_id: int, amount: float) -> DBUser | None:
        """Update the credits balance for a user."""

        # Fetch the existing user
        stmt = (
            select(DBUser)
            .where(DBUser.id == user_id)
            # Lock the row (prevents race conditions)
            .with_for_update()
        )
        result = await self.db.execute(stmt)
        db_user: DBUser | None = result.scalar_one_or_none()

        if not db_user:
            logger.warning(f"User {user_id} not found")
            return None

        # Update the data
        new_balance = db_user.credits + Decimal(amount)
        # Update
        db_user.credits = new_balance

        try:
            await self.db.commit()
            await self.db.refresh(db_user)
            logger.info(
                f"Adjusted credits for {user_id}: {amount:+.2f}. New Balance: {db_user.credits}"
            )
            return db_user

        except Exception as e:
            logger.error(f"Error adjusting credits for {user_id}: {e}")
            await self.db.rollback()
            raise e

    async def aassign_role_to_user(self, user_id: int, role: RoleTypeEnum) -> None:
        """Assign a role to a user via the user_roles association table.

        Parameters
        ----------
        user_id : int
            The unique user identifier.
        role : RoleTypeEnum
            The role enum to assign.

        Raises
        ------
        Exception
            If the assignment fails.
        """
        try:
            from src.db.models import DBRole, user_roles

            # Get the role by name
            stmt = select(DBRole).where(DBRole.name == role.value)
            db_role = await self.db.scalar(stmt)

            if not db_role:
                logger.error(f"Role '{role.value}' not found in database.")
                raise ValueError(f"Role '{role.value}' does not exist.")

            # Insert into user_roles association table
            insert_stmt = user_roles.insert().values(
                user_id=user_id, role_id=db_role.id
            )
            await self.db.execute(insert_stmt)
            await self.db.commit()
            logger.info(f"Assigned role='{role.value}' to user_id='{user_id}'.")

        except Exception as e:
            logger.error(
                f"Error assigning role='{role.value}' to user_id='{user_id}': {e}"
            )
            await self.db.rollback()
            raise e

    async def abatch_update_users(self, users: list[UserSchema]) -> None:
        """Batch update users in the database."""
        external_ids = [user.external_id for user in users]
        existing_users = await self.aget_users_by_external_ids(external_ids)
        existing_users_dict = {user.external_id: user for user in existing_users}

        for user in users:
            existing_user = existing_users_dict.get(user.external_id)
            if not existing_user:
                logger.warning(
                    f"User with external_id {user.external_id!r} does not exist. Skipping update."
                )
                continue

            # Filter out fields that are None/excluded
            updated_values = {
                k: v
                for k, v in user.model_dump(
                    exclude={"id", "created_at", "updated_at"}
                ).items()
                if v is not None
            }
            # Update only the fields that are provided
            for field, value in updated_values.items():
                setattr(existing_user, field, value)

        try:
            self.db.add_all(existing_users)
            await self.db.commit()
            logger.info(
                f"Successfully completed batch update of {len(existing_users)} users in the database."
            )

        except Exception as e:
            logger.error(f"Error batch updating users: {e}")
            await self.db.rollback()
            raise e

    async def aupdate_user_status(
        self, external_id: str, status: UserStatusEnum
    ) -> None:
        """Update the status of a user.

        Parameters
        ----------
        external_id : str
            The unique user identifier.
        status : UserStatusEnum
            The new status to set for the user.

        Raises
        ------
        Exception
            If the update fails.
        """
        try:
            update_values: dict[str, Any] = {"status": status.value}

            stmt = (
                update(DBUser)
                .where(DBUser.external_id == external_id)
                .values(**update_values)
            )
            await self.db.execute(stmt)
            await self.db.commit()
            logger.info(f"Marked external_id='{external_id}' as {status.name}.")

        except Exception as e:
            logger.error(
                f"Error marking external_id='{external_id}' as {status.name}: {e}"
            )
            await self.db.rollback()
            raise e

    # ----- Conversion operations -----
    def convert_DBUser_to_schema(  # noqa: N802
        self, db_user: DBUser
    ) -> BaseUserSchema | None:
        """Convert a DBUser ORM object directly to a Pydantic response schema."""
        try:
            return BaseUserSchema.model_validate(db_user)
        except Exception as e:
            logger.error(f"Error converting DBUser to BaseUserSchema: {e}")
            return None
