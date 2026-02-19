"""
Crud operations for the client repository.

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
from src.db.models import DBClient
from src.schemas.db.models import BaseClientSchema, ClientSchema
from src.schemas.types import ClientStatusEnum, RoleTypeEnum

logger = create_logger(__name__)


class ClientRepository:
    """CRUD operations for the Client repository."""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # ----- Read operations -----
    async def aget_client_by_id(self, id: int) -> DBClient | None:
        """Get a client by its ID with eager loading."""
        try:
            stmt = (
                select(DBClient)
                .where(DBClient.id == id)
                .options(selectinload(DBClient.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching client by id '{id}': {e}")
            return None

    async def aget_client_by_external_id(self, external_id: str) -> DBClient | None:
        """Get a client by its external ID with eager loading."""
        try:
            stmt = (
                select(DBClient)
                .where(DBClient.external_id == external_id)
                .options(selectinload(DBClient.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching client by external_id '{external_id}': {e}")
            return None

    async def aget_client_by_external_ids(
        self, external_ids: list[str]
    ) -> list[DBClient]:
        """Get client by their external IDs with eager loading."""
        try:
            stmt = (
                select(DBClient)
                .where(DBClient.external_id.in_(external_ids))
                .options(selectinload(DBClient.roles))
            )
            result = await self.db.scalars(stmt)
            return list(result.all())
        except Exception as e:
            logger.error(f"Error fetching client by ids {external_ids}: {e}")
            return []

    async def aget_client_by_name(self, name: str) -> DBClient | None:
        """Get a client by its name with eager loading."""
        try:
            stmt = (
                select(DBClient)
                .where(DBClient.name == name)
                .options(selectinload(DBClient.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching client by name '{name}': {e}")
            return None

    async def aget_client_by_email(self, email: str) -> DBClient | None:
        """Get a client by its email with eager loading."""
        try:
            stmt = (
                select(DBClient)
                .where(DBClient.email == email)
                .options(selectinload(DBClient.roles))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching client by email '{email}': {e}")
            return None

    async def aget_clients_cursor(
        self, limit: int = 20, last_seen_id: int | None = None
    ) -> tuple[list[DBClient], int | None]:
        """
        Fetch clients using cursor-based pagination (Seek Method) with eager loading.

        Parameters
        ----------
        limit : int, optional
            Number of clients to fetch, by default 20
        last_seen_id : int | None, optional
            The ID of the last seen client from the previous page, by default None

        Returns
        -------
        tuple[list[DBClient], int | None]
            A tuple containing the list of clients and the next cursor (last client's ID) or
            None if no more records.
        """
        try:
            query = (
                select(DBClient)
                .order_by(DBClient.id.asc())
                .limit(limit)
                .options(selectinload(DBClient.roles))
            )

            # If we have a cursor, seek to the next record
            if last_seen_id is not None:
                query = query.where(DBClient.id > last_seen_id)

            result = await self.db.scalars(query)
            clients = list(result.all())

            # Calculate the next cursor
            next_cursor = clients[-1].id if clients else None

            return (clients, next_cursor)

        except Exception as e:
            logger.error(f"Error fetching clients with cursor {last_seen_id}: {e}")
            return [], None

    async def aget_clients_by_creation_time(
        self, created_after: str, created_before: str
    ) -> list[DBClient]:
        """Get clients created within a specific time range. Uses database-level comparison.

        Parameters
        ----------
        created_after : str
            The start timestamp (inclusive). e.g. "2023-01-01T00:00:00"
        created_before : str
            The end timestamp (inclusive). e.g. "2023-01-31T23:59:59"

        Returns
        -------
        list[DBClient]
            List of clients created within the specified time range.
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
            select(DBClient)
            .where(
                DBClient.created_at >= start,
                DBClient.created_at <= end,
            )
            .options(selectinload(DBClient.roles))
        )
        result = await self.db.scalars(stmt)
        return list(result.all())

    # ----- Create operations -----
    async def acreate_client(self, client: ClientSchema) -> int:
        """Create client in the database."""
        try:
            db_client = DBClient(
                **client.model_dump(exclude={"id", "created_at", "updated_at"})
            )
        except Exception as e:
            logger.error(f"Error preparing clients for creation: {e}")
            raise e

        try:
            self.db.add(db_client)
            await self.db.commit()

            # Refresh to get the auto-generated ID
            await self.db.refresh(db_client)

            logger.info(
                f"Successfully created client with ID {db_client.id} in the database."
            )
            return db_client.id

        except IntegrityError as e:
            logger.error(f"Integrity error creating client: {e}")
            await self.db.rollback()
            raise e

        except Exception as e:
            logger.error(f"Error creating client: {e}")
            await self.db.rollback()
            raise e

    # ----- Update operations -----
    async def aupdate_client(
        self, client_id: int, update_data: dict[str, Any]
    ) -> DBClient | None:
        """Update a api_key in the database in a single round trip.

        Note
        ----
        - Only allows updating certain fields to prevent unauthorized changes.
        - Allowed fields: tier, status, credits, is_active
        """

        # Fetch the existing api_key
        stmt = (
            select(DBClient)
            .where(DBClient.id == client_id)
            # Lock the row (prevents race conditions)
            .with_for_update()
        )
        result = await self.db.execute(stmt)
        db_client: DBClient | None = result.scalar_one_or_none()

        if not db_client:
            logger.warning(f"Client id {client_id} not found!")
            return None

        # Update the data
        ALLOWED_FIELDS = {"tier", "status", "credits", "is_active"}
        has_changes = False

        for field, value in update_data.items():
            if field not in ALLOWED_FIELDS:
                logger.warning(
                    f"Attempt to update disallowed field '{field}' on client {client_id}"
                )
                continue

            # If the current field value is different, update it
            current_value = getattr(db_client, field)
            if current_value != value:
                setattr(db_client, field, value)
                has_changes = True

        if not has_changes:
            logger.info(f"No changes detected for client {client_id}. Skipping update.")
            return db_client

        try:
            await self.db.commit()
            logger.info(f"Successfully updated client {client_id}")
            return db_client

        except Exception as e:
            logger.error(f"Error updating client {client_id}: {e}")
            await self.db.rollback()
            raise

    async def aupdate_credits(self, client_id: int, amount: float) -> DBClient | None:
        """Update the credits balance for a client."""

        # Fetch the existing api_key
        stmt = (
            select(DBClient)
            .where(DBClient.id == client_id)
            # Lock the row (prevents race conditions)
            .with_for_update()
        )
        result = await self.db.execute(stmt)
        db_client: DBClient | None = result.scalar_one_or_none()

        if not db_client:
            logger.warning(f"Client {client_id} not found")
            return None

        # Update the data
        new_balance = db_client.credits + Decimal(amount)
        # Update
        db_client.credits = new_balance

        try:
            await self.db.commit()
            await self.db.refresh(db_client)
            logger.info(
                f"Adjusted credits for {client_id}: {amount:+.2f}. New Balance: {db_client.credits}"
            )
            return db_client

        except Exception as e:
            logger.error(f"Error adjusting credits for {client_id}: {e}")
            await self.db.rollback()
            raise e

    async def aassign_role_to_client(self, client_id: int, role: RoleTypeEnum) -> None:
        """Assign a role to a client via the user_roles association table.

        Parameters
        ----------
        client_id : int
            The unique client identifier.
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
                client_id=client_id, role_id=db_role.id
            )
            await self.db.execute(insert_stmt)
            await self.db.commit()
            logger.info(f"Assigned role='{role.value}' to client_id='{client_id}'.")

        except Exception as e:
            logger.error(
                f"Error assigning role='{role.value}' to client_id='{client_id}': {e}"
            )
            await self.db.rollback()
            raise e

    async def abatch_update_clients(self, clients: list[ClientSchema]) -> None:
        """Batch update clients in the database."""
        external_ids = [client.external_id for client in clients]
        existing_clients = await self.aget_client_by_external_ids(external_ids)
        existing_clients_dict = {
            client.external_id: client for client in existing_clients
        }

        for client in clients:
            existing_client = existing_clients_dict.get(client.external_id)
            if not existing_client:
                logger.warning(
                    f"Client with external_id {client.external_id!r} does not exist. Skipping update."
                )
                continue

            # Filter out fields that are None/excluded
            updated_values = {
                k: v
                for k, v in client.model_dump(
                    exclude={"id", "created_at", "updated_at"}
                ).items()
                if v is not None
            }
            # Update only the fields that are provided
            for field, value in updated_values.items():
                setattr(existing_client, field, value)

        try:
            self.db.add_all(existing_clients)
            await self.db.commit()
            logger.info(
                f"Successfully completed batch update of {len(existing_clients)} clients in the database."
            )

        except Exception as e:
            logger.error(f"Error batch updating clients: {e}")
            await self.db.rollback()
            raise e

    async def aupdate_client_status(
        self, external_id: str, status: ClientStatusEnum
    ) -> None:
        """Update the status of a client.

        Parameters
        ----------
        external_id : str
            The unique client identifier.
        status : ClientStatusEnum
            The new status to set for the client.

        Raises
        ------
        Exception
            If the update fails.
        """
        try:
            update_values: dict[str, Any] = {"status": status.value}

            stmt = (
                update(DBClient)
                .where(DBClient.external_id == external_id)
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
    def convert_DBClient_to_schema(  # noqa: N802
        self, db_client: DBClient
    ) -> BaseClientSchema | None:
        """Convert a DBClient ORM object directly to a Pydantic response schema."""
        try:
            return BaseClientSchema.model_validate(db_client)
        except Exception as e:
            logger.error(f"Error converting DBClient to BaseClientSchema: {e}")
            return None
