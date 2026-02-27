"""
Crud operations for the task repository.

(Using SQLAlchemy ORM v2.x)
"""

from datetime import datetime, timezone
from typing import Any

from dateutil.parser import parse  # Very fast, handles ISO formats well
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)
from sqlalchemy.orm import selectinload

from src import create_logger
from src.db.models import DBTask
from src.schemas.db.models import TaskSchema
from src.schemas.types import StatusTypeEnum

logger = create_logger(__name__)


class TaskRepository:
    """Repository for managing Task records in the database."""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # ----- Read operations -----
    async def aget_task_by_task_id(self, task_id: str) -> DBTask | None:
        """Fetch a task by its unique task ID."""
        try:
            stmt = (
                select(DBTask)
                .where(DBTask.task_id == task_id)
                # Eager loading to avoid N+1 queries
                .options(selectinload(DBTask.user))
            )
            return await self.db.scalar(stmt)
        except Exception as e:
            logger.error(f"Error fetching task with ID {task_id}: {e}")
            return None

    async def aget_tasks_by_status(self, status: StatusTypeEnum) -> list[DBTask]:
        """Fetch all tasks with a specific status."""
        try:
            stmt = (
                select(DBTask)
                .where(DBTask.status == status)
                .options(selectinload(DBTask.user))
            )
            result = await self.db.scalars(stmt)
            return list(result.all())
        except Exception as e:
            logger.error(f"Error fetching tasks with status '{status}': {e}")
            return []

    async def aget_tasks_by_creation_time(
        self, created_after: str, created_before: str
    ) -> list[DBTask]:
        """Fetch all tasks created within a specific time range."""

        try:
            start: datetime = parse(created_after)
            end: datetime = parse(created_before)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid date format passed to query: {e}")
            raise ValueError("Timestamps must be valid ISO 8601 strings.") from e

        try:
            stmt = select(DBTask).where(
                DBTask.created_at >= start,
                DBTask.created_at <= end,
            )
            result = await self.db.scalars(stmt)
            return list(result.all())
        except Exception as e:
            logger.error(
                f"Error fetching tasks created between '{created_after}' and '{created_before}': {e}"
            )
            return []

    # ----- Create operations -----
    async def acreate_task(self, task: TaskSchema) -> DBTask:
        """Create a new task record in the database."""
        try:
            db_task = DBTask(
                **task.model_dump(
                    exclude={"id", "created_at", "updated_at", "completed_at"}
                )
            )
        except Exception as e:
            logger.error(f"Error initializing DBTask from TaskSchema: {e}")
            raise e

        try:
            self.db.add(db_task)
            await self.db.commit()

            # Refresh to get the auto-generated ID
            await self.db.refresh(db_task)
            logger.info(
                f"Successfully created task with ID {db_task.id} in the database."
            )
            return db_task

        except IntegrityError as e:
            logger.error(f"Integrity error creating task: {e}")
            await self.db.rollback()
            raise e

        except Exception as e:
            logger.error(f"Error creating task: {e}")
            await self.db.rollback()
            raise e

    # ----- Update operations -----
    async def aupdate_task(
        self, task_id: str, update_data: dict[str, Any], add_completed_at: bool = False
    ) -> DBTask | None:
        """Update the status of a task.

        Parameters
        ----------
        task_id : str
            Unique identifier of the task to update.
        update_data : dict[str, Any]
            Dictionary containing the fields to update. Expected keys include:
            - 'status' (StatusTypeEnum): New status of the task.
            - 'file_result_key' (str, optional): S3 key of the processed result file.
            - 'file_size_bytes' (int, optional): Size of the uploaded file in bytes.
            - 'file_type' (MimeTypeEnum, optional): MIME type of the uploaded file
            - 'etag' (str, optional): ETag of the uploaded file in S3.
            - 'error_message' (str, optional): Error message if the task failed.
            - 'webhook_delivered_at' (datetime, optional): Timestamp for webhook delivery.
        add_completed_at : bool, optional
            If True and status is a terminal state (`completed` or `failed`), set
            `completed_at` when it is currently empty.

        Returns
        -------
        DBTask | None
            The updated task object if successful, or None if the task was not found or an error occurred.
        """
        # Fetch the existing task
        stmt = (
            select(DBTask)
            .where(DBTask.task_id == task_id)
            # Lock the row (prevents race conditions)
            .with_for_update()
        )
        result = await self.db.execute(stmt)
        db_task: DBTask | None = result.scalar_one_or_none()

        if not db_task:
            logger.warning(f"Task {task_id} not found")
            return None

        # Update the data
        ALLOWED_FIELDS = {
            "status",
            "file_result_key",
            "file_size_bytes",
            "file_type",
            "etag",
            "error_message",
            "webhook_delivered_at",
        }
        has_changes = False

        for field, value in update_data.items():
            if field not in ALLOWED_FIELDS:
                logger.warning(
                    f"Attempt to update disallowed field '{field}' on Task {task_id}"
                )
                continue

            # If the current field value is different, update it
            current_value = getattr(db_task, field)
            if current_value != value:
                setattr(db_task, field, value)
                has_changes = True

        if add_completed_at and db_task.completed_at is None:
            status_value = update_data.get("status")
            if isinstance(status_value, StatusTypeEnum):
                status_value = status_value.value

            if status_value in {
                StatusTypeEnum.COMPLETED.value,
                StatusTypeEnum.FAILED.value,
            }:
                db_task.completed_at = datetime.now(timezone.utc)
                has_changes = True

        if not has_changes:
            logger.info(f"No changes detected for Task {task_id}. Skipping update.")
            return db_task

        try:
            await self.db.commit()
            logger.info(f"Successfully updated Task {task_id}")
            return db_task

        except Exception as e:
            logger.error(f"Error updating Task {task_id}: {e}")
            await self.db.rollback()
            raise
