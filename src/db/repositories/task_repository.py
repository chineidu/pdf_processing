"""
Crud operations for the task repository.

(Using SQLAlchemy ORM v2.x)
"""

from datetime import datetime, timezone

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
    async def aupdate_task_status(
        self, task_id: str, new_status: StatusTypeEnum, add_completed_at: bool = False
    ) -> DBTask | None:
        """Update the status of a task.

        Parameters
        ----------
        task_id : str
            Unique identifier of the task to update.
        new_status : StatusTypeEnum
            New status to set for the task.
        add_completed_at : bool, optional
            Whether to set the completed_at timestamp if the new status is 'completed' (default is False).

        Returns
        -------
        DBTask | None
            The updated task object if successful, or None if the task was not found or an error occurred.
        """
        try:
            stmt = (
                select(DBTask)
                .where(DBTask.task_id == task_id)
                # Lock the row (prevents race conditions)
                .with_for_update()
            )
            db_task = await self.db.scalar(stmt)
            if not db_task:
                logger.warning(f"Task with ID {task_id} not found!")
                return None

            db_task.status: str = new_status.value  # type: ignore
            if add_completed_at and new_status == StatusTypeEnum.COMPLETED:
                db_task.completed_at = datetime.now(timezone.utc)

            await self.db.commit()
            logger.info(f"Updated status of task {task_id} to '{new_status}'.")
            return db_task

        except Exception as e:
            logger.error(f"Error updating status of task {task_id}: {e}")
            await self.db.rollback()
            return None

    # ----- Conversion operations -----
    def convert_DBTask_to_schema(  # noqa: N802
        self, db_task: DBTask
    ) -> TaskSchema | None:
        """Convert a DBTask ORM object directly to a Pydantic response schema."""
        try:
            return TaskSchema.model_validate(db_task)
        except Exception as e:
            logger.error(f"Error converting DBTask to TaskSchema: {e}")
            return None
