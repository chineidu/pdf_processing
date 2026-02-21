import time
from datetime import datetime, timezone
from typing import Any

from celery import shared_task

from src import create_logger
from src.celery_app import BaseCustomTask
from src.schemas.types import StatusTypeEnum

logger = create_logger(name=__name__)
logger.propagate = False  # This prevents double logging to the root logger


# Note: When `bind=True`, celery automatically passes the task instance as the first argument
# meaning that we need to use `self` and this provides additional functionality like retries, etc
@shared_task(bind=True, base=BaseCustomTask)
def process_data(
    self,  # noqa: ANN001, ARG001
    task_id: str,
    analysis_id: str = "unknown",
) -> dict[str, Any]:
    """Celery task to process data"""

    # Check if task_id is present in the DB
    # Download the file from S3 using the task_id (which is the same as the S3 key)
    # Validate the file (e.g. check if it's a valid PDF, check file size, etc.)
    # Process the file (e.g. extract text, run analysis, etc.)
    # Store the results metadata back in the DB (e.g. status, file size, number of pages, etc.)
    # Store the raw results back in S3 (e.g. extracted text, analysis results, etc.)
    # Return the summary of the results (e.g. status, file size, number of pages, etc.)

    logger.info(f"Started processing task_id: {task_id}, analysis_id: {analysis_id}")

    try:
        time.sleep(5)  # Simulate some processing time

        success = True
        return {
            "status": StatusTypeEnum.COMPLETED.value
            if success
            else StatusTypeEnum.FAILED.value,
            "success": success,
            "task_id": self.request.id,  # Accessing the task ID from the task request context
            "analysis_id": analysis_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.error(f"Error processing task_id {task_id}: {e}", exc_info=True)
        raise self.retry(exc=e) from e
