from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from src.config import app_config
from src.schemas.types import PriorityEnum

if TYPE_CHECKING:
    pass


class QueueInfo(NamedTuple):
    """Information about a queue and its priority.

    Attributes
    ----------
    queue_name : str
        The name of the queue to dispatch to.
    priority : int
        The priority level (1-10, where 10 is highest).
    """

    queue_name: str
    priority: int


def map_priority_enum_to_int(priority: PriorityEnum | str) -> int:
    """Convert PriorityEnum to its string representation.

    Parameters
    ----------
    priority : PriorityEnum | str
        The priority level as an enum or string.

    Returns
    -------
    str
        The string representation of the priority level.
    """
    valid_priorities = ", ".join({e.value for e in PriorityEnum})
    try:
        if isinstance(priority, str):
            priority = PriorityEnum(priority.lower())
        else:
            priority = priority

    except ValueError:
        raise ValueError(
            f"priority must be one of ({valid_priorities}), got '{priority}'"
        ) from None

    mapping: dict[PriorityEnum, int] = {
        PriorityEnum.LOW: app_config.queue_config.priority_config.weights.low_priority,
        PriorityEnum.MEDIUM: app_config.queue_config.priority_config.weights.medium_priority,
        PriorityEnum.HIGH: app_config.queue_config.priority_config.weights.high_priority,
    }
    # Default to MEDIUM if not found
    return mapping.get(priority, 6)


def get_pdf_page_count(filepath: str | Path) -> int:
    """Get the page count of a PDF file.

    Parameters
    ----------
    filepath : str | Path
        Path to the PDF file.

    Returns
    -------
    int
        Number of pages in the PDF. Returns 0 if unable to determine.
    """
    try:
        import sys

        import pymupdf

        DEFAULT_SIZE: int = 50

        with open(filepath, "rb") as f:
            pdf_reader = pymupdf.open(f)
            return pdf_reader.page_count

    except Exception as e:
        sys.stderr.write(f"Error reading PDF page count: {e}. Using default.\n")
        return DEFAULT_SIZE


def get_queue_and_priority(num_pages: int) -> QueueInfo:
    """Determine the queue and priority based on the number of pages.

    Routing rules:
    - < 5 pages: high_priority_ml queue with HIGH priority
    - < 20 pages: medium_priority_ml queue with MEDIUM priority
    - >= 20 pages: low_priority_ml queue with LOW priority

    Parameters
    ----------
    num_pages : int
        Number of pages in the PDF.

    Returns
    -------
    QueueInfo
        Named tuple with queue_name and priority level.
    """
    if num_pages < app_config.queue_config.priority_config.sizes.high_priority:
        return QueueInfo(
            queue_name=app_config.queue_config.high_priority_ml,
            priority=map_priority_enum_to_int(PriorityEnum.HIGH),
        )
    if num_pages < app_config.queue_config.priority_config.sizes.medium_priority:
        return QueueInfo(
            queue_name=app_config.queue_config.medium_priority_ml,
            priority=map_priority_enum_to_int(PriorityEnum.MEDIUM),
        )
    return QueueInfo(
        queue_name=app_config.queue_config.low_priority_ml,
        priority=map_priority_enum_to_int(PriorityEnum.LOW),
    )
