from src.schemas.types import PriorityEnum


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
        PriorityEnum.LOW: 1,
        PriorityEnum.MEDIUM: 6,
        PriorityEnum.HIGH: 10,
    }
    # Default to MEDIUM if not found
    return mapping.get(priority, 6)
