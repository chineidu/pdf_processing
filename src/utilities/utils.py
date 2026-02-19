import hashlib
import re
from typing import TYPE_CHECKING, Any

import msgspec

from src import create_logger
from src.schemas.types import TierEnum

if TYPE_CHECKING:
    pass

logger = create_logger(name=__name__)

# JSON encoder
MSGSPEC_ENCODER = msgspec.json.Encoder()

# JSON decoder
MSGSPEC_DECODER = msgspec.json.Decoder()


def sort_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively sort a dictionary by its keys.

    Parameters
    ----------
    data : dict[str, Any]
        The dictionary to sort.

    Returns
    -------
    dict[str, Any]
        A new dictionary with keys sorted recursively.
    """
    # Base case: if the data is not a dictionary, return it as is
    if not isinstance(data, dict):
        return data
    # Recursive case: sort the dictionary
    return {key: sort_dict(data[key]) for key in sorted(data)}


def generate_idempotency_key(
    payload: dict[str, Any], user_id: str | None = None
) -> str:
    """Generate an idempotency key based on the payload and optional user ID."""
    hasher = hashlib.sha256()
    if user_id:
        data: dict[str, Any] = {"payload": payload, "user_id": user_id}
    else:
        data = {"payload": payload}

    # Serialize the payload using msgspec for consistent hashing
    serialized_payload = MSGSPEC_ENCODER.encode(sort_dict(data))
    hasher.update(serialized_payload)

    return hasher.hexdigest()


def calculate_latency_ewma(
    old_latency: float | None, current_latency: float, alpha: float = 0.2
) -> float:
    """
    Calculates the exponentially weighted moving average latency (EWMA).
    Using EWMA helps to smooth out short-term fluctuations and highlight
    longer-term trends in latency.
    """
    if not old_latency:
        return round(current_latency, 2)

    new_latency = (alpha * current_latency) + ((1 - alpha) * old_latency)
    return round(new_latency, 2)


def get_ratelimit_value(tier: TierEnum) -> str:
    """Get the rate limit value based on the client tier.

    Parameters
    ----------
    tier : TierEnum
        The tier of the client.

    Returns
    -------
    str
        The rate limit string (e.g., "10/minute").
    """

    if tier == TierEnum.GUEST:
        return "10/minute"
    if tier == TierEnum.FREE:
        return "20/minute"
    if tier == TierEnum.PLUS:
        return "60/minute"
    if tier == TierEnum.PRO:
        return "120/minute"

    # Default to FREE tier limits
    return "10/minute"


def extract_rate_limit_number(tier: TierEnum, default: int = 5) -> int:
    """Extract the numerical rate limit from the rate limit string for a given tier.

    Parameters
    ----------
    tier : TierEnum
        The tier of the client.
    default : int, optional
        The default rate limit number to return if extraction fails, by default 5

    Returns
    -------
    int
        The numerical rate limit extracted from the rate limit string.
    """
    pattern = r"\d{1,3}"
    limit_rate = get_ratelimit_value(tier)

    # Extract number from rate limit string
    match = re.search(pattern, string=limit_rate)
    if match:
        result = match.group()
    else:
        # Handle the case where no match is found
        result = default

    return int(result)
