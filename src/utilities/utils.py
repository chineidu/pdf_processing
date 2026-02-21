import hashlib
import os
import re
from typing import TYPE_CHECKING, Any

import msgspec
import torch

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


def log_system_info() -> None:
    """
    Utility function to log system resource details for debugging and monitoring.
    It captures GPU details (if available), CPU info, memory usage, and container limits.
    """

    # 1. GPU Logic (Keep your existing CUDA check)
    if torch.cuda.is_available():
        try:
            device_name = torch.cuda.get_device_name(0)
            properties = torch.cuda.get_device_properties(0)
            total_gb = properties.total_memory / (1024**3)
            allocated_gb = torch.cuda.memory_allocated() / (1024**3)
            reserved_gb = torch.cuda.memory_reserved() / (1024**3)

            logger.info(f"🔥 CUDA GPU: {device_name} ({total_gb:.2f} GB total)")
            logger.info(
                f"   GPU RAM - Allocated: {allocated_gb:.2f}GB, Reserved: {reserved_gb:.2f}GB"
            )
            return
        except Exception as e:
            logger.warning(f"Failed to log GPU info: {e}")

    # 2. Process-Level Context (Critical for Threads/Celery)
    import platform

    import psutil

    process = psutil.Process(os.getpid())
    process_mem_gb = process.memory_info().rss / (1024**3)

    # 3. CPU & Global Host Info
    cpu_count = os.cpu_count() or 0
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    host_total_gb = memory.total / (1024**3)
    host_avail_gb = memory.available / (1024**3)

    # 4. Container Limit Detection (The "Docker Lie" Fix)
    container_limit_gb = None
    if platform.system() == "Linux":
        # Check cgroup v2 (modern Docker/K8s)
        try:
            if os.path.exists("/sys/fs/cgroup/memory.max"):
                with open("/sys/fs/cgroup/memory.max", "r") as f:
                    val = f.read().strip()
                    if val != "max":
                        container_limit_gb = int(val) / (1024**3)
        except Exception:
            logger.warning("Failed to read cgroup v2 memory limit.")
            pass

    # 5. Load Average (Detects CPU over-subscription)
    load_avg = ""
    if hasattr(os, "getloadavg"):
        load_avg = f" | Load: {os.getloadavg()[0]:.2f}"

    logger.info("🚨 Inference Resource Report (CPU Mode)")
    logger.info(f"💻 CPU: {cpu_count} cores, Usage: {cpu_percent}%{load_avg}")

    # Log Process vs System
    logger.info(f"Worker Process RAM: {process_mem_gb:.2f} GB (RSS)")

    limit_msg = (
        f"{container_limit_gb:.2f} GB (Container Limit)"
        if container_limit_gb
        else f"{host_total_gb:.2f} GB (Host Total)"
    )
    logger.info(f"   System RAM - Available: {host_avail_gb:.2f} GB / {limit_msg}")
