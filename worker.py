import os
import sys
from typing import Any

import numpy as np
import torch

from src import create_logger
from src.celery_app.app import celery_app
from src.config import app_config, app_settings
from src.schemas.types import PoolType
from src.utilities.utils import log_system_info

logger = create_logger(name="worker")
rng = np.random.default_rng()
name: str = str(rng.integers(0, 10_000))


def _set_pooltype_and_processes() -> tuple[PoolType, int]:
    """Determine optimal pool type and number of processes based on hardware.

    Returns
    -------
    tuple[PoolType, int]
        The selected pool type and number of processes.
    """
    pool_env = os.environ.get("CELERY_POOL")
    if pool_env and pool_env.lower() in ["prefork", "threads"]:
        pool_type: PoolType = PoolType(pool_env.lower())
        logger.info(f"Using pool type from environment: {pool_type.value}")

    # Determine number of processes and pool type based on hardware availability
    if torch.cuda.is_available():
        # GPU: use threads pool (1 process) to avoid spawn/prefork conflicts with CUDA
        # Model is loaded once in the main process and shared across threads
        pool_type = PoolType.THREADS
        num_processes: int = min(os.cpu_count() or 1, 4)
        logger.info(
            f"🔥 GPU detected: using {pool_type.value!r} pool with {num_processes} concurrent threads"
        )

    else:
        # CPU-only: use prefork pool for parallelism and copy-on-write (COW) memory sharing
        # Each process loads model once; COW keeps ~800MB shared across processes
        # Expected memory per pod: ~1.0-1.2 GB (model 800MB + process overhead)
        pool_type = PoolType.THREADS
        num_processes = min(os.cpu_count() or 1, app_settings.CELERY_CONCURRENCY)
        logger.info(
            f"🚨 CPU-only: using {pool_type.value!r} with {num_processes} processes for parallelism "
            "and memory efficiency (COW)"
        )
    return (pool_type, num_processes)


def _resolve_environment_variables() -> dict[str, Any]:
    """Resolve environment variables for Celery worker configuration.

    Returns
    -------
    dict[str, Any]
        A dictionary of resolved environment variables.
    """
    pool_type, num_processes = _set_pooltype_and_processes()
    # Accept overrides via environment variables for dynamic runs/containers.
    CELERY_LOGLEVEL: str = os.environ.get("CELERY_LOGLEVEL", "warning")
    # `CONCURRENCY` overrides the computed `num_processes`/threads when provided
    concurrency_env: str = os.environ.get("CELERY_CONCURRENCY", default="2").strip()
    concurrency: int = (
        min(2, int(concurrency_env))
        if concurrency_env and concurrency_env.isdigit()
        else num_processes
    )
    # `QUEUES` should be a comma-separated list if provided, otherwise use configured queues
    default_queues: tuple[str, ...] = (
        app_config.queue_config.low_priority_ml,
        app_config.queue_config.medium_priority_ml,
        app_config.queue_config.high_priority_ml,
        app_config.queue_config.notifications,
        "celery",
    )
    CELERY_QUEUES: str = os.environ.get(
        "CELERY_QUEUES", ",".join(default_queues)
    ).strip()
    # Hostname can be overridden (allow %h token for celery to expand)
    CELERY_HOSTNAME_SUFFIX: str = os.environ.get(
        "CELERY_HOSTNAME_SUFFIX", f"worker_{name}@%h"
    ).strip()

    return {
        "POOL_TYPE": pool_type,
        "CONCURRENCY": concurrency,
        "CELERY_LOGLEVEL": CELERY_LOGLEVEL,
        "CELERY_QUEUES": CELERY_QUEUES,
        "CELERY_HOSTNAME_SUFFIX": CELERY_HOSTNAME_SUFFIX,
    }


def run_worker() -> None:
    """Run the Celery worker with optimized pool configuration."""

    env_vars = _resolve_environment_variables()
    pool_type: PoolType = env_vars["POOL_TYPE"]
    concurrency: int = env_vars["CONCURRENCY"]
    CELERY_LOGLEVEL: str = env_vars["CELERY_LOGLEVEL"]
    CELERY_QUEUES: str = env_vars["CELERY_QUEUES"]
    CELERY_HOSTNAME_SUFFIX: str = env_vars["CELERY_HOSTNAME_SUFFIX"]

    try:
        logger.info("Database initialized successfully.")
        log_system_info()

        # Configure worker arguments (respect environment overrides)
        argv: list[str] = [
            "worker",  # Command to start the Celery worker
            f"--loglevel={CELERY_LOGLEVEL}",
            f"--concurrency={concurrency}",  # Number of worker processes/threads
            f"--queues={CELERY_QUEUES}",
            f"--hostname={CELERY_HOSTNAME_SUFFIX}",  # Set a unique hostname for the worker
            f"--pool={pool_type.value}",  # Pool type: 'prefork' (processes) or 'threads'
            "--without-gossip",  # Disable gossip to reduce network overhead
            "--without-mingle",  # Disable mingle to speed up worker startup
            "--without-heartbeat",  # Disable heartbeat to reduce network traffic
        ]

        # Configure ONNX/BLAS threading more intelligently
        num_cores = os.cpu_count() or 1

        if pool_type == PoolType.THREADS:
            # For threads pool, limit ONNX to avoid oversubscription
            # Formula: Leave 1 core for Python threads, rest for ONNX
            num_threads = max(1, num_cores - concurrency)
            num_threads = min(num_threads, 2)  # Cap at 2 for stability
        else:
            # For prefork, each process gets fewer threads
            num_threads = max(1, num_cores // concurrency)

        os.environ["OMP_NUM_THREADS"] = str(num_threads)
        os.environ["MKL_NUM_THREADS"] = str(num_threads)
        os.environ["OPENBLAS_NUM_THREADS"] = str(num_threads)
        os.environ["VECLIB_MAXIMUM_THREADS"] = str(num_threads)

        logger.info(
            f"Starting Celery worker with pool={pool_type.value}, "
            f"concurrency={concurrency}, threads={num_threads}, queues={CELERY_QUEUES}, "
            f"loglevel={CELERY_LOGLEVEL}"
        )
        logger.info(f"Worker args: {' '.join(argv)}")

        celery_app.worker_main(argv=argv)

    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Failed to start Celery worker: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    run_worker()
