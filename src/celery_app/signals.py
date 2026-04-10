import os
import time
from typing import TYPE_CHECKING, Any

from celery.signals import (
    task_failure,
    task_postrun,
    task_prerun,
    worker_init,
    worker_process_init,
    worker_ready,
    worker_shutdown,
)
from prometheus_client import Counter, Histogram

from src import create_logger
from src.config import app_config, app_settings

if TYPE_CHECKING:
    from docling.document_converter import DocumentConverter

logger = create_logger(name=__name__)


# ========================================
# Celery Task Metrics (for SLO tracking)
# ========================================
CELERY_TASKS_STARTED = Counter(
    "celery_tasks_started_total",
    "Total Celery tasks started",
    labelnames=["task_name"],
)

CELERY_TASKS_COMPLETED = Counter(
    "celery_tasks_completed_total",
    "Total Celery tasks completed successfully",
    labelnames=["task_name"],
)

CELERY_TASKS_FAILED = Counter(
    "celery_tasks_failed_total",
    "Total Celery tasks failed",
    labelnames=["task_name", "exception_type"],
)

CELERY_TASK_DURATION_SECONDS = Histogram(
    "celery_task_duration_seconds",
    "Celery task duration in seconds",
    labelnames=["task_name"],
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0),
)

# Track task start times for duration calculation
_task_start_times: dict[str, float] = {}


# Global reference
_document_converter: "DocumentConverter | None" = None


def _initialize_document_converter() -> None:
    """Initialize the worker-scoped Docling converter once per process."""
    global _document_converter

    if _document_converter is not None:
        return

    from docling.datamodel.accelerator_options import (
        AcceleratorDevice,
        AcceleratorOptions,
    )
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import (
        EasyOcrOptions,
        PdfPipelineOptions,
        TableStructureOptions,
    )
    from docling.document_converter import DocumentConverter, PdfFormatOption

    from src.config.settings import _setup_environment

    perform_ocr: bool = app_config.pdf_processing_config.perform_ocr
    use_gpu: bool = app_config.pdf_processing_config.use_gpu

    _setup_environment()

    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = perform_ocr
    pipeline_options.ocr_options = EasyOcrOptions()
    pipeline_options.ocr_options.use_gpu = use_gpu
    pipeline_options.table_structure_options = TableStructureOptions(
        do_cell_matching=True
    )
    pipeline_options.accelerator_options = AcceleratorOptions(
        num_threads=4, device=AcceleratorDevice.AUTO
    )

    _document_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
        }
    )
    logger.info(f"Docling DocumentConverter initialized for worker {os.getpid()}.")


# ===========================================
# ======= Worker Lifecycle Management =======
# ===========================================
@worker_init.connect
def worker_init_handler(sender: Any | None = None, **kwargs: Any) -> None:  # noqa: ANN003, ARG001
    """Callback function triggered when a Celery worker process is initialized.

    Similar to Lifespan in FastAPI, this class ensures that the model is loaded
    and ready when the Celery worker starts, and provides access to the model
    loader for task instances.
    """
    logger.info(f"Environment: {app_settings.ENV.value}")
    logger.info(f"Celery worker {os.getpid()} is initializing (worker_init).")

    pass  # Model logic goes here if there's any.


@worker_process_init.connect
def on_worker_process_init(sender: Any | None = None, **kwargs: Any) -> None:  # noqa: ARG001
    """Initialize Docling inside each worker process after fork.

    Using worker_process_init instead of worker_ready ensures that model
    initialization happens in the child process (after fork) rather than in
    the main process (before fork). This prevents the macOS SIGABRT caused by
    fork() being called after ObjC/CoreFoundation libraries are already loaded.
    """
    try:
        _initialize_document_converter()
    except Exception as e:
        logger.error(f"Failed to initialize DocumentConverter: {e}", exc_info=True)
        raise


@worker_ready.connect
def on_worker_ready(sender: Any | None = None, **kwargs: Any) -> None:  # noqa: ARG001
    """Callback function triggered when a Celery worker is ready to accept tasks."""
    logger.info(f"Celery worker {os.getpid()} is ready to accept tasks.")

    # In non-prefork pools (e.g., threads), worker_process_init does not run per child.
    pool_name = os.environ.get("CELERY_POOL", "").strip().lower()
    if pool_name != "prefork":
        try:
            _initialize_document_converter()
        except Exception as e:
            logger.error(f"Failed to initialize DocumentConverter: {e}", exc_info=True)
            raise


@worker_shutdown.connect
def on_worker_shutdown(sender: Any | None = None, **kwargs: Any) -> None:  # noqa: ARG001
    """Event handler for the worker shutdown signal."""
    logger.info(f"Celery worker {os.getpid()} is shutting down.")

    # Clean up model resources
    _document_converter = None

    # Clean up GPU memory
    import torch  # noqa: PLC0415  # lazy import to avoid loading MPS pre-fork

    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        logger.info("GPU cache cleared on worker shutdown.")


# ===========================================
# == Celery signal handlers for monitoring ==
# ===========================================
@task_prerun.connect
def task_prerun_handler(
    task_id: str,
    task: Any,
    args: tuple[Any, ...],  # noqa: ARG001
    kwargs: dict[str, Any],
    **extra: Any,  # noqa: ARG001
) -> None:
    """Log when task starts and record start time for duration tracking"""
    analysis_id = kwargs.get("analysis_id", "unknown")
    task_name = task.name
    logger.info(f"Task {task_name} started: {task_id} (analysis_id: {analysis_id})")

    # Record task started metric
    CELERY_TASKS_STARTED.labels(task_name=task_name).inc()

    # Record start time for duration calculation
    _task_start_times[task_id] = time.time()


@task_postrun.connect
def task_postrun_handler(
    task_id: str,
    task: Any,
    args: tuple[Any, ...],  # noqa: ARG001
    kwargs: dict[str, Any],
    retval: Any,  # noqa: ARG001
    **extra: Any,  # noqa: ARG001
) -> None:
    """Log when task completes and record completion metrics"""
    analysis_id = kwargs.get("analysis_id", "unknown")
    task_name = task.name
    logger.info(f"Task {task_name} finished: {task_id} (analysis_id: {analysis_id})")

    # Record completion metric
    CELERY_TASKS_COMPLETED.labels(task_name=task_name).inc()

    # Record duration if we have the start time
    if task_id in _task_start_times:
        duration = time.time() - _task_start_times[task_id]
        CELERY_TASK_DURATION_SECONDS.labels(task_name=task_name).observe(duration)
        del _task_start_times[task_id]


@task_failure.connect
def task_failure_handler(
    task_id: str,
    exception: Exception,
    args: tuple[Any, ...],  # noqa: ARG001
    kwargs: dict[str, Any],
    traceback: Any,  # noqa: ARG001
    einfo: Any,  # noqa: ARG001
    **extra: Any,  # noqa: ARG001
) -> None:
    """Log when task fails and record failure metrics"""
    analysis_id = kwargs.get("analysis_id", "unknown")
    task_name = extra.get("task", "unknown")
    if hasattr(task_name, "name"):
        task_name = task_name.name
    exception_type = type(exception).__name__
    logger.error(
        f"Task failed: {task_id} (analysis_id: {analysis_id}), exception: {exception}"
    )

    # Record failure metric
    CELERY_TASKS_FAILED.labels(
        task_name=str(task_name),
        exception_type=exception_type,
    ).inc()

    # Record duration if we have the start time
    if task_id in _task_start_times:
        duration = time.time() - _task_start_times[task_id]
        CELERY_TASK_DURATION_SECONDS.labels(task_name=str(task_name)).observe(duration)
        del _task_start_times[task_id]
