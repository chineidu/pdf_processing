import os
from typing import TYPE_CHECKING, Any

import torch
from celery.signals import (
    task_failure,
    task_postrun,
    task_prerun,
    worker_init,
    worker_ready,
    worker_shutdown,
)

from src import create_logger
from src.config import app_config, app_settings

if TYPE_CHECKING:
    from docling.document_converter import DocumentConverter

logger = create_logger(name=__name__)


# Global reference
_document_converter: "DocumentConverter | None" = None


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


@worker_ready.connect
def on_worker_ready(sender: Any | None = None, **kwargs: Any) -> None:  # noqa: ARG001
    """Callback function triggered when a Celery worker is ready to accept tasks."""
    logger.info(f"Celery worker {os.getpid()} is ready to accept tasks.")

    global _document_converter

    try:
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

        PERFORM_OCR: bool = app_config.pdf_processing_config.perform_ocr
        USE_GPU: bool = app_config.pdf_processing_config.use_gpu

        _setup_environment()

        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = PERFORM_OCR
        pipeline_options.ocr_options = EasyOcrOptions()
        pipeline_options.ocr_options.use_gpu = USE_GPU
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
    """Log when task starts"""
    analysis_id = kwargs.get("analysis_id", "unknown")
    logger.info(f"Task {task.name} started: {task_id} (analysis_id: {analysis_id})")


@task_postrun.connect
def task_postrun_handler(
    task_id: str,
    task: Any,
    args: tuple[Any, ...],  # noqa: ARG001
    kwargs: dict[str, Any],
    retval: Any,  # noqa: ARG001
    **extra: Any,  # noqa: ARG001
) -> None:
    """Log when task completes"""
    analysis_id = kwargs.get("analysis_id", "unknown")
    logger.info(f"Task {task.name} finished: {task_id} (analysis_id: {analysis_id})")


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
    """Log when task fails"""
    analysis_id = kwargs.get("analysis_id", "unknown")
    logger.error(
        f"Task failed: {task_id} (analysis_id: {analysis_id}), exception: {exception}"
    )
