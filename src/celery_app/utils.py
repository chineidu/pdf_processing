from docling.document_converter import DocumentConverter


def get_document_converter() -> DocumentConverter:
    """Retrieve the worker-scoped DocumentConverter instance."""
    from src.celery_app.signals import _document_converter

    if _document_converter is None:
        raise RuntimeError(
            "DocumentConverter not initialized. Ensure worker_ready signal fired."
        )
    return _document_converter
