import io
import json
import tempfile
import time
from pathlib import Path
from typing import Any

import easyocr
import fitz  # PyMuPDF
import pandas as pd

from src import create_logger
from src.celery_app.utils import get_document_converter
from src.config import app_config
from src.schemas.types import ExportFormat
from src.services.storage import S3StorageService

logger = create_logger(__name__)

MAX_NUM_PAGES: int = app_config.pdf_processing_config.max_num_pages
MAX_FILE_SIZE_BYTES: int = app_config.pdf_processing_config.max_file_size_bytes
PERFORM_OCR: bool = app_config.pdf_processing_config.perform_ocr
USE_GPU: bool = app_config.pdf_processing_config.use_gpu


class PDFProcessor:
    """A class to handle PDF processing using Docling with configurable options."""

    def __init__(self) -> None:
        self._models_downloaded = False

    def _validate_pdf_file(self, filepath: str | Path) -> None:
        """Validate that the PDF file exists and is readable.

        Parameters
        ----------
        filepath : str | Path
            Path to the PDF file to validate.

        Raises
        ------
        FileNotFoundError
            If the file doesn't exist.
        ValueError
            If the file is empty, too small, or doesn't appear to be a valid PDF.
        """
        path = Path(filepath)

        # Check if file exists
        if not path.exists():
            raise FileNotFoundError(f"PDF file not found: {filepath}")

        # Check file size
        file_size = path.stat().st_size
        if file_size == 0:
            raise ValueError(f"PDF file is empty (0 bytes): {filepath}")

        if file_size < 100:  # PDFs should be at least 100 bytes
            raise ValueError(
                f"PDF file is too small ({file_size} bytes), likely corrupted: {filepath}"
            )

        # Check PDF header (should start with %PDF)
        try:
            with open(path, "rb") as f:
                header = f.read(5)
                if not header.startswith(b"%PDF-"):
                    raise ValueError(
                        f"File does not appear to be a valid PDF (missing PDF header): {filepath}"
                    )
        except Exception as e:
            raise ValueError(f"Cannot read PDF file {filepath}: {e}") from e

        logger.info(f"PDF validation passed: {filepath} ({file_size:,} bytes)")

    def _inspect_pdf(self, filepath: str | Path) -> dict:
        """Inspect PDF and return detailed information for debugging.

        Parameters
        ----------
        filepath : str | Path
            Path to the PDF file to inspect.

        Returns
        -------
        dict
            Dictionary containing PDF metadata and properties.
        """
        path = Path(filepath)
        try:
            doc = fitz.open(str(path))

            # Get basic info
            num_pages = doc.page_count
            metadata = doc.metadata or {}

            # Check if encrypted
            is_encrypted = doc.is_encrypted

            # Try to extract text from first page to see if it's text-based
            has_text = False
            text_length = 0
            if num_pages > 0:
                try:
                    text = doc[0].get_text()
                    has_text = bool(text and text.strip())
                    text_length = len(text.strip()) if text else 0
                except Exception:  # noqa: S110
                    pass

            info = {
                "num_pages": num_pages,
                "is_encrypted": is_encrypted,
                "has_text": has_text,
                "text_length_page1": text_length,
                "producer": metadata.get("producer", "Unknown"),
                "creator": metadata.get("creator", "Unknown"),
                "format": metadata.get("format", "Unknown"),
            }

            doc.close()

            logger.info(f"PDF Info: {info}")
            return info

        except Exception as e:
            logger.error(f"Failed to inspect PDF: {e}")
            return {"error": str(e)}

    def _fix_browser_pdf(self, filepath: str | Path) -> Path:
        """Fix browser-generated PDFs by re-saving them using PyMuPDF.

        Chrome/browser PDFs sometimes have structures that Docling rejects.
        This re-saves the PDF in a clean format that Docling can process.

        Parameters
        ----------
        filepath : str | Path
            Path to the original PDF file.

        Returns
        -------
        Path
            Path to the fixed PDF file (temp file).
        """

        path = Path(filepath)
        logger.info("Re-saving browser-generated PDF for Docling compatibility...")

        try:
            # Open the PDF with PyMuPDF
            doc = fitz.open(str(path))

            # Create a temporary file for the fixed PDF
            temp_fd, temp_path = tempfile.mkstemp(suffix=".pdf", prefix="fixed_")
            import os

            os.close(temp_fd)  # Close the file descriptor

            # Save as a clean PDF
            doc.save(temp_path, garbage=4, deflate=True, clean=True)
            doc.close()

            logger.info(f"PDF re-saved successfully to {temp_path}")
            return Path(temp_path)

        except Exception as e:
            logger.error(f"Failed to fix browser PDF: {e}")
            raise

    def _extract_with_pymupdf(self, filepath: str | Path) -> dict[str, str]:
        """Fallback extraction using PyMuPDF when Docling fails.

        This provides basic text extraction for browser-generated PDFs that
        Docling cannot process.

        Parameters
        ----------
        filepath : str | Path
            Path to the PDF file.

        Returns
        -------
        dict[str, str]
            Dictionary with extracted content in markdown format.
        """
        logger.info("Using PyMuPDF fallback extraction...")
        path = Path(filepath)

        try:
            doc = fitz.open(str(path))
            full_text: list[str] = []
            page_count = doc.page_count

            for page_num in range(page_count):
                page = doc[page_num]

                # Extract text
                text = page.get_text()

                # Add page header
                full_text.append(f"\n## Page {page_num + 1}\n")
                full_text.append(text)

            doc.close()

            markdown_content = "\n".join(full_text)
            logger.info(
                f"✓ PyMuPDF extraction complete: {len(markdown_content)} characters from {page_count} pages"
            )

            return {"markdown": markdown_content}

        except Exception as e:
            logger.error(f"PyMuPDF fallback extraction failed: {e}", exc_info=True)
            raise

    def _download_easyocr_models(self) -> None:
        """Run this function once to download EasyOCR models for offline use.

        Use a VPN if GitHub release assets are blocked in your region. This will
        cache the models locally for future use.
        """
        if self._models_downloaded:
            return

        logger.info(f"Downloading EasyOCR models for offline use (gpu={USE_GPU})...")
        try:
            # Download models with the same GPU setting as the pipeline
            _ = easyocr.Reader(["en"], gpu=USE_GPU)
            self._models_downloaded = True
            logger.info("EasyOCR models downloaded successfully")

        except Exception as e:
            logger.error(f"Failed to download EasyOCR models: {e}", exc_info=True)
            raise

    async def _aupload_bytes(
        self,
        s3_service: S3StorageService,
        data: bytes,
        *,
        task_id: str,
        correlation_id: str,
        file_extension: str = ".json",
    ) -> tuple[str | None, str | None]:
        """Upload bytes to S3 and return both the etag and the S3 URL.

        Returns
        -------
        tuple[str | None, str | None]
            Tuple of (etag, s3_url) if successful, (None, None) if failed.
        """
        buffer = io.BytesIO(data)
        etag = await s3_service.aupload_fileobj_to_s3(
            fileobj=buffer,
            task_id=task_id,
            correlation_id=correlation_id,
            file_extension=file_extension,
            operation="output",
        )
        if etag:
            s3_url = s3_service.get_s3_object_url(
                task_id=task_id,
                file_extension=file_extension,
                operation="output",
            )
            return etag, s3_url
        return None, None

    async def aprocess_data_and_upload(
        self,
        source: str | Path,
        s3_service: S3StorageService,
        task_id: str = "unknown",
        export_format: ExportFormat = ExportFormat.ALL,
    ) -> dict[str, str]:
        """Main function to convert a PDF document using Docling and export results and upload them to S3.

        Returns
        -------
        dict[str, str]
            Dictionary mapping export format names to their S3 URLs for download.
            Example: {'markdown': 's3://bucket/uploads/task-id/output/task-id.md'}
        """

        # Validate PDF file before processing
        self._validate_pdf_file(source)

        # Inspect PDF for debugging
        pdf_info: dict[str, Any] = self._inspect_pdf(source)

        # Store original source path before any modifications
        original_source = Path(source)

        # Check if this is a browser-generated PDF that needs fixing
        is_browser_pdf: bool = "Skia" in pdf_info.get(
            "producer", ""
        ) or "Chrome" in pdf_info.get("creator", "")
        fixed_pdf_path = None

        if is_browser_pdf:
            logger.warning(
                "Detected browser-generated PDF - applying compatibility fix for Docling..."
            )
            try:
                fixed_pdf_path = self._fix_browser_pdf(source)
                source = fixed_pdf_path  # Use the fixed PDF for processing
            except Exception as e:
                logger.error(f"Failed to fix browser PDF, will try original: {e}")

        # Download models once
        self._download_easyocr_models()

        start_time: float = time.time()
        doc_converter = get_document_converter()

        # Try Docling first, fall back to PyMuPDF for browser PDFs
        use_fallback: bool = False
        conv_result = None

        try:
            logger.info(
                f"Converting PDF (pages={pdf_info.get('num_pages')}, "
                f"has_text={pdf_info.get('has_text')})"
            )
            conv_result = doc_converter.convert(
                source,
                max_num_pages=MAX_NUM_PAGES,
                max_file_size=MAX_FILE_SIZE_BYTES,
                raises_on_error=True,  # Raise errors instead of silently failing
            )
        except Exception as e:
            if is_browser_pdf:
                logger.warning(
                    "Docling failed on browser PDF (expected). Using PyMuPDF fallback extraction."
                )
                use_fallback = True
            else:
                logger.error(
                    f"Failed to convert document {task_id}: {e}\n"
                    f"PDF Info: {pdf_info}\n"
                    f"This may be due to: browser-generated PDF structure, "
                    "encryption, or unsupported PDF features",
                    exc_info=True,
                )
                raise
        finally:
            # Clean up temporary fixed PDF if created
            if fixed_pdf_path and fixed_pdf_path.exists():
                try:
                    fixed_pdf_path.unlink()
                    logger.info("Cleaned up temporary fixed PDF")
                except Exception:  # noqa: S110
                    pass

        # Use fallback extraction if Docling failed on browser PDF
        if use_fallback:
            # Use the original source path we stored earlier (before fixing)
            fallback_content = self._extract_with_pymupdf(original_source)
            markdown_content = fallback_content.get("markdown", "")
            logger.info(
                "Using PyMuPDF fallback - only text/markdown export available (no tables, JSON, or doctags)"
            )
        else:
            # Check if document has content
            if conv_result and conv_result.document:
                markdown_content = conv_result.document.export_to_markdown()
                if not markdown_content:
                    logger.warning(
                        f"Document {task_id} has no extractable content. "
                        f"OCR enabled: {PERFORM_OCR}, GPU enabled: {USE_GPU}"
                    )
            else:
                markdown_content = ""
                logger.warning(f"Document {task_id} conversion resulted in no document")

        logger.info(f"Converting results to the format: '{export_format.value}'")

        # Track all uploaded URLs for returning to user
        uploaded_urls: dict[str, str] = {}

        # ----- Table Export -----
        # Only available when using Docling (not fallback)
        if (
            not use_fallback
            and conv_result
            and conv_result.document
            and export_format in [ExportFormat.ALL, ExportFormat.TABLE]
        ):
            for table_ix, table in enumerate(conv_result.document.tables):
                table_df: pd.DataFrame = table.export_to_dataframe(
                    doc=conv_result.document
                )

                print(f"## Table {table_ix}")
                print(table_df.head(3).to_markdown())

                buffer = io.BytesIO()
                table_df.to_csv(buffer, index=False)

                etag, s3_url = await self._aupload_bytes(
                    s3_service,
                    buffer.getvalue(),
                    task_id=f"{task_id}-table-{table_ix + 1}",
                    correlation_id="",
                    file_extension=".csv",
                )
                if s3_url:
                    uploaded_urls[f"table_{table_ix + 1}"] = s3_url

            if etag:
                logger.info(f"Uploaded table with etag={etag}")

        # ----- JSON Export -----
        # Only available when using Docling (not fallback)
        if (
            not use_fallback
            and conv_result
            and conv_result.document
            and export_format in [ExportFormat.ALL, ExportFormat.JSON]
        ):
            json_bytes = json.dumps(conv_result.document.export_to_dict()).encode(
                "utf-8"
            )

            etag, s3_url = await self._aupload_bytes(
                s3_service,
                json_bytes,
                task_id=f"{task_id}-document-json",
                correlation_id="",
                file_extension=".json",
            )
            if s3_url:
                uploaded_urls["json"] = s3_url

            if etag:
                logger.info(f"Uploaded JSON with etag={etag}")
        # ----- Text Export -----
        if export_format in [ExportFormat.ALL, ExportFormat.TEXT]:
            # Use markdown_content which is already set (from Docling or fallback)
            if use_fallback:
                text_bytes = markdown_content.encode("utf-8")
            else:
                text_bytes = (
                    conv_result.document.export_to_markdown(strict_text=True).encode(
                        "utf-8"
                    )
                    if conv_result and conv_result.document
                    else markdown_content.encode("utf-8")
                )

            etag, s3_url = await self._aupload_bytes(
                s3_service,
                text_bytes,
                task_id=f"{task_id}-document-text",
                correlation_id="",
                file_extension=".txt",
            )
            if s3_url:
                uploaded_urls["text"] = s3_url

            if etag:
                logger.info(f"Uploaded text with etag={etag}")

        # ----- Markdown Export -----
        if export_format in [ExportFormat.ALL, ExportFormat.MARKDOWN]:
            # Use markdown_content which is already set (from Docling or fallback)
            md_bytes = markdown_content.encode("utf-8")

            etag, s3_url = await self._aupload_bytes(
                s3_service,
                md_bytes,
                task_id=f"{task_id}-document-md",
                correlation_id="",
                file_extension=".md",
            )
            if s3_url:
                uploaded_urls["markdown"] = s3_url

            if etag:
                logger.info(f"Uploaded markdown with etag={etag}")

        # ----- Document Tags Export -----
        # Only available when using Docling (not fallback)
        if (
            not use_fallback
            and conv_result
            and conv_result.document
            and export_format in [ExportFormat.ALL, ExportFormat.DOCUMENT_TAGS]
        ):
            tags_bytes = conv_result.document.export_to_doctags().encode("utf-8")

            etag, s3_url = await self._aupload_bytes(
                s3_service,
                tags_bytes,
                task_id=f"{task_id}-document-doctags",
                correlation_id="",
                file_extension=".doctags",
            )
            if s3_url:
                uploaded_urls["doctags"] = s3_url

            if etag:
                logger.info(f"Uploaded doctags with etag={etag}")

        end_time = time.time() - start_time

        logger.info(
            f"Document converted and tables exported in {end_time:.2f} seconds."
        )
        logger.info(f"Uploaded URLs: {uploaded_urls}")

        return uploaded_urls
