import io
import json
import logging
import time
from pathlib import Path

import easyocr
import pandas as pd

from src import ROOT, create_logger
from src.celery_app.utils import get_document_converter
from src.config import app_config

# from src.config.settings import _setup_environment
from src.schemas.types import ExportFormat
from src.services.storage import S3StorageService

logger = create_logger(__name__)

artifacts_path: str = "/Users/mac/.cache/docling/models"
MAX_NUM_PAGES: int = app_config.pdf_processing_config.max_num_pages
MAX_FILE_SIZE_BYTES: int = app_config.pdf_processing_config.max_file_size_bytes
PERFORM_OCR: bool = app_config.pdf_processing_config.perform_ocr
USE_GPU: bool = app_config.pdf_processing_config.use_gpu


class PDFProcessor:
    """A class to handle PDF processing using Docling with configurable options."""

    def __init__(self) -> None:
        pass

    def _download_easyocr_models(self) -> None:
        """Run this function once to download EasyOCR models for offline use.

        Use a VPN if GitHub release assets are blocked in your region. This will
        cache the models locally for future use.
        """
        logger.info("Downloading EasyOCR models for offline use...")
        _ = easyocr.Reader(["en"], gpu=False)

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
        logging.basicConfig(level=logging.INFO)

        # Run this once to download models
        self._download_easyocr_models()

        start_time: float = time.time()
        doc_converter = get_document_converter()

        conv_result = doc_converter.convert(
            source,
            max_num_pages=MAX_NUM_PAGES,
            max_file_size=MAX_FILE_SIZE_BYTES,
            raises_on_error=False,
        )

        # self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Converting results to the format: '{export_format.value}'")

        # Track all uploaded URLs for returning to user
        uploaded_urls: dict[str, str] = {}

        # ----- Table Export -----
        if export_format in [ExportFormat.ALL, ExportFormat.TABLE]:
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
        if export_format in [ExportFormat.ALL, ExportFormat.JSON]:
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
            text_bytes = conv_result.document.export_to_markdown(
                strict_text=True
            ).encode("utf-8")

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
            md_bytes = conv_result.document.export_to_markdown().encode("utf-8")

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
        if export_format in [ExportFormat.ALL, ExportFormat.DOCUMENT_TAGS]:
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


if __name__ == "__main__":
    source: Path = ROOT / "data/AI_roadmap.pdf"
    output_dir: Path = ROOT / "data/results"
    # pdf_processor = PDFProcessor(output_dir=output_dir)
    # pdf_processor.aprocess_data_and_upload(source=source,
    # s3_service=S3StorageService(), export_format=ExportFormat.ALL)
