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

logger = create_logger(__name__)

artifacts_path: str = "/Users/mac/.cache/docling/models"
MAX_NUM_PAGES: int = app_config.pdf_processing_config.max_num_pages
MAX_FILE_SIZE_BYTES: int = app_config.pdf_processing_config.max_file_size_bytes
PERFORM_OCR: bool = app_config.pdf_processing_config.perform_ocr
USE_GPU: bool = app_config.pdf_processing_config.use_gpu


class PDFProcessor:
    """A class to handle PDF processing using Docling with configurable options."""

    def __init__(
        self, source: str | Path, output_dir: str | Path = "data/results"
    ) -> None:
        self.source = source
        self.output_dir = ROOT / Path(output_dir)

    def _download_easyocr_models(self) -> None:
        """Run this function once to download EasyOCR models for offline use.

        Use a VPN if GitHub release assets are blocked in your region. This will
        cache the models locally for future use.
        """
        logger.info("Downloading EasyOCR models for offline use...")
        _ = easyocr.Reader(["en"], gpu=False)

    def process_data(self, export_format: ExportFormat = ExportFormat.ALL) -> None:
        """Main function to convert a PDF document using Docling and export results."""
        logging.basicConfig(level=logging.INFO)

        # Run this once to download models
        self._download_easyocr_models()

        start_time: float = time.time()
        doc_converter = get_document_converter()

        conv_result = doc_converter.convert(
            self.source,
            max_num_pages=MAX_NUM_PAGES,
            max_file_size=MAX_FILE_SIZE_BYTES,
            raises_on_error=False,
        )

        self.output_dir.mkdir(parents=True, exist_ok=True)
        doc_filename: str = conv_result.input.file.stem

        logger.info(f"Converting results to the format: '{export_format.value}'")
        if export_format in [ExportFormat.ALL, ExportFormat.TABLE]:
            for table_ix, table in enumerate(conv_result.document.tables):
                table_df: pd.DataFrame = table.export_to_dataframe(
                    doc=conv_result.document
                )
                print(f"## Table {table_ix}")
                print(table_df.head(3).to_markdown())

                # Save the table as CSV
                element_csv_filename = (
                    self.output_dir / f"{doc_filename}-table-{table_ix + 1}.csv"
                )
                logger.info(f"Saving CSV table to {element_csv_filename}")
                table_df.to_csv(element_csv_filename)

        if export_format in [ExportFormat.ALL, ExportFormat.JSON]:
            # Export Docling document JSON format:
            with (self.output_dir / f"{doc_filename}.json").open(
                "w", encoding="utf-8"
            ) as fp:
                fp.write(json.dumps(conv_result.document.export_to_dict()))
        if export_format in [ExportFormat.ALL, ExportFormat.TEXT]:
            # Export Text format (plain text via Markdown export):
            with (self.output_dir / f"{doc_filename}.txt").open(
                "w", encoding="utf-8"
            ) as fp:
                fp.write(conv_result.document.export_to_markdown(strict_text=True))
        if export_format in [ExportFormat.ALL, ExportFormat.MARKDOWN]:
            # Export Markdown format:
            with (self.output_dir / f"{doc_filename}.md").open(
                "w", encoding="utf-8"
            ) as fp:
                fp.write(conv_result.document.export_to_markdown())

        if export_format in [ExportFormat.ALL, ExportFormat.DOCUMENT_TAGS]:
            # Export Document Tags format:
            with (self.output_dir / f"{doc_filename}.doctags").open(
                "w", encoding="utf-8"
            ) as fp:
                fp.write(conv_result.document.export_to_doctags())

        end_time = time.time() - start_time

        logger.info(
            f"Document converted and tables exported in {end_time:.2f} seconds."
        )


if __name__ == "__main__":
    source: Path = ROOT / "data/AI_roadmap.pdf"
    output_dir: Path = ROOT / "data/results"
    pdf_processor = PDFProcessor(source=source, output_dir=output_dir)
    pdf_processor.process_data(export_format=ExportFormat.ALL)
