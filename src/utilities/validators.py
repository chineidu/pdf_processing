"""Utility functions for validating operations.

inspired by:
https://betterstack.com/community/guides/scaling-python/uploading-files-using-fastapi/#building-comprehensive-file-validation-systems
"""

import io
from pathlib import Path

import magic
from fastapi import UploadFile

from src import create_logger
from src.schemas.types import DocumentValidationResult, MimeTypeEnum

logger = create_logger(__name__)


class DocumentValidator:
    # 2KB is sufficient for libmagic to identify most file headers
    MAGIC_HEADER_BYTES = 2048

    # Map extensions to valid MIME types
    ALLOWED_MIME_TYPES: dict[str, set[MimeTypeEnum]] = {
        ".pdf": {MimeTypeEnum.PDF},
        ".txt": {MimeTypeEnum.TEXT},
        # JSON is often detected as text/plain by libmagic since it is UTF-8 text
        ".json": {MimeTypeEnum.JSON, MimeTypeEnum.TEXT},
    }

    def __init__(self, max_size: int = 10 * 1024 * 1024) -> None:
        """Initialize the DocumentValidator.

        Parameters
        ----------
        max_size : int, optional
            Maximum allowed file size in bytes, by default 10MB.
        """
        self.max_size = max_size
        self.allowed_extensions: set[str] = set(self.ALLOWED_MIME_TYPES.keys())

    async def validate_file(self, file: UploadFile) -> DocumentValidationResult:
        """Validate a file before uploading.

        Performs validation without loading the full file into memory:
        - Filename safety (presence, path traversal, null bytes)
        - Extension allowlist
        - File size via stream seeking
        - MIME type via magic number header sampling
        """
        result: DocumentValidationResult = {"valid": True, "errors": []}

        # Filename Presence Check
        if not file.filename or file.filename.strip() == "":
            result["valid"] = False
            result["errors"].append("No file selected.")
            return result

        # Null Byte Injection Guard
        # Attackers use \x00 to attempt to truncate filenames in C-based system calls.
        # While Python handles the string correctly, the underlying OS might truncate
        # "report.pdf\x00.exe" to just "report.pdf", bypassing OS-level filters.
        if "\x00" in file.filename:
            # Use repr() to log the raw string with the null byte visible, preventing log corruption
            logger.warning(
                f"Security alert: Null byte detected in filename: {repr(file.filename)}"
            )
            result["valid"] = False
            result["errors"].append("Invalid filename detected.")
            return result

        # Path Traversal Guard
        safe_filename = Path(file.filename).name
        if safe_filename != file.filename:
            # Use repr() to safely log potential control characters or newlines
            logger.warning(f"Path traversal attempt detected: {repr(file.filename)}")
            result["valid"] = False
            result["errors"].append("Invalid filename structure.")
            return result

        # Extension Allowlist
        file_ext = Path(safe_filename).suffix.lower()
        if file_ext not in self.allowed_extensions:
            result["valid"] = False
            result["errors"].append(
                f"File type '{file_ext}' is not allowed. "
                f"Accepted types: {', '.join(sorted(self.allowed_extensions))}"
            )
            return result

        # Stream-Based Size Check (No Memory Load)
        try:
            await file.seek(0, io.SEEK_END)  # type:ignore
            file_size = await file.tell()  # type:ignore            await file.seek(0)
        except Exception as e:
            # Log the specific error internally, but return a generic error to the user
            logger.error(f"Failed to seek file stream for {repr(safe_filename)}: {e}")
            result["valid"] = False
            result["errors"].append("Could not read file metadata.")
            return result

        if file_size == 0:
            result["valid"] = False
            result["errors"].append("File is empty.")
            return result

        if file_size > self.max_size:
            max_mb = self.max_size / (1024 * 1024)
            actual_mb = file_size / (1024 * 1024)
            result["valid"] = False
            result["errors"].append(
                f"File is too large ({actual_mb:.2f} MB). Maximum allowed size is {max_mb:.0f} MB."
            )
            return result

        # Magic Number MIME Validation
        try:
            header = await file.read(self.MAGIC_HEADER_BYTES)
            await file.seek(0)
        except Exception as e:
            logger.error(f"Failed to read file header for {repr(safe_filename)}: {e}")
            result["valid"] = False
            result["errors"].append("Could not verify file content.")
            return result

        detected_mime = magic.from_buffer(header, mime=True)
        allowed_mimes: set[str] = {
            mime.value for mime in self.ALLOWED_MIME_TYPES.get(file_ext, set())
        }

        if detected_mime not in allowed_mimes:
            # Log the detailed mismatch for debugging/auditing
            logger.warning(
                f"MIME mismatch for {repr(safe_filename)}: detected '{detected_mime}', "
                f"expected one of {allowed_mimes}"
            )
            # Return a GENERIC error to the user to avoid leaking detection logic
            result["valid"] = False
            result["errors"].append(
                f"File content does not match the '{file_ext}' extension."
            )
            return result

        return result
