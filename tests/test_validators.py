"""Tests for src/utilities/validators.py — DocumentValidator."""

from unittest.mock import MagicMock, patch

import pytest

from src.utilities.validators import DocumentValidator


def make_upload_file(
    filename: str,
    content: bytes = b"",
    size: int | None = None,
) -> MagicMock:
    """Build a minimal UploadFile-like mock."""
    f = MagicMock()
    f.filename = filename

    actual_size = size if size is not None else len(content)
    _pos = [0]  # mutable so the closures below can update it

    async def seek(offset: int, whence: int = 0) -> None:
        if whence == 0:
            _pos[0] = offset
        elif whence == 2:  # SEEK_END
            _pos[0] = actual_size

    async def tell() -> int:
        return _pos[0]

    async def read(n: int = -1) -> bytes:
        if n == -1:
            return content
        return content[:n]

    f.seek = seek
    f.tell = tell
    f.read = read
    return f


class TestDocumentValidatorInit:
    def test_default_max_size_is_10mb(self):
        v = DocumentValidator()
        assert v.max_size == 10 * 1024 * 1024

    def test_custom_max_size(self):
        v = DocumentValidator(max_size=5 * 1024 * 1024)
        assert v.max_size == 5 * 1024 * 1024

    def test_allowed_extensions_contains_pdf_txt_json(self):
        v = DocumentValidator()
        assert ".pdf" in v.allowed_extensions
        assert ".txt" in v.allowed_extensions
        assert ".json" in v.allowed_extensions


class TestDocumentValidatorValidateFile:
    @pytest.mark.asyncio
    async def test_empty_filename_is_invalid(self):
        v = DocumentValidator()
        f = make_upload_file(filename="")
        result = await v.validate_file(f)
        assert result["valid"] is False
        assert any("No file selected" in e for e in result["errors"])

    @pytest.mark.asyncio
    async def test_whitespace_only_filename_is_invalid(self):
        v = DocumentValidator()
        f = make_upload_file(filename="   ")
        result = await v.validate_file(f)
        assert result["valid"] is False

    @pytest.mark.asyncio
    async def test_null_byte_in_filename_is_invalid(self):
        v = DocumentValidator()
        f = make_upload_file(filename="report.pdf\x00.exe")
        result = await v.validate_file(f)
        assert result["valid"] is False
        assert any("Invalid filename" in e for e in result["errors"])

    @pytest.mark.asyncio
    async def test_path_traversal_is_invalid(self):
        v = DocumentValidator()
        f = make_upload_file(filename="../etc/passwd.pdf")
        result = await v.validate_file(f)
        assert result["valid"] is False
        assert any("Invalid filename structure" in e for e in result["errors"])

    @pytest.mark.asyncio
    async def test_disallowed_extension_is_invalid(self):
        v = DocumentValidator()
        f = make_upload_file(filename="malware.exe")
        result = await v.validate_file(f)
        assert result["valid"] is False
        assert any("not allowed" in e for e in result["errors"])

    @pytest.mark.asyncio
    async def test_empty_file_content_is_invalid(self):
        v = DocumentValidator()
        f = make_upload_file(filename="empty.pdf", content=b"", size=0)
        result = await v.validate_file(f)
        assert result["valid"] is False
        assert any("empty" in e for e in result["errors"])

    @pytest.mark.asyncio
    async def test_file_exceeding_max_size_is_invalid(self):
        v = DocumentValidator(max_size=100)
        f = make_upload_file(filename="big.pdf", content=b"x" * 10, size=200)
        result = await v.validate_file(f)
        assert result["valid"] is False
        assert any("too large" in e for e in result["errors"])

    @pytest.mark.asyncio
    async def test_valid_pdf_file_passes(self):
        v = DocumentValidator(max_size=10 * 1024 * 1024)
        # Minimal PDF magic bytes
        pdf_content = b"%PDF-1.4 fake content for header detection"
        f = make_upload_file(
            filename="document.pdf",
            content=pdf_content,
            size=len(pdf_content),
        )
        with patch("magic.from_buffer", return_value="application/pdf"):
            result = await v.validate_file(f)
        assert result["valid"] is True
        assert result["errors"] == []

    @pytest.mark.asyncio
    async def test_mime_type_mismatch_is_invalid(self):
        v = DocumentValidator(max_size=10 * 1024 * 1024)
        content = b"GIF89a fake gif data padded out"
        f = make_upload_file(
            filename="sneaky.pdf",
            content=content,
            size=len(content),
        )
        with patch("magic.from_buffer", return_value="image/gif"):
            result = await v.validate_file(f)
        assert result["valid"] is False
        assert any("does not match" in e for e in result["errors"])

    @pytest.mark.asyncio
    async def test_valid_txt_file_passes(self):
        v = DocumentValidator(max_size=10 * 1024 * 1024)
        content = b"Hello, world! This is a plain text file."
        f = make_upload_file(filename="notes.txt", content=content, size=len(content))
        with patch("magic.from_buffer", return_value="text/plain"):
            result = await v.validate_file(f)
        assert result["valid"] is True

    @pytest.mark.asyncio
    async def test_json_file_accepts_text_plain_mime(self):
        """JSON files may be detected as text/plain by libmagic."""
        v = DocumentValidator(max_size=10 * 1024 * 1024)
        content = b'{"key": "value"}'
        f = make_upload_file(filename="data.json", content=content, size=len(content))
        with patch("magic.from_buffer", return_value="text/plain"):
            result = await v.validate_file(f)
        assert result["valid"] is True
