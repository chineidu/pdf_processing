"""Tests for src/utilities/utils.py"""

from pathlib import Path

import pytest

from src.schemas.types import TierEnum
from src.utilities.utils import (
    calculate_latency_ewma,
    extract_rate_limit_number,
    generate_idempotency_key,
    get_ratelimit_value,
    sort_dict,
    validate_pdf_file,
)


class TestSortDict:
    def test_sorts_top_level_keys(self):
        result = sort_dict({"z": 1, "a": 2, "m": 3})
        assert list(result.keys()) == ["a", "m", "z"]

    def test_sorts_nested_keys_recursively(self):
        result = sort_dict({"b": {"z": 1, "a": 2}, "a": {"y": 3, "x": 4}})
        assert list(result.keys()) == ["a", "b"]
        assert list(result["a"].keys()) == ["x", "y"]
        assert list(result["b"].keys()) == ["a", "z"]

    def test_non_dict_value_returned_as_is(self):
        assert sort_dict("string") == "string"  # type: ignore[arg-type]
        assert sort_dict(42) == 42  # type: ignore[arg-type]
        assert sort_dict([3, 1, 2]) == [3, 1, 2]  # type: ignore[arg-type]

    def test_empty_dict_returns_empty_dict(self):
        assert sort_dict({}) == {}

    def test_dict_with_mixed_value_types(self):
        result = sort_dict({"z": [1, 2, 3], "a": None, "m": {"b": 1, "a": 2}})
        assert list(result.keys()) == ["a", "m", "z"]
        assert result["z"] == [1, 2, 3]


class TestGenerateIdempotencyKey:
    def test_same_payload_produces_same_key(self):
        payload = {"file": "test.pdf", "page_count": 5}
        key1 = generate_idempotency_key(payload)
        key2 = generate_idempotency_key(payload)
        assert key1 == key2

    def test_different_payloads_produce_different_keys(self):
        key1 = generate_idempotency_key({"a": 1})
        key2 = generate_idempotency_key({"a": 2})
        assert key1 != key2

    def test_user_id_changes_key(self):
        payload = {"file": "doc.pdf"}
        key_no_user = generate_idempotency_key(payload)
        key_with_user = generate_idempotency_key(payload, user_id="user-123")
        assert key_no_user != key_with_user

    def test_key_is_64_char_hex_string(self):
        key = generate_idempotency_key({"x": 1})
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)

    def test_key_is_order_independent(self):
        """Payload key order should not affect the idempotency key."""
        key1 = generate_idempotency_key({"b": 2, "a": 1})
        key2 = generate_idempotency_key({"a": 1, "b": 2})
        assert key1 == key2


class TestCalculateLatencyEwma:
    def test_no_old_latency_returns_current(self):
        result = calculate_latency_ewma(None, 100.0)
        assert result == 100.0

    def test_ewma_weights_towards_new_value(self):
        result = calculate_latency_ewma(100.0, 200.0, alpha=0.5)
        assert result == 150.0

    def test_default_alpha_0_2(self):
        result = calculate_latency_ewma(100.0, 200.0)
        expected = round(0.2 * 200.0 + 0.8 * 100.0, 2)
        assert result == expected

    def test_result_is_rounded_to_2_decimal_places(self):
        result = calculate_latency_ewma(33.333, 66.666)
        assert result == round(result, 2)

    def test_zero_old_latency_treated_as_none(self):
        result = calculate_latency_ewma(0.0, 50.0)
        assert result == 50.0


class TestGetRatelimitValue:
    def test_guest_tier(self):
        assert get_ratelimit_value(TierEnum.GUEST) == "10/minute"

    def test_free_tier(self):
        assert get_ratelimit_value(TierEnum.FREE) == "20/minute"

    def test_plus_tier(self):
        assert get_ratelimit_value(TierEnum.PLUS) == "60/minute"

    def test_pro_tier(self):
        assert get_ratelimit_value(TierEnum.PRO) == "120/minute"


class TestExtractRateLimitNumber:
    def test_guest_returns_10(self):
        assert extract_rate_limit_number(TierEnum.GUEST) == 10

    def test_free_returns_20(self):
        assert extract_rate_limit_number(TierEnum.FREE) == 20

    def test_plus_returns_60(self):
        assert extract_rate_limit_number(TierEnum.PLUS) == 60

    def test_pro_returns_120(self):
        assert extract_rate_limit_number(TierEnum.PRO) == 120

    def test_default_fallback(self):
        assert extract_rate_limit_number(TierEnum.GUEST, default=99) == 10


class TestValidatePdfFile:
    def test_nonexistent_file_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            validate_pdf_file("/nonexistent/path/file.pdf")

    def test_empty_file_raises_value_error(self, tmp_path: Path):
        empty = tmp_path / "empty.pdf"
        empty.write_bytes(b"")
        with pytest.raises(ValueError, match="empty"):
            validate_pdf_file(empty)

    def test_file_too_small_raises_value_error(self, tmp_path: Path):
        small = tmp_path / "small.pdf"
        small.write_bytes(b"%PDF-1.4 short")
        with pytest.raises(ValueError, match="too small"):
            validate_pdf_file(small)

    def test_missing_pdf_header_raises_value_error(self, tmp_path: Path):
        bad = tmp_path / "bad.pdf"
        bad.write_bytes(b"Not a PDF" + b"x" * 200)
        with pytest.raises(ValueError, match="PDF header"):
            validate_pdf_file(bad)

    def test_valid_pdf_header_passes(self, tmp_path: Path):
        valid = tmp_path / "valid.pdf"
        valid.write_bytes(b"%PDF-1.4" + b"\n" * 200)
        # Should not raise
        validate_pdf_file(valid)

    def test_accepts_path_object(self, tmp_path: Path):
        p = tmp_path / "ok.pdf"
        p.write_bytes(b"%PDF-1.4" + b"\n" * 200)
        validate_pdf_file(p)

    def test_accepts_string_path(self, tmp_path: Path):
        p = tmp_path / "ok.pdf"
        p.write_bytes(b"%PDF-1.4" + b"\n" * 200)
        validate_pdf_file(str(p))
