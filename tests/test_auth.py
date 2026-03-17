"""Tests for src/api/core/auth.py — pure utility functions."""

from datetime import timedelta

import pytest
from jose import jwt

from src.api.core.auth import (
    create_access_token,
    generate_api_key,
    get_password_hash,
    hash_api_key,
    verify_api_key,
    verify_password,
)
from src.config import app_settings

ALGORITHM = app_settings.ALGORITHM
SECRET_KEY = app_settings.SECRET_KEY.get_secret_value()


class TestGenerateApiKey:
    def test_returns_tuple_of_prefix_and_full_key(self):
        prefix, full_key = generate_api_key(prefix_len=4, total_len=32)
        assert isinstance(prefix, str)
        assert isinstance(full_key, str)

    def test_full_key_starts_with_prefix(self):
        _prefix, full_key = generate_api_key(prefix_len=4, total_len=32)
        # prefix is custom_prefix + random chars, full_key = prefix + body
        assert full_key.startswith(_prefix)

    def test_full_key_has_correct_total_length(self):
        custom_prefix = "test_"
        _, full_key = generate_api_key(
            prefix_len=4, total_len=20, custom_prefix=custom_prefix
        )
        # full_key = custom_prefix (5) + 4 prefix chars + 16 body chars = 25 chars
        # Actually total_len is the random chars portion: prefix_len + body_len = total_len
        # custom_prefix len + total_len = total length
        assert len(full_key) == len(custom_prefix) + 20

    def test_prefix_len_equal_total_len_raises(self):
        with pytest.raises(ValueError):
            generate_api_key(prefix_len=10, total_len=10)

    def test_prefix_len_greater_than_total_len_raises(self):
        with pytest.raises(ValueError):
            generate_api_key(prefix_len=15, total_len=10)

    def test_generated_keys_are_unique(self):
        keys = {generate_api_key(prefix_len=4, total_len=32)[1] for _ in range(20)}
        assert len(keys) == 20

    def test_default_custom_prefix_from_settings(self):
        _, full_key = generate_api_key(prefix_len=4, total_len=16)
        assert full_key.startswith(app_settings.API_KEY_PREFIX)


class TestPasswordHashing:
    def test_hash_is_not_plaintext(self):
        hashed = get_password_hash("mysecretpass")
        assert hashed != "mysecretpass"

    def test_verify_correct_password_returns_true(self):
        password = "correct_password_1!"
        hashed = get_password_hash(password)
        assert verify_password(password, hashed) is True

    def test_verify_wrong_password_returns_false(self):
        hashed = get_password_hash("correct_password_1!")
        assert verify_password("wrong_password", hashed) is False

    def test_different_passwords_produce_different_hashes(self):
        hash1 = get_password_hash("password_one_1!")
        hash2 = get_password_hash("password_two_2!")
        assert hash1 != hash2


class TestApiKeyHashing:
    def test_hash_api_key_is_deterministic(self):
        api_token = "fixture-token-001"
        assert hash_api_key(api_token) == hash_api_key(api_token)

    def test_hash_api_key_produces_64_char_hex(self):
        result = hash_api_key("some_api_key")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_verify_api_key_correct_key_returns_true(self):
        api_token = "fixture-token-123"
        stored_hash = hash_api_key(api_token)
        assert verify_api_key(api_token, stored_hash) is True

    def test_verify_api_key_wrong_key_returns_false(self):
        api_token = "fixture-token-123"
        stored_hash = hash_api_key(api_token)
        assert verify_api_key("fixture-token-999", stored_hash) is False


class TestCreateAccessToken:
    def test_token_is_decodeable(self):
        token = create_access_token({"sub": "testuser"})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        assert payload["sub"] == "testuser"

    def test_token_contains_expiry(self):
        token = create_access_token({"sub": "testuser"})
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        assert "exp" in payload

    def test_custom_expiry_delta(self):
        from calendar import timegm
        from datetime import datetime

        delta = timedelta(hours=2)
        # jose encodes naive datetimes via timegm(dt.utctimetuple()), so
        # compute the expected exp the same way to stay timezone-agnostic.
        before = timegm(datetime.now().utctimetuple())
        token = create_access_token({"sub": "testuser"}, expires_delta=delta)
        after = timegm(datetime.now().utctimetuple())
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        # exp must fall within [before + delta, after + delta] (within a 2-s window)
        assert (
            before + int(delta.total_seconds())
            <= payload["exp"]
            <= after + int(delta.total_seconds()) + 2
        )

    def test_invalid_secret_raises_on_decode(self):
        from jose import JWTError

        token = create_access_token({"sub": "testuser"})
        with pytest.raises(JWTError):
            jwt.decode(token, "wrong_secret", algorithms=[ALGORITHM])
