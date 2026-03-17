"""Tests for src/schemas/ — Pydantic models and validators."""

import pytest
from pydantic import SecretStr, ValidationError

from src.schemas.db.models import (
    BaseUserSchema,
    GuestUserSchema,
    UserCreateSchema,
)
from src.schemas.routes.routes import (
    PresignedURLResponse,
    TaskStatusResponse,
    TaskUploadConfirmRequest,
)
from src.schemas.types import (
    MimeTypeEnum,
    RoleTypeEnum,
    StatusTypeEnum,
    TierEnum,
)


class TestBaseUserSchema:
    def test_valid_user(self):
        user = BaseUserSchema(name="alice", email="alice@example.com")
        assert user.name == "alice"
        assert user.email == "alice@example.com"

    def test_default_tier_is_free(self):
        user = BaseUserSchema(name="bob", email="bob@example.com")
        assert user.tier == TierEnum.FREE

    def test_default_credits_is_zero(self):
        user = BaseUserSchema(name="bob", email="bob@example.com")
        assert user.credits == 0.0

    def test_default_is_active_true(self):
        user = BaseUserSchema(name="bob", email="bob@example.com")
        assert user.is_active is True

    def test_invalid_email_raises_validation_error(self):
        with pytest.raises(ValidationError):
            BaseUserSchema(name="bad", email="not-an-email")

    def test_credits_above_max_raises_validation_error(self):
        with pytest.raises(ValidationError):
            BaseUserSchema(name="rich", email="rich@example.com", credits=2_000_000.0)

    def test_credits_below_min_raises_validation_error(self):
        with pytest.raises(ValidationError):
            BaseUserSchema(name="broke", email="broke@example.com", credits=-1.0)

    def test_roles_default_is_empty_list(self):
        user = BaseUserSchema(name="eve", email="eve@example.com")
        assert user.roles == []

    def test_convert_roles_from_string(self):
        user = BaseUserSchema.model_validate(
            {"name": "admin", "email": "a@example.com", "roles": ["admin"]}
        )
        assert user.roles == [RoleTypeEnum.ADMIN]

    def test_convert_roles_from_object_with_name_attr(self):
        class FakeRole:
            name = "user"

        user = BaseUserSchema.model_validate(
            {"name": "u", "email": "u@example.com", "roles": [FakeRole()]}
        )
        assert user.roles == [RoleTypeEnum.USER]


class TestGuestUserSchema:
    def test_default_tier_is_guest(self):
        guest = GuestUserSchema()
        assert guest.tier == TierEnum.GUEST

    def test_default_name_is_guest(self):
        guest = GuestUserSchema()
        assert guest.name == "guest"

    def test_default_external_id_is_guest(self):
        guest = GuestUserSchema()
        assert guest.external_id == "guest"

    def test_default_id_is_none(self):
        guest = GuestUserSchema()
        assert guest.id is None

    def test_default_credits_is_zero(self):
        guest = GuestUserSchema()
        assert guest.credits == 0.0


class TestUserCreateSchema:
    def test_valid_user_with_password(self):
        user = UserCreateSchema(
            name="carol",
            email="carol@example.com",
            password=SecretStr("securepass123"),
        )
        assert user.name == "carol"

    def test_password_too_short_raises_validation_error(self):
        with pytest.raises(ValidationError):
            UserCreateSchema(
                name="short",
                email="short@example.com",
                password=SecretStr("abc"),
            )

    def test_password_too_long_raises_validation_error(self):
        with pytest.raises(ValidationError):
            UserCreateSchema(
                name="long",
                email="long@example.com",
                password=SecretStr("a" * 21),
            )

    def test_password_is_secret_str(self):
        user = UserCreateSchema(
            name="carol",
            email="carol@example.com",
            password=SecretStr("securepass123"),
        )
        # SecretStr should not expose the value in str()
        assert "securepass123" not in str(user)


class TestTaskUploadConfirmRequest:
    def test_valid_payload(self):
        payload = TaskUploadConfirmRequest(file_size_bytes=1024)
        assert payload.file_size_bytes == 1024

    def test_optional_fields_default_to_none(self):
        payload = TaskUploadConfirmRequest(file_size_bytes=512)
        assert payload.file_type is None
        assert payload.file_page_count is None
        assert payload.etag is None

    def test_with_all_fields(self):
        payload = TaskUploadConfirmRequest(
            file_size_bytes=2048,
            file_type=MimeTypeEnum.PDF,
            file_page_count=10,
            etag="abc123",
        )
        assert payload.file_type == MimeTypeEnum.PDF
        assert payload.file_page_count == 10
        assert payload.etag == "abc123"


class TestPresignedURLResponse:
    def test_valid_response(self):
        resp = PresignedURLResponse(
            task_id="task-001",
            url="https://bucket.s3.example.com/object?X-Amz-Signature=sig",
            expires_at="2026-03-17T10:00:00Z",
            content_type=MimeTypeEnum.PDF,
        )
        assert resp.task_id == "task-001"
        assert resp.content_type == MimeTypeEnum.PDF

    def test_upload_headers_default_to_none(self):
        resp = PresignedURLResponse(
            task_id="task-002",
            url="https://example.com/upload",
            expires_at="2026-03-17T10:00:00Z",
            content_type=None,
        )
        assert resp.upload_headers is None


class TestTaskStatusResponse:
    def test_valid_response(self):
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        resp = TaskStatusResponse(
            task_id="task-xyz",
            status=StatusTypeEnum.PROCESSING,
            file_type=MimeTypeEnum.PDF,
            file_size_bytes=4096,
            created_at=now,
            updated_at=now,
            completed_at=None,
            error_message=None,
            download_urls=None,
        )
        assert resp.status == StatusTypeEnum.PROCESSING
        assert resp.task_id == "task-xyz"

    def test_completed_with_download_urls(self):
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        resp = TaskStatusResponse(
            task_id="task-done",
            status=StatusTypeEnum.COMPLETED,
            file_type=MimeTypeEnum.PDF,
            file_size_bytes=8192,
            created_at=now,
            updated_at=now,
            completed_at=now,
            error_message=None,
            download_urls={"markdown": "https://example.com/result.md"},
        )
        assert resp.download_urls == {"markdown": "https://example.com/result.md"}

    def test_failed_with_error_message(self):
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        resp = TaskStatusResponse(
            task_id="task-fail",
            status=StatusTypeEnum.FAILED,
            file_type=None,
            file_size_bytes=0,
            created_at=now,
            updated_at=now,
            completed_at=None,
            error_message="Processing timed out",
            download_urls=None,
        )
        assert resp.error_message == "Processing timed out"
