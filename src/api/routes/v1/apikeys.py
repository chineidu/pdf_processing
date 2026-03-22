from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from src import create_logger
from src.api.core.auth import (
    generate_api_key,
    get_current_active_user,
    hash_api_key,
)
from src.api.core.exceptions import HTTPError
from src.api.core.responses import MsgSpecJSONResponse
from src.config import app_settings
from src.db.models import aget_db
from src.db.repositories.apikey_repository import APIKeyRepository
from src.schemas.db.models import APIKeySchema, UserSchema
from src.schemas.routes.apikeys import (
    APICreationSchema,
    APIResponseSchema,
    APIRotationResponseSchema,
    APIRotationSchema,
)

if TYPE_CHECKING:
    pass


logger = create_logger(name=__name__)
router = APIRouter(tags=["apikeys"], default_response_class=MsgSpecJSONResponse)


@router.post("/apikeys", status_code=status.HTTP_201_CREATED)
async def create_api_key(
    input_data: APICreationSchema,
    client: UserSchema = Depends(get_current_active_user),
    db: AsyncSession = Depends(aget_db),
) -> APIResponseSchema:
    """Create a new API key for the authenticated user.

    Parameters
    ----------
    request : Request
        The incoming HTTP request (required for rate limiting).
    client : UserSchema
        The currently authenticated user (injected by dependency).
    db : AsyncSession
        Asynchronous database session dependency used to query and persist user data
        (default is provided by dependency injection).

    Returns
    -------
    APIResponseSchema
        Schema representation of the newly created API key, including the full key value.
    """
    apikey_repo = APIKeyRepository(db=db)
    if not apikey_repo:
        raise HTTPError(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details="Failed to access API key repository",
        )

    prefix, full_key = generate_api_key()
    apikey_hash: str = hash_api_key(full_key)

    api_key_obj = APIKeySchema(
        name=input_data.name,
        requests_per_minute=None,
        expires_at=input_data.expires_at,
        user_id=client.id,
        key_prefix=app_settings.API_KEY_PREFIX,
        full_key_truncated=full_key,
        key_hash=apikey_hash,
        scopes=input_data.scopes,
    )
    key_id = await apikey_repo.acreate_apikey(api_key_obj=api_key_obj)
    if not key_id:
        raise HTTPError(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details="Failed to create API key",
        )
    return APIResponseSchema(
        id=key_id,
        name=input_data.name,
        prefix=app_settings.API_KEY_PREFIX,
        full_key=full_key,
        owner=client.external_id,
        created_at=api_key_obj.created_at,
        expires_at=input_data.expires_at,
        scopes=input_data.scopes,
    )


@router.get("/apikeys", status_code=status.HTTP_200_OK)
async def list_api_keys(
    client: UserSchema = Depends(get_current_active_user),
    db: AsyncSession = Depends(aget_db),
) -> list[APIResponseSchema]:
    """List all API keys for the authenticated user.

    Parameters
    ----------
    client : UserSchema
        The currently authenticated user (injected by dependency).
    db : AsyncSession
        Asynchronous database session dependency used to query user data
        (default is provided by dependency injection).

    Returns
    -------
    list[APIResponseSchema]
        A list of API keys owned by the authenticated user, excluding full key values.
    """
    apikey_repo = APIKeyRepository(db=db)
    if not apikey_repo:
        raise HTTPError(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details="Failed to access API key repository",
        )
    if client.id is None:
        raise HTTPError(
            status_code=status.HTTP_400_BAD_REQUEST,
            details="Authenticated user does not have a valid ID",
        )
    api_keys = await apikey_repo.aget_apikeys_by_owner(owner_id=client.id)
    return [
        APIResponseSchema(
            id=key.id,
            name=key.name,
            prefix=key.key_prefix,
            full_key=key.full_key_truncated,
            owner=client.external_id,
            created_at=key.created_at,
            expires_at=key.expires_at,
            scopes=key.scopes,  # type:ignore
        )
        for key in api_keys
    ]


@router.patch("/apikeys/{key_id}", status_code=status.HTTP_200_OK)
async def update_api_key(
    key_id: int,
    input_data: APICreationSchema,
    client: UserSchema = Depends(get_current_active_user),
    db: AsyncSession = Depends(aget_db),
) -> APIResponseSchema:
    """Update an existing API key for the authenticated user.

    Parameters
    ----------
    key_id : int
        The unique identifier of the API key to be updated.
    input_data : APICreationSchema
        The data for updating the API key, including name, expiration, and scopes.
    client : UserSchema
        The currently authenticated user (injected by dependency).
    db : AsyncSession
        Asynchronous database session dependency used to query and persist user data
        (default is provided by dependency injection).

    Returns
    -------
    APIResponseSchema
        Schema representation of the updated API key, including the full key value.
    """
    apikey_repo = APIKeyRepository(db=db)
    if not apikey_repo:
        raise HTTPError(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details="Failed to access API key repository",
        )

    existing_key = await apikey_repo.aget_apikey_by_id(key_id)
    if not existing_key or existing_key.user_id != client.id:
        raise HTTPError(
            status_code=status.HTTP_404_NOT_FOUND,
            details="API key not found or does not belong to the authenticated user",
        )
    if client.id is None:
        raise HTTPError(
            status_code=status.HTTP_400_BAD_REQUEST,
            details="Authenticated user does not have a valid ID",
        )

    update_data = {
        "name": input_data.name,
        "expires_at": input_data.expires_at,
        "scopes": input_data.scopes,
    }

    success = await apikey_repo.aupdate_apikey(
        key_id=key_id,
        user_id=client.id,
        update_data=update_data,
    )
    if not success:
        raise HTTPError(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details="Failed to update API key",
        )

    return APIResponseSchema(
        id=key_id,
        name=input_data.name,
        prefix=existing_key.key_prefix,
        full_key=existing_key.full_key_truncated,
        owner=client.external_id,
        created_at=existing_key.created_at,
        expires_at=input_data.expires_at,
        scopes=input_data.scopes,
    )


@router.post("/apikeys/{key_id}/rotate", status_code=status.HTTP_201_CREATED)
async def rotate_api_key(
    key_id: int,
    input_data: APIRotationSchema,
    client: UserSchema = Depends(get_current_active_user),
    db: AsyncSession = Depends(aget_db),
) -> APIRotationResponseSchema:
    """Rotate an API key.

    Issues a brand-new key with the same name, scopes, and rate-limit as the
    old one.  The old key is kept usable for ``grace_period_minutes`` so
    callers can migrate without downtime; set it to 0 to revoke immediately.

    The new plaintext key is returned **only once** in this response.

    Parameters
    ----------
    key_id : int
        ID of the key to rotate.
    input_data : APIRotationSchema
        ``grace_period_minutes`` and optional ``new_expires_at``.
    client : UserSchema
        Authenticated user (injected by dependency).
    db : AsyncSession
        Database session (injected by dependency).

    Returns
    -------
    APIRotationResponseSchema
        New key details including the one-time plaintext value and metadata
        about when the old key expires.
    """
    apikey_repo = APIKeyRepository(db=db)

    if client.id is None:
        raise HTTPError(
            status_code=status.HTTP_400_BAD_REQUEST,
            details="Authenticated user does not have a valid ID",
        )

    old_key = await apikey_repo.aget_apikey_by_id(key_id)
    if not old_key or old_key.user_id != client.id:
        raise HTTPError(
            status_code=status.HTTP_404_NOT_FOUND,
            details="API key not found or does not belong to the authenticated user",
        )
    if not old_key.is_active:
        raise HTTPError(
            status_code=status.HTTP_409_CONFLICT,
            details="Cannot rotate an inactive API key",
        )

    # Inherit old key's expiry unless the caller supplies a new one
    new_expires_at = input_data.new_expires_at or old_key.expires_at

    prefix, full_key = generate_api_key()
    apikey_hash: str = hash_api_key(full_key)

    new_key_obj = APIKeySchema(
        name=old_key.name,
        requests_per_minute=old_key.requests_per_minute,
        expires_at=new_expires_at,
        user_id=client.id,
        key_prefix=app_settings.API_KEY_PREFIX,
        full_key_truncated=full_key,
        key_hash=apikey_hash,
        scopes=old_key.scopes,  # type: ignore
    )

    result = await apikey_repo.arotate_apikey(
        key_id=key_id,
        user_id=client.id,
        new_key_obj=new_key_obj,
        grace_period_minutes=input_data.grace_period_minutes,
    )
    if not result:
        raise HTTPError(
            status_code=status.HTTP_404_NOT_FOUND,
            details="API key not found or does not belong to the authenticated user",
        )

    new_db_key, new_key_id = result

    now = datetime.now(timezone.utc)
    old_key_expires_at = (
        now + timedelta(minutes=input_data.grace_period_minutes)
        if input_data.grace_period_minutes > 0
        else None
    )

    return APIRotationResponseSchema(
        id=new_key_id,
        name=new_db_key.name,
        prefix=new_db_key.key_prefix,
        full_key=full_key,
        owner=client.external_id,
        created_at=new_db_key.created_at,
        expires_at=new_expires_at,
        scopes=new_db_key.scopes,  # type: ignore
        rotated_from_id=key_id,
        old_key_expires_at=old_key_expires_at,
    )


@router.delete("/apikeys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_api_key(
    key_id: int,
    client: UserSchema = Depends(get_current_active_user),
    db: AsyncSession = Depends(aget_db),
) -> None:
    """Delete an existing API key for the authenticated user.

    Parameters
    ----------
    key_id : int
        The unique identifier of the API key to be deleted.
    client : UserSchema
        The currently authenticated user (injected by dependency).
    db : AsyncSession
        Asynchronous database session dependency used to query and persist user data
        (default is provided by dependency injection).

    Returns
    -------
    None
        Returns no content on successful deletion.
    """
    apikey_repo = APIKeyRepository(db=db)
    if not apikey_repo:
        raise HTTPError(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details="Failed to access API key repository",
        )
    if client.id is None:
        raise HTTPError(
            status_code=status.HTTP_400_BAD_REQUEST,
            details="Authenticated user does not have a valid ID",
        )

    success = await apikey_repo.adelete_owned_apikey(key_id=key_id, owner_id=client.id)
    if not success:
        raise HTTPError(
            status_code=status.HTTP_404_NOT_FOUND,
            details="API key not found or does not belong to the authenticated user",
        )
