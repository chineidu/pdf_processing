import hashlib
import secrets
import string
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Coroutine

from aiocache import Cache
from fastapi import Depends, Request, Security, status
from fastapi.security import APIKeyHeader, OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession

from src import create_logger
from src.api.core.dependencies import get_cache
from src.api.core.exceptions import HTTPError
from src.config import app_config, app_settings
from src.db.models import DBUser, aget_db
from src.db.repositories.api_repository import APIKeyRepository
from src.db.repositories.user_repository import UserRepository
from src.schemas.db.models import (
    APIKeySchema,
    BaseUserSchema,
    GuestUserSchema,
    UserSchema,
)
from src.schemas.types import RoleTypeEnum, UserStatusEnum

logger = create_logger(__name__)
prefix: str = app_config.api_config.prefix
auth_prefix: str = app_config.api_config.auth_prefix

# =========== Configuration ===========
API_KEY_HEADER = APIKeyHeader(name="X-API-KEY", auto_error=False)
SECRET_KEY: str = app_settings.SECRET_KEY.get_secret_value()
ALGORITHM: str = app_settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES: int = app_settings.ACCESS_TOKEN_EXPIRE_MINUTES
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{auth_prefix}/token")
oauth2_scheme_optional = OAuth2PasswordBearer(
    tokenUrl=f"{auth_prefix}/token", auto_error=False
)

# =========== Password hashing context ===========
# Using `scrypt` instead of `bcrypt` to avoid compatibility issues on macOS
pwd_context = CryptContext(schemes=["scrypt"], deprecated="auto")


# =========== API Key Generation ===========
def generate_api_key(
    prefix_len: int | None = None,
    total_len: int | None = None,
    custom_prefix: str | None = None,
) -> tuple[str, str]:
    """Generates an API key with a specified prefix length and total length.

    Parameters
    ----------
    prefix_len : int | None
        Length of the prefix part of the API key. If None, uses default from settings.
    total_len : int | None
        Total length of the API key. If None, uses default from settings.

    Returns
    -------
    tuple[str, str]
        A tuple containing the prefix and full API key.
    """
    custom_prefix = custom_prefix or app_settings.API_KEY_PREFIX
    prefix_len = prefix_len or app_settings.API_KEY_PREFIX_LENGTH
    total_len = total_len or app_settings.API_KEY_LENGTH
    if prefix_len >= total_len:
        raise ValueError("total_len must be greater than prefix_len")

    alphabet: str = string.ascii_uppercase + string.digits
    body_alphabet: str = alphabet + string.ascii_lowercase
    prefix: str = custom_prefix + "".join(
        secrets.choice(alphabet) for _ in range(prefix_len)
    )
    body: str = "".join(
        secrets.choice(body_alphabet) for _ in range(total_len - prefix_len)
    )
    full_key: str = prefix + body

    return (prefix, full_key)


# =========== Password Verification & Hashing ===========
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hash a password."""
    return pwd_context.hash(password)


# =========== API Key Verification & Hashing ===========
def verify_api_key(provided_key: str, stored_hash: str) -> bool:
    """Verify a password against its hash."""
    computed_hash = hash_api_key(provided_key)
    return computed_hash == stored_hash


def hash_api_key(provided_keys: str) -> str:
    """Hash a provided keys using the SHA256 algorithm and salt."""
    salt = app_settings.API_KEY_SALT or ""
    return hashlib.sha256((salt + provided_keys).encode()).hexdigest()


# =========== JWT Token Management ===========
def create_access_token(
    data: dict[str, str], expires_delta: timedelta | None = None
) -> str:
    """Create a JWT access token."""
    to_encode: dict[str, Any] = data.copy()
    expire: datetime = datetime.now() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})  # type: ignore

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(
    token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(aget_db)
) -> BaseUserSchema:
    """Get the current user from the JWT token."""
    credentials_exception = HTTPError(
        status_code=status.HTTP_401_UNAUTHORIZED,
        details="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload: dict[str, Any] = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        name: str | None = payload.get("sub")

        if name is None:
            raise credentials_exception

    except JWTError as e:
        raise credentials_exception from e

    user_repo = UserRepository(db)
    db_user = await user_repo.aget_user_by_name(name=name)
    if db_user is None:
        raise credentials_exception

    user_schema: BaseUserSchema | None = user_repo.convert_DBUser_to_schema(db_user)
    if user_schema is None:
        raise credentials_exception

    if not user_schema.is_active:
        raise HTTPError(status_code=status.HTTP_403_FORBIDDEN, details="Inactive user")

    return user_schema


async def get_current_user_or_guest(
    token: str = Depends(oauth2_scheme_optional), db: AsyncSession = Depends(aget_db)
) -> BaseUserSchema | GuestUserSchema:
    """Get the current user from JWT token or return a guest if no token provided."""
    # If no token provided, return guest user
    if not token:
        logger.info("No authentication token provided, returning guest user")
        return GuestUserSchema()
    try:
        payload: dict[str, Any] = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        name: str | None = payload.get("sub")

        if name is None:
            logger.warning("No 'sub' found in JWT payload, returning guest user")
            return GuestUserSchema()

    except JWTError as e:
        logger.warning(f"JWT validation failed: {e}, returning guest user")
        return GuestUserSchema()

    user_repo = UserRepository(db)
    db_user = await user_repo.aget_user_by_name(name=name)
    if db_user is None:
        logger.warning(f"user not found for name: {name}, returning guest user")
        return GuestUserSchema()

    user_schema: BaseUserSchema | None = user_repo.convert_DBUser_to_schema(db_user)
    if user_schema is None:
        logger.error(
            f"Failed to convert DBUser to UserSchema for name: {name}, returning guest user"
        )
        return GuestUserSchema()

    if not user_schema.is_active:
        logger.warning(f"Inactive user attempted access: {name}, returning guest user")
        return GuestUserSchema()

    return user_schema


async def authenticate_user(
    db: AsyncSession, username: str, password: str
) -> DBUser | None:
    """Authenticate user with username and password."""
    user_repo = UserRepository(db)
    db_user = await user_repo.aget_user_by_name(username)
    if not db_user:
        return None

    if not verify_password(password, db_user.password_hash):
        return None

    return db_user


async def get_current_active_user(
    current_user: UserSchema = Depends(get_current_user),
) -> UserSchema:  # noqa: B008
    """Get the current active user."""
    if not current_user.is_active:
        raise HTTPError(status_code=status.HTTP_403_FORBIDDEN, details="Inactive user")
    return current_user


async def get_current_admin_user(
    current_user: UserSchema = Depends(get_current_active_user),
) -> UserSchema:  # noqa: B008
    """Get the current active admin user."""
    if RoleTypeEnum.ADMIN not in current_user.roles:
        raise HTTPError(
            status_code=status.HTTP_403_FORBIDDEN,
            details="Admin access required",
        )
    return current_user


# =========== API Key Authentication ===========
def get_api_key_from_header(api_key: str | None = Security(API_KEY_HEADER)) -> str:
    """Extract API key from header."""
    if not api_key:
        raise HTTPError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            details="API key missing",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return api_key


async def avalidate_credit_balance(db_user: DBUser, cost: Decimal) -> bool:
    """Validate that the user has enough credits.

    Raises
    ------
    HTTPError
        If the user does not have enough credits.
    """
    if db_user.credits < cost:
        raise HTTPError(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            details="Insufficient credits",
        )
    return True


async def get_current_api_key(
    request: Request,
    api_key: str = Depends(get_api_key_from_header),
    db: AsyncSession = Depends(aget_db),
    cache: Cache = Depends(get_cache),  # noqa: ARG001
) -> APIKeySchema:
    """Dependency to get the current API key."""
    custom_prefix = app_settings.API_KEY_PREFIX
    prefix_length = len(custom_prefix) + app_settings.API_KEY_PREFIX_LENGTH

    if len(api_key) < prefix_length:
        raise HTTPError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            details="Invalid API key format",
            headers={"WWW-Authenticate": "Bearer"},
        )
    key_prefix = api_key[:prefix_length]

    api_key_repo = APIKeyRepository(db)
    db_api_key = await api_key_repo.aget_api_key_by_prefix(key_prefix=key_prefix)
    db_user = db_api_key.user if db_api_key else None

    # Validate the API key and associated user
    if not db_user or db_user.status != UserStatusEnum.ACTIVE:
        logger.warning(f"Unauthorized access attempt with API key prefix: {key_prefix}")
        raise HTTPError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            details="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not db_api_key or not verify_api_key(api_key, db_api_key.key_hash):
        logger.warning(f"Unauthorized access attempt with API key prefix: {key_prefix}")
        raise HTTPError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            details="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check if the API key is active
    if not db_api_key.is_active:
        logger.warning(f"Inactive API key used with prefix: {key_prefix}")
        raise HTTPError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            details="API key is inactive",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check if the API key is expired
    if db_api_key.expires_at and db_api_key.expires_at < datetime.now(timezone.utc):
        logger.warning(f"Expired API key used with prefix: {key_prefix}")
        raise HTTPError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            details="API key has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Store credit deduction info in request state (will be deducted by middleware on success)
    if db_user:
        # Per-endpoint cost overrides
        path: str = request.url.path if request else ""
        cost_map = app_config.endpoint_policies_config.credit_costs or {}
        default_cost = cost_map.get("default", app_settings.CREDIT_COST_PER_REQUEST)
        cost_value = cost_map.get(path, default_cost)
        cost = Decimal(str(cost_value))

        if await avalidate_credit_balance(db_user, cost):
            # Required by BillingMiddleware to deduct credits after successful request
            request.state.api_key_user_id = db_user.id
            request.state.api_key_id = db_api_key.id
            request.state.api_key_cost = cost

    key_obj = api_key_repo.convert_DBAPIKey_to_schema(db_api_key)
    if not key_obj:
        logger.error(f"Failed to convert DB object to schema for prefix: {key_prefix}")
        raise HTTPError(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details="Internal server error",
        )

    return key_obj


# =========== Scope-based Authentication ===========
def require_scope(*required_scopes: str) -> Callable[..., Coroutine[Any, Any, None]]:
    """Dependency to require specific scopes for an endpoint. It requires an API key with all
    specified scopes.

    Parameters
    ----------
    required_scopes : str
        The scopes required to access the endpoint.

    Returns
    -------
    Callable[..., Coroutine[Any, Any, None]]
        A dependency function that checks for the required scopes.

    Usage
    -----
        @router.get("/some-endpoint")
        async def some_endpoint(
            api_key: ApiKeySchema = Depends(require_scope("read:data"))
        ):
            ...

        # Multiple scopes
        @router.post("/another-endpoint")
        async def another_endpoint(
            api_key: ApiKeySchema = Depends(require_scope("write:data", "admin"))
        ):
            ...
    """

    async def _arequire_scope(
        api_key: APIKeySchema = Depends(get_current_api_key),
    ) -> None:
        if not all(scope in api_key.scopes for scope in required_scopes):
            logger.warning(
                f"API key {api_key.key_prefix} missing required scopes: {required_scopes}"
            )
            raise HTTPError(
                status_code=status.HTTP_403_FORBIDDEN,
                details=f"Insufficient permissions. Needs scopes: {', '.join(required_scopes)}",
            )

    return _arequire_scope


def require_any_scope(
    *required_scopes: str,
) -> Callable[..., Coroutine[Any, Any, None]]:
    """Dependency to require any scope for an endpoint. It requires an API key with at
    least one of the specified scopes.

    Parameters
    ----------
    required_scopes : str
        The scopes required to access the endpoint.

    Returns
    -------
    Callable[..., Coroutine[Any, Any, None]]
        A dependency function that checks for the required scopes.

    Usage
    -----

        # Multiple scopes
        @router.post("/another-endpoint")
        async def another_endpoint(
            api_key: ApiKeySchema = Depends(require_any_scope("read:data", "admin"))
        ):
            ...
    """

    async def _arequire_any_scope(
        api_key: APIKeySchema = Depends(get_current_api_key),
    ) -> None:
        if not any(scope in api_key.scopes for scope in required_scopes):
            logger.warning(
                f"API key {api_key.key_prefix} has insufficient required scopes: {required_scopes}"
            )
            raise HTTPError(
                status_code=status.HTTP_403_FORBIDDEN,
                details=f"Insufficient permissions. Needs one of {', '.join(required_scopes)}",
            )

    return _arequire_any_scope
