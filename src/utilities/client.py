import json
from typing import Any

import httpx


class HTTPXClient:
    """
    Async HTTPX client with connection pooling and robust error handling.

    Features
    --------
    - Explicit connection pooling configuration
    - Proper timeout handling
    - Comprehensive exception handling
    - Automatic resource cleanup
    - HTTP/2 support
    """

    def __init__(
        self,
        base_url: str = "",
        timeout: int = 30,
        http2: bool = False,
        pool_max_connections: int = 100,
        pool_max_keepalive: int = 20,
    ) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self.http2 = http2
        self.pool_max_connections = pool_max_connections
        self.pool_max_keepalive = pool_max_keepalive
        self.client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "HTTPXClient":
        """Context manager entry - creates client with connection pool"""
        # Create limits for connection pool
        limits = httpx.Limits(
            max_connections=self.pool_max_connections,  # Total pool size
            max_keepalive_connections=self.pool_max_keepalive,  # Keep-alive pool
            keepalive_expiry=5.0,  # Keep connections alive for 5 seconds
        )

        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            http2=self.http2,
            follow_redirects=True,
            limits=limits,
        )
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - ensures client cleanup"""
        if self.client:
            await self.client.aclose()

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Make an asynchronous GET request with consistent error handling and response formatting.

        This method performs an HTTP GET request using the internal httpx client and returns
        a standardized dictionary containing success status, HTTP status code, parsed response
        data (JSON if possible, otherwise raw text), response headers, and any error message.

        Parameters
        ----------
        url : str
            The target URL for the GET request.
        params : dict[str, Any] or None, optional
            Query parameters to include in the URL (default: None).
        headers : dict[str, str] or None, optional
            Custom HTTP headers to send with the request (default: None).

        Returns
        -------
        dict[str, Any]
            A dictionary with the following keys:

            - success : bool
                True if the request completed with status code < 400, False otherwise.
            - status : int
                The HTTP status code returned by the server.
            - data : dict or str or None
                Parsed JSON response if content-type is JSON and parsing succeeded,
                otherwise the raw response text, or None in case of connection-level errors.
            - headers : dict[str, str]
                The response headers as a dictionary.
            - error : str or None
                Error message if the request failed (connection error, timeout, HTTP error, etc.),
                or None if the request was successful.
        """
        if not self.client:
            return {"success": False, "error": "Client not initialized", "data": None}

        try:
            url = self._get_url(url)
            response = await self.client.get(url, params=params, headers=headers)

            # Try to parse JSON response
            try:
                data = response.json()
            except json.JSONDecodeError:
                data = response.text

            return {
                "success": response.status_code < 400,
                "status": response.status_code,
                "data": data,
                "headers": dict(response.headers),
                "error": None
                if response.status_code < 400
                else f"HTTP {response.status_code}",
            }

        except httpx.ConnectError as e:
            return {"success": False, "error": f"Connection error: {e}", "data": None}
        except httpx.TimeoutException:
            return {"success": False, "error": "Request timeout", "data": None}
        except httpx.HTTPStatusError as e:
            return {"success": False, "error": f"HTTP error: {e}", "data": None}
        except Exception as e:
            return {"success": False, "error": f"Unexpected error: {e}", "data": None}

    async def post(
        self,
        url: str,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Make an asynchronous POST request with consistent error handling and response formatting.

        This method performs an HTTP POST request using the internal httpx client. It supports
        sending either form-encoded data (`data`) or JSON payload (`json_data`), and returns
        a standardized dictionary with success status, HTTP status code, parsed response data,
        response headers, and any error message.

        Parameters
        ----------
        url : str
            The target URL/endpoint for the POST request.
        data : dict[str, Any] or None, optional
            Form-encoded data to send in the request body (e.g., for multipart/form-data or
            application/x-www-form-urlencoded). Mutually exclusive with `json_data` in most
            use cases (default: None).
        json_data : dict[str, Any] or None, optional
            JSON-serializable data to send as the request body (sets Content-Type: application/json
            automatically). Mutually exclusive with `data` in most use cases (default: None).
        headers : dict[str, str] or None, optional
            Custom HTTP headers to include in the request (default: None).

        Returns
        -------
        dict[str, Any]
            A dictionary with the following keys:

            - success : bool
                True if the request completed with status code < 400, False otherwise.
            - status : int
                The HTTP status code returned by the server.
            - data : dict or str or None
                Parsed JSON response if the content-type is JSON and parsing succeeded,
                otherwise the raw response text, or None in case of connection-level errors.
            - headers : dict[str, str]
                The response headers as a dictionary.
            - error : str or None
                Error message if the request failed (connection error, timeout, HTTP error, etc.),
                or None if the request was successful.
        """
        if not self.client:
            return {"success": False, "error": "Client not initialized", "data": None}

        # Fail-fast validation for mutually exclusive payloads
        if data is not None and json_data is not None:
            raise ValueError(
                "Cannot specify both 'data' and 'json_data' simultaneously. Choose one payload format."
            )

        try:
            url = self._get_url(url)
            response = await self.client.post(
                url, data=data, json=json_data, headers=headers
            )

            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = response.text

            return {
                "success": response.status_code < 400,
                "status": response.status_code,
                "data": response_data,
                "headers": dict(response.headers),
                "error": None
                if response.status_code < 400
                else f"HTTP {response.status_code}",
            }

        except httpx.ConnectError as e:
            return {"success": False, "error": f"Connection error: {e}", "data": None}
        except httpx.TimeoutException:
            return {"success": False, "error": "Request timeout", "data": None}
        except httpx.HTTPStatusError as e:
            return {"success": False, "error": f"HTTP error: {e}", "data": None}
        except Exception as e:
            return {"success": False, "error": f"Unexpected error: {e}", "data": None}

    async def put(
        self,
        url: str,
        data: dict[str, Any] | None = None,
        content: bytes | None = None,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Make an asynchronous PUT request with consistent error handling and response formatting.

        This method performs an HTTP PUT request using the internal httpx client. It supports
        sending form-encoded data (`data`), raw bytes (`content`), or JSON payload (`json_data`),
        and returns a standardized dictionary with success status, HTTP status code, parsed
        response data, response headers, and any error message.

        Parameters
        ----------
        url : str
            The target URL/endpoint for the PUT request.
        data : dict[str, Any] or None, optional
            Form-encoded data to send in the request body. Mutually exclusive with `content`
            and `json_data` in most use cases (default: None).
        content : bytes or None, optional
            Raw bytes to send in the request body. Use for binary uploads (e.g., presigned S3 PUT).
            Mutually exclusive with `data` and `json_data` in most use cases (default: None).
        json_data : dict[str, Any] or None, optional
            JSON-serializable data to send as the request body (sets Content-Type: application/json
            automatically). Mutually exclusive with `data` and `content` in most use cases (default: None).
        headers : dict[str, str] or None, optional
            Custom HTTP headers to include in the request (default: None).

        Returns
        -------
        dict[str, Any]
            A dictionary with the following keys:

            - success : bool
                True if the request completed with status code < 400, False otherwise.
            - status : int
                The HTTP status code returned by the server.
            - data : dict or str or None
                Parsed JSON response if the content-type is JSON and parsing succeeded,
                otherwise the raw response text, or None in case of connection-level errors.
            - headers : dict[str, str]
                The response headers as a dictionary.
            - error : str or None
                Error message if the request failed (connection error, timeout, HTTP error, etc.),
                or None if the request was successful.
        """
        if not self.client:
            return {"success": False, "error": "Client not initialized", "data": None}

        # Validate that only one type of payload is provided
        provided_payloads = sum(p is not None for p in (data, content, json_data))
        if provided_payloads > 1:
            raise ValueError(
                "Cannot specify more than one of 'data', 'content', or 'json_data' simultaneously. "
                "Choose one payload format."
            )

        try:
            url = self._get_url(url)
            response = await self.client.put(
                url, data=data, content=content, json=json_data, headers=headers
            )

            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = response.text

            return {
                "success": response.status_code < 400,
                "status": response.status_code,
                "data": response_data,
                "headers": dict(response.headers),
                "error": None
                if response.status_code < 400
                else f"HTTP {response.status_code}",
            }

        except httpx.ConnectError as e:
            return {"success": False, "error": f"Connection error: {e}", "data": None}
        except httpx.TimeoutException:
            return {"success": False, "error": "Request timeout", "data": None}
        except httpx.HTTPStatusError as e:
            return {"success": False, "error": f"HTTP error: {e}", "data": None}
        except Exception as e:
            return {"success": False, "error": f"Unexpected error: {e}", "data": None}

    def _get_url(self, url: str) -> str:
        """Get the full URL of the client."""
        if self.base_url and not url.startswith(("http://", "https://")):
            return self.base_url + url
        return url
