from typing import Any

import msgspec
from fastapi.responses import JSONResponse


class MsgSpecJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        """Render the content to JSON bytes using msgspec.

        Parameters
        ----------
        content : Any
            The content to be rendered as JSON.

        Returns
        -------
        bytes
            The JSON-encoded bytes of the content.
        """
        assert content is not None, "Content to render cannot be None"
        return msgspec.json.encode(content)
