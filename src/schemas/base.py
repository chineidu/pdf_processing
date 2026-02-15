from typing import Annotated

from pydantic import BaseModel, BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel


def round_probability(value: float) -> float:
    """Round a float value to two decimal places.

    Parameters
    ----------
        value (float): The float value to be rounded.

    Returns
    -------
        float: Rounded value.
    """
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return value


Float = Annotated[float, BeforeValidator(round_probability)]


class BaseSchema(BaseModel):
    """Base schema class that inherits from Pydantic BaseModel.

    This class provides common configuration for all schema classes including
    camelCase alias generation, population by field name, and attribute mapping.
    """

    model_config: ConfigDict = ConfigDict(  # type: ignore
        alias_generator=to_camel,  # Convert field names to camelCase
        populate_by_name=True,
        from_attributes=True,
        arbitrary_types_allowed=False,  # Allow non-standard types like DataFrame objects, etc.
        validate_assignment=True,  # Validate changes after creation
        str_strip_whitespace=True,  # Strip whitespace from strings
        frozen=True,  # Make instances immutable
        use_enum_values=True,  # Serialize enums as their values
    )
