import warnings

from .config import app_config
from .settings import app_settings

warnings.filterwarnings("ignore")
__all__ = ["app_config", "app_settings"]
