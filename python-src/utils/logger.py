import sys
from loguru import logger as _loguru_logger
from config import CONFIG, __NODE_ENV_DEV

# Remove default logger
_loguru_logger.remove()

# Configure based on environment
_level = "DEBUG" if CONFIG["app"]["node_env"] == __NODE_ENV_DEV else "ERROR"
_loguru_logger.add(
    sys.stderr,
    level=_level,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
    colorize=True,
)

logger = _loguru_logger.bind(name="web_scrapers")

__all__ = ["logger"]
