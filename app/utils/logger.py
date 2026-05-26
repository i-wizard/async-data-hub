import logging
from typing import Any, Dict, Optional


class CustomLogger:
    """
    Keeps logging usage uniform across the project so async execution traces are
    searchable and consistent even as more subsystems are added.
    """

    _logger = logging.getLogger("async_data_hub")

    @classmethod
    def info(cls, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """
        Writes informational events with structured context because concurrent
        systems are much harder to debug from plain strings alone.
        """

        cls._logger.info(message, extra=extra or {})

    @classmethod
    def warning(cls, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """
        Records warning-level events so partial failures remain visible without
        failing the entire request flow.
        """

        cls._logger.warning(message, extra=extra or {})

    @classmethod
    def error(cls, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """
        Records error-level events with enough context to reconstruct where an
        async workflow failed across services and transports.
        """

        cls._logger.error(message, extra=extra or {})


def configure_logging(log_level: str) -> None:
    """
    Configures root logging once during startup so every async subsystem emits
    logs at the same verbosity and format.
    """

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
