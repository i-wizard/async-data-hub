import logging
from typing import Any, Dict, Optional

RESERVED_LOG_RECORD_KEYS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys())


class _ExtraFormatter(logging.Formatter):
    """
    Appends any extra fields passed to the log call as key=value pairs so
    structured context is always visible without hardcoding field names.
    """

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extra_fields = {k: v for k, v in record.__dict__.items() if k not in RESERVED_LOG_RECORD_KEYS}
        if extra_fields:
            context = " ".join(f"{k}={v}" for k, v in extra_fields.items())
            return f"{base} {context}"
        return base


class CustomLogger:
    """
    Keeps logging usage uniform across the project so async execution traces are
    searchable and consistent even as more subsystems are added.
    """

    _logger = logging.getLogger("async_data_hub")
    _handler = logging.StreamHandler()
    _handler.setFormatter(_ExtraFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    _logger.addHandler(_handler)

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
