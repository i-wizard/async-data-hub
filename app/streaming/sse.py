import json
from typing import Any, Dict


def format_sse(data: Dict[str, Any], event: str = "message") -> str:
    """
    Formats payloads as SSE frames so streaming endpoints can focus on when to
    emit events rather than on wire-format string details.
    """

    return "event: {event}\ndata: {data}\n\n".format(
        event=event,
        data=json.dumps(data),
    )
