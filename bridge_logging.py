import json
import logging
import re
from typing import Any


def format_log_value(value: Any, max_len: int = 1800) -> str:
    if value is None:
        return "-"
    try:
        if isinstance(value, str):
            text = re.sub(r"\s+", " ", value).strip()
        else:
            text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:  # noqa: BLE001
        text = str(value)
    if len(text) > max_len:
        return text[: max_len - 1].rstrip() + "…"
    return text


def log_io(
    source: str,
    direction: str,
    content: str,
    received: Any = None,
    sent: Any = None,
    *,
    level: int = logging.INFO,
) -> None:
    logging.log(
        level,
        "来源=%s | 请求方向=%s | 内容=%s | 原始接收信息=%s | 发送信息=%s",
        source,
        direction,
        content,
        format_log_value(received),
        format_log_value(sent),
    )
