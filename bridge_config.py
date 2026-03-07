import os
from dataclasses import dataclass


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass
class Config:
    # Satori 接入参数
    satori_http_base: str = os.getenv("SATORI_HTTP_BASE", "http://127.0.0.1:5600")
    satori_ws_url: str = os.getenv("SATORI_WS_URL", "ws://127.0.0.1:5600/v1/events")
    satori_token: str = os.getenv("SATORI_TOKEN", "")
    satori_platform: str = os.getenv("SATORI_PLATFORM", "chronocat")
    satori_self_id: str = os.getenv("SATORI_SELF_ID", os.getenv("BOT_SELF_QQ", ""))
    satori_processing_emoji_id: int = env_int("SATORI_PROCESSING_EMOJI_ID", 30)

    openclaw_ws_url: str = os.getenv("OPENCLAW_WS_URL", "ws://127.0.0.1:18789")
    openclaw_gateway_token: str = os.getenv(
        "OPENCLAW_GATEWAY_TOKEN", "tifptgkqzf32zrraggcych6gqyq7x52x"
    )
    openclaw_gateway_password: str = os.getenv("OPENCLAW_GATEWAY_PASSWORD", "")
    openclaw_client_id: str = os.getenv("OPENCLAW_CLIENT_ID", "gateway-client")
    openclaw_client_mode: str = os.getenv("OPENCLAW_CLIENT_MODE", "backend")
    openclaw_role: str = os.getenv("OPENCLAW_ROLE", "operator")
    openclaw_scopes: str = os.getenv("OPENCLAW_SCOPES", "operator.read,operator.write")
    openclaw_timeout_sec: int = env_int("OPENCLAW_TIMEOUT_SEC", 180)
    openclaw_session_prefix: str = os.getenv("OPENCLAW_SESSION_PREFIX", "llonebot")
    require_pairing: bool = env_bool(
        "REQUIRE_PAIRING",
        True,
    )
    pairing_code_len: int = env_int(
        "PAIRING_CODE_LEN",
        8,
    )
    pairing_ttl_sec: int = env_int(
        "PAIRING_TTL_SEC",
        3600,
    )
    pairing_store_path: str = os.getenv(
        "PAIRING_STORE_PATH",
        "./.bridge_pairings.json",
    )
    op_user_ids: str = os.getenv("OP_USER_IDS", "1216198007")
    op_store_path: str = os.getenv("OP_STORE_PATH", "./.bridge_ops.json")

    request_timeout_sec: int = env_int("REQUEST_TIMEOUT_SEC", 200)
    reconnect_delay_sec: int = env_int("RECONNECT_DELAY_SEC", 3)
    max_reconnect_delay_sec: int = env_int("MAX_RECONNECT_DELAY_SEC", 30)
    max_concurrency: int = env_int("MAX_CONCURRENCY", 3)
    context_observation_limit: int = env_int("CONTEXT_OBSERVATION_LIMIT", 30)
    context_flush_limit: int = env_int("CONTEXT_FLUSH_LIMIT", 12)
    max_image_attachments: int = env_int("MAX_IMAGE_ATTACHMENTS", 3)
    max_image_download_bytes: int = env_int("MAX_IMAGE_DOWNLOAD_BYTES", 6 * 1024 * 1024)
    image_model_probe_ttl_sec: int = env_int("IMAGE_MODEL_PROBE_TTL_SEC", 300)
    bridge_proxy_url: str = os.getenv("BRIDGE_PROXY_URL", "")
    openclaw_media_container_root: str = os.getenv(
        "OPENCLAW_MEDIA_CONTAINER_ROOT",
        "/home/node/.openclaw",
    )
    openclaw_media_host_root: str = os.getenv(
        "OPENCLAW_MEDIA_HOST_ROOT",
        "/opt/1panel/apps/openclaw/OpenClaw/data/conf",
    )

    group_require_at: bool = env_bool("GROUP_REQUIRE_AT", True)
    group_prefix: str = os.getenv("GROUP_PREFIX", "/ai")
    group_reply_at_sender: bool = env_bool("GROUP_REPLY_AT_SENDER", False)

    self_qq: str = os.getenv("BOT_SELF_QQ", "")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
