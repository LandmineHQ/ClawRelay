#!/usr/bin/env python3
import asyncio
import base64
import json
import logging
import mimetypes
import os
import re
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from typing import Any
from urllib.parse import urlparse

import aiohttp


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
    onebot_http_base: str = os.getenv("ONEBOT_HTTP_BASE", "http://127.0.0.1:3000")
    onebot_ws_url: str = os.getenv("ONEBOT_WS_URL", "ws://127.0.0.1:3001")
    onebot_access_token: str = os.getenv(
        "ONEBOT_ACCESS_TOKEN", "7debeb46-2354-4b62-90d6-d308967214b3"
    )

    # User requested 18790. In this host, OpenClaw gateway websocket is 18789.
    openclaw_ws_url: str = os.getenv("OPENCLAW_WS_URL", "ws://127.0.0.1:18790")
    openclaw_ws_fallback_url: str = os.getenv("OPENCLAW_WS_FALLBACK_URL", "ws://127.0.0.1:18789")
    openclaw_gateway_token: str = os.getenv(
        "OPENCLAW_GATEWAY_TOKEN", "75fsdbnztp22tbifainyjchy4t7jt5cx"
    )
    openclaw_gateway_password: str = os.getenv("OPENCLAW_GATEWAY_PASSWORD", "")
    openclaw_client_id: str = os.getenv("OPENCLAW_CLIENT_ID", "gateway-client")
    openclaw_client_mode: str = os.getenv("OPENCLAW_CLIENT_MODE", "backend")
    openclaw_role: str = os.getenv("OPENCLAW_ROLE", "operator")
    openclaw_scopes: str = os.getenv("OPENCLAW_SCOPES", "operator.read,operator.write")
    openclaw_timeout_sec: int = env_int("OPENCLAW_TIMEOUT_SEC", 180)
    openclaw_session_prefix: str = os.getenv("OPENCLAW_SESSION_PREFIX", "qq")

    request_timeout_sec: int = env_int("REQUEST_TIMEOUT_SEC", 200)
    reconnect_delay_sec: int = env_int("RECONNECT_DELAY_SEC", 3)
    max_reconnect_delay_sec: int = env_int("MAX_RECONNECT_DELAY_SEC", 30)
    max_concurrency: int = env_int("MAX_CONCURRENCY", 3)
    context_observation_limit: int = env_int("CONTEXT_OBSERVATION_LIMIT", 30)
    context_flush_limit: int = env_int("CONTEXT_FLUSH_LIMIT", 12)
    max_image_attachments: int = env_int("MAX_IMAGE_ATTACHMENTS", 3)
    max_image_download_bytes: int = env_int("MAX_IMAGE_DOWNLOAD_BYTES", 6 * 1024 * 1024)
    image_model_probe_ttl_sec: int = env_int("IMAGE_MODEL_PROBE_TTL_SEC", 300)

    group_require_at: bool = env_bool("GROUP_REQUIRE_AT", True)
    group_prefix: str = os.getenv("GROUP_PREFIX", "/ai")
    group_reply_at_sender: bool = env_bool("GROUP_REPLY_AT_SENDER", True)

    self_qq: str = os.getenv("BOT_SELF_QQ", "")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


@dataclass
class MessageImage:
    url: str
    file: str


@dataclass
class ParsedMessage:
    text: str
    mentioned: bool
    images: list[MessageImage]


@dataclass
class PendingObservation:
    line: str
    normalized_text: str
    images: list[MessageImage]
    ts: float


class OpenClawOneBotBridge:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session: aiohttp.ClientSession | None = None
        self.sem = asyncio.Semaphore(max(1, self.cfg.max_concurrency))
        self.bg_tasks: set[asyncio.Task[Any]] = set()
        self.seen_ids: deque[str] = deque(maxlen=2000)
        self.seen_set: set[str] = set()
        self.pending_context: dict[str, deque[PendingObservation]] = defaultdict(
            lambda: deque(maxlen=max(2, self.cfg.context_observation_limit))
        )
        self.preferred_openclaw_ws_url: str = ""
        self.image_support_cache_until: float = 0.0
        self.image_support_cache_value: bool | None = None
        self.image_support_model_desc: str = "unknown"

    def _onebot_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.cfg.onebot_access_token:
            headers["Authorization"] = f"Bearer {self.cfg.onebot_access_token}"
        return headers

    def _openclaw_auth_params(self) -> dict[str, str]:
        auth: dict[str, str] = {}
        if self.cfg.openclaw_gateway_token:
            auth["token"] = self.cfg.openclaw_gateway_token
        if self.cfg.openclaw_gateway_password:
            auth["password"] = self.cfg.openclaw_gateway_password
        return auth

    def _scopes_list(self) -> list[str]:
        return [s.strip() for s in self.cfg.openclaw_scopes.split(",") if s.strip()]

    @staticmethod
    def _extract_text_from_content(content: Any) -> str:
        if isinstance(content, str):
            return content.strip()
        if not isinstance(content, list):
            return ""
        chunks: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                chunks.append(item["text"])
        return "".join(chunks).strip()

    def _build_session_key(self, event: dict[str, Any]) -> str:
        if event.get("message_type") == "private":
            return f"{self.cfg.openclaw_session_prefix}:private:{event.get('user_id')}"
        return (
            f"{self.cfg.openclaw_session_prefix}:group:{event.get('group_id')}:"
            f"user:{event.get('user_id')}"
        )

    def _get_self_qq(self, event: dict[str, Any]) -> str:
        if self.cfg.self_qq:
            return self.cfg.self_qq
        self_id = event.get("self_id")
        return str(self_id) if self_id is not None else ""

    @staticmethod
    def _parse_cq_params(params_text: str) -> dict[str, str]:
        out: dict[str, str] = {}
        if not params_text.strip():
            return out
        for piece in params_text.split(","):
            if "=" not in piece:
                continue
            key, value = piece.split("=", 1)
            out[key.strip()] = unescape(value.strip())
        return out

    @staticmethod
    def _merge_images(primary: list[MessageImage], secondary: list[MessageImage]) -> list[MessageImage]:
        merged: list[MessageImage] = []
        seen: set[str] = set()
        for img in [*primary, *secondary]:
            key = f"{img.url}|{img.file}"
            if key in seen:
                continue
            seen.add(key)
            merged.append(img)
        return merged

    @staticmethod
    def _image_hash_hint(img: MessageImage) -> str:
        if img.file.strip():
            return os.path.basename(img.file.strip())
        if img.url.strip():
            path = urlparse(img.url.strip()).path
            base = os.path.basename(path)
            if base:
                return base
        return "unknown"

    def _image_tag(self, img: MessageImage) -> str:
        return f"[图片:{self._image_hash_hint(img)}]"

    def _extract_message_from_payload(
        self, payload: Any, self_qq: str
    ) -> ParsedMessage:
        mentioned = False
        images: list[MessageImage] = []

        if isinstance(payload, list):
            text_parts: list[str] = []
            for seg in payload:
                if not isinstance(seg, dict):
                    continue
                seg_type = seg.get("type")
                data = seg.get("data") or {}
                if seg_type == "text":
                    text_parts.append(str(data.get("text", "")))
                elif seg_type == "at":
                    qq = str(data.get("qq", ""))
                    if self_qq and qq == self_qq:
                        mentioned = True
                elif seg_type == "image":
                    url = str(data.get("url", "")).strip()
                    file = str(data.get("file", "")).strip()
                    img = MessageImage(url=url, file=file)
                    images.append(img)
                    text_parts.append(self._image_tag(img))
            text = re.sub(r"\s+", " ", "".join(text_parts)).strip()
            return ParsedMessage(text=text, mentioned=mentioned, images=images)

        if isinstance(payload, str):
            text_parts: list[str] = []
            last = 0
            for matched in re.finditer(r"\[CQ:([a-zA-Z0-9_]+)(?:,([^\]]*))?\]", payload):
                text_parts.append(payload[last : matched.start()])
                last = matched.end()
                cq_type = matched.group(1)
                params = self._parse_cq_params(matched.group(2) or "")
                if cq_type == "at":
                    qq = params.get("qq", "")
                    if self_qq and qq == self_qq:
                        mentioned = True
                    continue
                if cq_type == "image":
                    img = MessageImage(
                        url=params.get("url", "").strip(),
                        file=params.get("file", "").strip(),
                    )
                    images.append(img)
                    text_parts.append(self._image_tag(img))
            text_parts.append(payload[last:])
            merged = "".join(text_parts)

            # llonebot 部分场景会把图片渲染成 [图片]filename.ext
            for img_name in re.findall(
                r"\[图片\]\s*([A-Za-z0-9._-]+\.(?:png|jpg|jpeg|gif|webp|bmp))",
                merged,
                flags=re.IGNORECASE,
            ):
                images.append(MessageImage(url="", file=img_name.strip()))

            # 统一把 [图片]xxx.png 规范成 [图片:xxx.png]
            merged = re.sub(
                r"\[图片\]\s*([A-Za-z0-9._-]+\.(?:png|jpg|jpeg|gif|webp|bmp))",
                r"[图片:\1]",
                merged,
                flags=re.IGNORECASE,
            )

            text = re.sub(r"\s+", " ", merged).strip()
            return ParsedMessage(text=text, mentioned=mentioned, images=images)

        return ParsedMessage(text="", mentioned=False, images=[])

    def _extract_message(self, event: dict[str, Any]) -> ParsedMessage:
        self_qq = self._get_self_qq(event)
        parsed_main = self._extract_message_from_payload(event.get("message"), self_qq)
        parsed_raw = self._extract_message_from_payload(event.get("raw_message"), self_qq)
        merged_images = self._merge_images(parsed_main.images, parsed_raw.images)
        text = parsed_main.text or parsed_raw.text
        return ParsedMessage(
            text=text,
            mentioned=parsed_main.mentioned or parsed_raw.mentioned,
            images=merged_images,
        )

    @staticmethod
    def _sender_name(event: dict[str, Any]) -> str:
        sender = event.get("sender") or {}
        if isinstance(sender, dict):
            card = str(sender.get("card", "")).strip()
            nickname = str(sender.get("nickname", "")).strip()
            if card:
                return card
            if nickname:
                return nickname
        return str(event.get("user_id", "unknown"))

    @staticmethod
    def _format_observation_line(
        sender_name: str, normalized_text: str, images: list[MessageImage], ts: float
    ) -> str:
        display = normalized_text or "（无文本）"
        if images:
            refs = [os.path.basename(img.file.strip()) if img.file.strip() else "unknown" for img in images]
            display = f"{display} | 图片哈希: {', '.join(refs)}"
        time_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
        return f"{time_str} {sender_name}: {display}"

    def _record_observation(
        self, event: dict[str, Any], session_key: str, normalized_text: str, images: list[MessageImage]
    ) -> None:
        sender_name = self._sender_name(event)
        ts_raw = event.get("time")
        try:
            ts = float(ts_raw) if ts_raw is not None else datetime.now().timestamp()
        except (TypeError, ValueError):
            ts = datetime.now().timestamp()
        line = self._format_observation_line(sender_name, normalized_text, images, ts)
        self.pending_context[session_key].append(
            PendingObservation(
                line=line,
                normalized_text=normalized_text,
                images=list(images),
                ts=ts,
            )
        )

    def _should_process_event(self, event: dict[str, Any]) -> bool:
        if event.get("post_type") != "message":
            return False

        message_type = event.get("message_type")
        if message_type not in {"private", "group"}:
            return False

        user_id = event.get("user_id")
        self_id = event.get("self_id")
        if user_id is not None and self_id is not None and str(user_id) == str(self_id):
            return False
        return True

    def _should_reply(
        self, event: dict[str, Any], parsed: ParsedMessage
    ) -> tuple[bool, str]:
        message_type = event.get("message_type")
        raw = parsed.text.strip()
        has_content = bool(raw or parsed.images)
        if not has_content:
            return False, ""

        if message_type == "private":
            return True, raw

        prefix = self.cfg.group_prefix.strip()
        if prefix and raw.startswith(prefix):
            stripped = raw[len(prefix) :].strip()
            if stripped or parsed.images:
                return True, stripped

        if self.cfg.group_require_at and not parsed.mentioned:
            return False, raw

        return True, raw

    @staticmethod
    def _detect_local_command(text: str, images: list[MessageImage]) -> str | None:
        if images:
            return None
        stripped = (text or "").strip().lower()
        if stripped in {"/new", "/help"}:
            return stripped
        return None

    @staticmethod
    def _help_text() -> str:
        return (
            "可用指令：\n"
            "1) `/new`：重置当前会话上下文（开启新会话）\n"
            "2) `/help`：查看指令说明"
        )

    def _openclaw_full_session_key(self, session_key: str) -> str:
        if session_key.startswith("agent:"):
            return session_key
        return f"agent:main:{session_key}"

    async def _reset_openclaw_session(self, session_key: str) -> tuple[bool, str]:
        scopes = self._scopes_list()
        if "operator.admin" not in scopes:
            scopes = [*scopes, "operator.admin"]

        urls: list[str] = []
        for item in [self.preferred_openclaw_ws_url, self.cfg.openclaw_ws_url, self.cfg.openclaw_ws_fallback_url]:
            item = item.strip()
            if item and item not in urls:
                urls.append(item)

        full_key = self._openclaw_full_session_key(session_key)
        last_err: Exception | None = None

        for url in urls:
            ws: aiohttp.ClientWebSocketResponse | None = None
            try:
                ws = await self._connect_gateway_for_probe(url, scopes=scopes)
                await self._gateway_ws_request(ws, "sessions.reset", {"key": full_key})
                self.preferred_openclaw_ws_url = url
                return True, ""
            except Exception as exc:  # noqa: BLE001
                last_err = exc
            finally:
                if ws is not None:
                    await ws.close()

        return False, str(last_err) if last_err else "unknown error"

    async def _handle_local_command(
        self, event: dict[str, Any], session_key: str, command: str
    ) -> None:
        cmd = command.strip().lower()
        if cmd == "/help":
            await self._send_onebot_reply(event, self._help_text())
            return

        if cmd == "/new":
            # 清除桥接侧暂存上下文，并重置 OpenClaw 会话。
            self.pending_context[session_key].clear()
            ok, err = await self._reset_openclaw_session(session_key)
            if ok:
                await self._send_onebot_reply(event, "已重置当前会话。")
            else:
                await self._send_onebot_reply(
                    event,
                    f"会话重置失败：{err}\n请检查 OpenClaw token/scopes（需要 operator.admin）。",
                )

    def _build_prompt_from_pending(
        self, pending: list[PendingObservation], latest_text: str
    ) -> str:
        recent = pending[-max(1, self.cfg.context_flush_limit) :]
        context_lines = [f"{i + 1}. {item.line}" for i, item in enumerate(recent)]
        latest = latest_text.strip() or "（用户发送了图片）"
        return (
            "下面是同一会话近期消息记录（按时间顺序）：\n"
            + "\n".join(context_lines)
            + "\n\n请结合以上记录，回复最后一条来自用户的消息。\n"
            + f"最后一条消息：{latest}"
        )

    @staticmethod
    def _is_image_only_text(text: str) -> bool:
        return bool(re.fullmatch(r"\s*(?:\[图片:[^\]]+\]\s*)+", text or ""))

    def _collect_recent_images(self, pending: list[PendingObservation]) -> list[MessageImage]:
        out: list[MessageImage] = []
        seen: set[str] = set()
        for obs in reversed(pending):
            for img in obs.images:
                key = f"{img.url}|{img.file}"
                if key in seen:
                    continue
                seen.add(key)
                out.append(img)
                if len(out) >= max(1, self.cfg.max_image_attachments):
                    return out
        return out

    def _mark_seen(self, message_id: Any) -> bool:
        if message_id is None:
            return True
        key = str(message_id)
        if key in self.seen_set:
            return False
        self.seen_set.add(key)
        self.seen_ids.append(key)
        while len(self.seen_ids) > self.seen_ids.maxlen:
            old = self.seen_ids.popleft()
            self.seen_set.discard(old)
        return True

    async def _send_onebot_reply(self, event: dict[str, Any], text: str) -> None:
        assert self.session is not None

        message_type = event.get("message_type")
        endpoint: str
        payload: dict[str, Any]
        if message_type == "private":
            endpoint = "send_private_msg"
            payload = {"user_id": event.get("user_id"), "message": text, "auto_escape": False}
        else:
            endpoint = "send_group_msg"
            group_text = text
            if self.cfg.group_reply_at_sender and event.get("user_id") is not None:
                group_text = f"[CQ:at,qq={event['user_id']}] {text}"
            payload = {"group_id": event.get("group_id"), "message": group_text, "auto_escape": False}

        url = f"{self.cfg.onebot_http_base.rstrip('/')}/{endpoint}"
        async with self.session.post(
            url,
            headers=self._onebot_headers(),
            json=payload,
        ) as resp:
            body = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"OneBot HTTP {resp.status}: {body[:500]}")
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                logging.warning("OneBot non-JSON response: %s", body[:200])
                return
            if data.get("status") != "ok" and data.get("retcode") not in (0, None):
                raise RuntimeError(f"OneBot send failed: {data}")

    async def _onebot_action(
        self, action: str, payload: dict[str, Any], timeout_sec: float | None = None
    ) -> dict[str, Any] | None:
        assert self.session is not None
        url = f"{self.cfg.onebot_http_base.rstrip('/')}/{action}"
        try:
            async with self.session.post(
                url,
                headers=self._onebot_headers(),
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout_sec) if timeout_sec else None,
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    logging.warning("OneBot action %s HTTP %s: %s", action, resp.status, text[:200])
                    return None
                data = json.loads(text)
        except Exception as exc:  # noqa: BLE001
            logging.warning("OneBot action %s failed: %s", action, exc)
            return None

        if data.get("status") != "ok" and data.get("retcode") not in (0, None):
            logging.warning("OneBot action %s retcode failed: %s", action, data)
            return None
        result = data.get("data")
        return result if isinstance(result, dict) else {}

    async def _augment_parsed_message(self, event: dict[str, Any], parsed: ParsedMessage) -> ParsedMessage:
        # 当 message 只呈现为 “[图片]xxx.png” 时，补查 get_msg 还原标准消息段。
        if self.session is None:
            return parsed
        message_id = event.get("message_id")
        if message_id is None:
            return parsed

        needs_augment = (not parsed.images) or any((not img.url and img.file) for img in parsed.images)
        if not needs_augment:
            return parsed

        extra = await self._onebot_action("get_msg", {"message_id": message_id})
        if not extra:
            return parsed

        self_qq = self._get_self_qq(event)
        parsed_from_msg = self._extract_message_from_payload(extra.get("message"), self_qq)
        parsed_from_raw = self._extract_message_from_payload(extra.get("raw_message"), self_qq)
        images = self._merge_images(parsed.images, parsed_from_msg.images)
        images = self._merge_images(images, parsed_from_raw.images)
        text = parsed.text or parsed_from_msg.text or parsed_from_raw.text
        return ParsedMessage(
            text=text,
            mentioned=parsed.mentioned or parsed_from_msg.mentioned or parsed_from_raw.mentioned,
            images=images,
        )

    async def _resolve_image_with_get_image(self, file_id: str) -> MessageImage | None:
        # OneBot v11: /get_image(file) -> { file, url, file_name, ... }
        data = await self._onebot_get_image_info(file_id)
        if data:
            url = str(data.get("url", "")).strip()
            file_name = str(data.get("file_name", "") or data.get("filename", "")).strip()
            local_file = str(data.get("file", "")).strip()
            merged_file = file_name or file_id or local_file
            return MessageImage(url=url, file=merged_file)

        return None

    async def _onebot_get_image_info(self, file_id: str) -> dict[str, Any] | None:
        candidates = [file_id]
        if file_id:
            lower = file_id.lower()
            upper = file_id.upper()
            if lower not in candidates:
                candidates.append(lower)
            if upper not in candidates:
                candidates.append(upper)
        for cand in candidates:
            data = await self._onebot_action("get_image", {"file": cand}, timeout_sec=8)
            if data:
                return data
        return None

    @staticmethod
    def _extract_ocr_text(data: dict[str, Any]) -> str:
        if not data:
            return ""
        if isinstance(data.get("text"), str):
            return data["text"].strip()
        lines: list[str] = []
        for key in ("texts", "result", "results"):
            arr = data.get(key)
            if not isinstance(arr, list):
                continue
            for item in arr:
                if isinstance(item, str):
                    val = item.strip()
                elif isinstance(item, dict):
                    val = str(item.get("text", "")).strip()
                else:
                    val = ""
                if val:
                    lines.append(val)
        return "\n".join(lines).strip()

    async def _onebot_ocr_fallback(self, images: list[MessageImage]) -> str:
        # OneBot OCR 作为降级路径，短超时，避免卡住主流程。
        outputs: list[str] = []
        for img in images[: max(1, self.cfg.max_image_attachments)]:
            file_id = img.file.strip()
            if not file_id:
                continue
            info = await self._onebot_get_image_info(file_id)
            if not info:
                continue
            refs = [str(info.get("file", "")).strip(), str(info.get("url", "")).strip()]
            for ref in refs:
                if not ref:
                    continue
                data = await self._onebot_action("ocr_image", {"image": ref}, timeout_sec=10)
                text = self._extract_ocr_text(data or {})
                if text:
                    outputs.append(f"[{file_id}]\n{text}")
                    break
        return "\n\n".join(outputs).strip()

    async def _download_image(self, url: str) -> tuple[bytes | None, str]:
        assert self.session is not None
        onebot_host = urlparse(self.cfg.onebot_http_base).netloc
        headers: dict[str, str] = {"User-Agent": "Mozilla/5.0"}
        if onebot_host and urlparse(url).netloc == onebot_host and self.cfg.onebot_access_token:
            headers["Authorization"] = f"Bearer {self.cfg.onebot_access_token}"

        try:
            async with self.session.get(url, headers=headers) as resp:
                if resp.status >= 400:
                    return None, ""
                body = await resp.read()
                ctype = resp.headers.get("Content-Type", "")
                return body, ctype
        except Exception:  # noqa: BLE001
            return None, ""

    @staticmethod
    def _guess_image_mime(url: str, file_name: str, content_type: str) -> str:
        if content_type:
            lowered = content_type.split(";", 1)[0].strip().lower()
            if lowered.startswith("image/"):
                return lowered
        guessed = mimetypes.guess_type(url or file_name)[0]
        if guessed and guessed.startswith("image/"):
            return guessed
        return "image/jpeg"

    async def _build_image_attachments(self, images: list[MessageImage]) -> list[dict[str, str]]:
        assert self.session is not None
        if not images:
            return []

        out: list[dict[str, str]] = []
        max_count = max(1, self.cfg.max_image_attachments)
        max_bytes = max(1024 * 1024, self.cfg.max_image_download_bytes)
        for img in images:
            if len(out) >= max_count:
                break
            resolved = img
            if resolved.file.strip():
                # 优先使用 get_image 刷新 URL，避免旧 URL 失效。
                got = await self._resolve_image_with_get_image(resolved.file.strip())
                if got is not None:
                    resolved = got
            elif not resolved.url.strip():
                continue

            url = resolved.url.strip()
            if not (url.startswith("http://") or url.startswith("https://")):
                logging.warning(
                    "Skip image without downloadable url (file=%s)", resolved.file or img.file
                )
                continue

            try:
                content, ctype = await self._download_image(url)
                if (not content) and resolved.file.strip():
                    # URL 可能过期，再刷新一次并重试。
                    refreshed = await self._resolve_image_with_get_image(resolved.file.strip())
                    if refreshed and refreshed.url.strip() and refreshed.url.strip() != url:
                        resolved = refreshed
                        content, ctype = await self._download_image(resolved.url.strip())

                if not content:
                    logging.warning("Skip image download failed (file=%s url=%s)", resolved.file, url)
                    continue
                if len(content) > max_bytes:
                    logging.warning("Skip image too large (%s bytes): %s", len(content), url)
                    continue

                mime_type = self._guess_image_mime(resolved.url.strip(), resolved.file, ctype)
                out.append(
                    {
                        "type": "image",
                        "mimeType": mime_type,
                        "content": base64.b64encode(content).decode("ascii"),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                logging.warning("Failed to fetch image %s: %s", url, exc)

        return out

    async def _ws_wait_text_json(
        self, ws: aiohttp.ClientWebSocketResponse, timeout_sec: float
    ) -> dict[str, Any] | None:
        msg = await ws.receive(timeout=timeout_sec)
        if msg.type != aiohttp.WSMsgType.TEXT:
            return None
        try:
            parsed = json.loads(msg.data)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return None
        return None

    async def _gateway_ws_request(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        method: str,
        params: dict[str, Any],
        timeout_sec: float = 15.0,
    ) -> dict[str, Any]:
        req_id = str(uuid.uuid4())
        await ws.send_str(
            json.dumps(
                {"type": "req", "id": req_id, "method": method, "params": params},
                ensure_ascii=False,
            )
        )
        deadline = asyncio.get_running_loop().time() + timeout_sec
        while True:
            remaining = max(1.0, deadline - asyncio.get_running_loop().time())
            packet = await self._ws_wait_text_json(ws, remaining)
            if not packet:
                continue
            if packet.get("type") == "res" and packet.get("id") == req_id:
                if packet.get("ok") is True:
                    payload = packet.get("payload")
                    return payload if isinstance(payload, dict) else {}
                err = packet.get("error") or {}
                raise RuntimeError(
                    f"Gateway request {method} failed: {err.get('code')} {err.get('message')}"
                )

    async def _connect_gateway_for_probe(
        self, ws_url: str, scopes: list[str] | None = None
    ) -> aiohttp.ClientWebSocketResponse:
        assert self.session is not None
        ws = await self.session.ws_connect(ws_url, heartbeat=20)
        deadline = asyncio.get_running_loop().time() + 15.0
        connect_req_id = str(uuid.uuid4())
        while True:
            remaining = max(1.0, deadline - asyncio.get_running_loop().time())
            packet = await self._ws_wait_text_json(ws, remaining)
            if not packet:
                continue
            if packet.get("type") == "event" and packet.get("event") == "connect.challenge":
                req = {
                    "type": "req",
                    "id": connect_req_id,
                    "method": "connect",
                    "params": {
                        "minProtocol": 3,
                        "maxProtocol": 3,
                        "client": {
                            "id": self.cfg.openclaw_client_id,
                            "version": "0.1.0",
                            "platform": "linux",
                            "mode": self.cfg.openclaw_client_mode,
                        },
                        "role": self.cfg.openclaw_role,
                        "scopes": scopes if scopes is not None else self._scopes_list(),
                        "caps": [],
                        "commands": [],
                        "permissions": {},
                        "auth": self._openclaw_auth_params(),
                        "locale": "zh-CN",
                        "userAgent": "llonebot-openclaw-bridge/0.1.0",
                    },
                }
                await ws.send_str(json.dumps(req, ensure_ascii=False))
                continue
            if packet.get("type") == "res" and packet.get("id") == connect_req_id:
                if packet.get("ok") is True:
                    return ws
                err = packet.get("error") or {}
                await ws.close()
                raise RuntimeError(f"Gateway connect failed: {err.get('code')} {err.get('message')}")

    async def _detect_image_model_support(self) -> tuple[bool, str]:
        now = time.time()
        if self.image_support_cache_value is not None and now < self.image_support_cache_until:
            return self.image_support_cache_value, self.image_support_model_desc

        urls: list[str] = []
        for item in [self.preferred_openclaw_ws_url, self.cfg.openclaw_ws_url, self.cfg.openclaw_ws_fallback_url]:
            item = item.strip()
            if item and item not in urls:
                urls.append(item)

        last_err: Exception | None = None
        for url in urls:
            ws: aiohttp.ClientWebSocketResponse | None = None
            try:
                ws = await self._connect_gateway_for_probe(url)
                sessions = await self._gateway_ws_request(ws, "sessions.list", {})
                defaults = sessions.get("defaults") or {}
                model_provider = str(defaults.get("modelProvider", "")).strip()
                model = str(defaults.get("model", "")).strip()
                model_desc = f"{model_provider}/{model}" if model_provider and model else (model or "unknown")

                models_payload = await self._gateway_ws_request(ws, "models.list", {})
                models = models_payload.get("models") if isinstance(models_payload, dict) else []

                supports_image = False
                if isinstance(models, list):
                    for item in models:
                        if not isinstance(item, dict):
                            continue
                        pid = str(item.get("provider", "")).strip()
                        mid = str(item.get("id", "")).strip()
                        if model_provider and pid and model_provider != pid:
                            continue
                        # sessions.list 返回 model 是裸 id；models.list 里的 id 可能是全路径
                        if model and not (mid == model or mid.endswith(f"/{model}")):
                            continue
                        inputs = item.get("input")
                        if isinstance(inputs, list) and any(str(x).lower() == "image" for x in inputs):
                            supports_image = True
                            break

                self.preferred_openclaw_ws_url = url
                self.image_support_cache_value = supports_image
                self.image_support_model_desc = model_desc
                self.image_support_cache_until = now + max(30, self.cfg.image_model_probe_ttl_sec)
                return supports_image, model_desc
            except Exception as exc:  # noqa: BLE001
                last_err = exc
            finally:
                if ws is not None:
                    await ws.close()

        logging.warning("Image model capability probe failed: %s", last_err)
        # 探测失败时不阻塞主流程，默认放行
        return True, "unknown"

    async def _ask_openclaw_once(
        self,
        ws_url: str,
        session_key: str,
        user_text: str,
        attachments: list[dict[str, str]],
    ) -> str:
        assert self.session is not None
        timeout = float(self.cfg.openclaw_timeout_sec)

        async with self.session.ws_connect(ws_url, heartbeat=25) as ws:
            connect_req_id = str(uuid.uuid4())
            chat_req_id = str(uuid.uuid4())
            connect_deadline = asyncio.get_running_loop().time() + timeout

            connected = False
            while not connected:
                remaining = max(1.0, connect_deadline - asyncio.get_running_loop().time())
                packet = await self._ws_wait_text_json(ws, remaining)
                if not packet:
                    continue

                if packet.get("type") == "event" and packet.get("event") == "connect.challenge":
                    connect_params: dict[str, Any] = {
                        "minProtocol": 3,
                        "maxProtocol": 3,
                        "client": {
                            "id": self.cfg.openclaw_client_id,
                            "version": "0.1.0",
                            "platform": "linux",
                            "mode": self.cfg.openclaw_client_mode,
                        },
                        "role": self.cfg.openclaw_role,
                        "scopes": [s.strip() for s in self.cfg.openclaw_scopes.split(",") if s.strip()],
                        "caps": [],
                        "commands": [],
                        "permissions": {},
                        "auth": self._openclaw_auth_params(),
                        "locale": "zh-CN",
                        "userAgent": "llonebot-openclaw-bridge/0.1.0",
                    }
                    req = {
                        "type": "req",
                        "id": connect_req_id,
                        "method": "connect",
                        "params": connect_params,
                    }
                    await ws.send_str(json.dumps(req, ensure_ascii=False))
                    continue

                if packet.get("type") == "res" and packet.get("id") == connect_req_id:
                    if packet.get("ok") is True:
                        connected = True
                        break
                    err = packet.get("error") or {}
                    raise RuntimeError(
                        f"OpenClaw connect failed: {err.get('code')} {err.get('message')}"
                    )

            run_id: str | None = None
            latest_text = ""
            done = False
            end_deadline = asyncio.get_running_loop().time() + timeout

            chat_req = {
                "type": "req",
                "id": chat_req_id,
                "method": "chat.send",
                "params": {
                    "sessionKey": session_key,
                    "message": user_text,
                    "deliver": False,
                    "idempotencyKey": str(uuid.uuid4()),
                },
            }
            if attachments:
                chat_req["params"]["attachments"] = attachments
            logging.info(
                "OpenClaw chat.send via=%s key=%s attachments=%s",
                ws_url,
                session_key,
                len(attachments),
            )
            await ws.send_str(json.dumps(chat_req, ensure_ascii=False))

            while not done:
                remaining = max(1.0, end_deadline - asyncio.get_running_loop().time())
                packet = await self._ws_wait_text_json(ws, remaining)
                if not packet:
                    continue

                p_type = packet.get("type")
                if p_type == "res" and packet.get("id") == chat_req_id:
                    if packet.get("ok") is not True:
                        err = packet.get("error") or {}
                        raise RuntimeError(
                            f"OpenClaw chat.send failed: {err.get('code')} {err.get('message')}"
                        )
                    payload = packet.get("payload") or {}
                    rid = payload.get("runId")
                    if isinstance(rid, str) and rid:
                        run_id = rid
                    continue

                if p_type == "event" and packet.get("event") == "chat":
                    payload = packet.get("payload") or {}
                    event_run_id = payload.get("runId")
                    if run_id is None and isinstance(event_run_id, str) and event_run_id:
                        run_id = event_run_id
                    if run_id and event_run_id and run_id != event_run_id:
                        continue

                    message = payload.get("message") or {}
                    text = self._extract_text_from_content(message.get("content"))
                    if text:
                        latest_text = text
                    if payload.get("state") == "final":
                        done = True
                        break

            if latest_text:
                return latest_text
            return "我暂时没有收到有效回复，请稍后再试。"

    async def _ask_openclaw(
        self, session_key: str, user_text: str, attachments: list[dict[str, str]]
    ) -> str:
        urls: list[str] = []
        ordered_sources = [
            self.preferred_openclaw_ws_url,
            self.cfg.openclaw_ws_url,
            self.cfg.openclaw_ws_fallback_url,
        ]
        for item in ordered_sources:
            item = item.strip()
            if item and item not in urls:
                urls.append(item)

        last_err: Exception | None = None
        for url in urls:
            try:
                result = await self._ask_openclaw_once(url, session_key, user_text, attachments)
                self.preferred_openclaw_ws_url = url
                return result
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                logging.warning("OpenClaw connection via %s failed: %s", url, exc)

        raise RuntimeError(f"OpenClaw unavailable: {last_err}")

    async def _process_message(
        self,
        event: dict[str, Any],
        session_key: str,
        prompt_text: str,
        image_candidates: list[MessageImage],
        latest_text: str,
    ) -> None:
        async with self.sem:
            logging.info(
                "Processing message user=%s group=%s key=%s text=%s images=%s",
                event.get("user_id"),
                event.get("group_id"),
                session_key,
                prompt_text[:120],
                len(image_candidates),
            )

            try:
                attachments = await self._build_image_attachments(image_candidates)
                logging.info(
                    "Attachment build result: candidate_images=%s attachments=%s key=%s",
                    len(image_candidates),
                    len(attachments),
                    session_key,
                )

                if attachments:
                    supports_image, model_desc = await self._detect_image_model_support()
                    if not supports_image:
                        ocr_text = await self._onebot_ocr_fallback(image_candidates)
                        if ocr_text:
                            ocr_prompt = (
                                "当前模型不支持直接图像输入。以下是桥接通过 OneBot OCR 提取的文字，"
                                "请基于 OCR 结果和上下文进行回答；若 OCR 可能不完整请明确说明。\n\n"
                                f"{prompt_text}\n\n"
                                f"OCR结果：\n{ocr_text}"
                            )
                            reply = await self._ask_openclaw(session_key, ocr_prompt, [])
                            await self._send_onebot_reply(event, reply)
                            return

                        warn = (
                            "图片已由桥接下载并上传到 OpenClaw，但当前默认模型不支持图像输入，"
                            "且 OneBot OCR 降级未成功。\n"
                            f"当前模型：`{model_desc}`\n\n"
                            "可选方案：\n"
                            "1) 切换到支持 image 的模型\n"
                            "2) 发送图片中的文字内容，我继续处理"
                        )
                        await self._send_onebot_reply(event, warn)
                        return

                final_prompt = prompt_text
                if attachments:
                    visual_prefix = (
                        f"你已收到 {len(attachments)} 张图片附件，必须基于附件内容回答，"
                    )
                    if self._is_image_only_text(latest_text):
                        visual_suffix = (
                            "\n用户这条消息是纯图片。请先输出：\n"
                            "1) 图片内容描述\n2) OCR文字提取\n3) 关键信息总结"
                        )
                    else:
                        visual_suffix = ""
                    final_prompt = f"{visual_prefix}\n{prompt_text}{visual_suffix}"

                reply = await self._ask_openclaw(session_key, final_prompt, attachments)
                await self._send_onebot_reply(event, reply)
            except Exception as exc:  # noqa: BLE001
                logging.exception("Bridge processing failed: %s", exc)
                try:
                    await self._send_onebot_reply(event, "OpenClaw暂时不可用，请稍后再试。")
                except Exception:  # noqa: BLE001
                    logging.exception("Fallback reply send failed")

    async def _handle_onebot_event(self, event: dict[str, Any]) -> None:
        message_id = event.get("message_id")
        if not self._mark_seen(message_id):
            return

        if not self._should_process_event(event):
            return

        session_key = self._build_session_key(event)
        parsed = self._extract_message(event)
        parsed = await self._augment_parsed_message(event, parsed)
        normalized_text = parsed.text.strip()

        should_reply, latest_text = self._should_reply(event, parsed)
        local_cmd = self._detect_local_command(latest_text, parsed.images)
        if should_reply and local_cmd:
            await self._handle_local_command(event, session_key, local_cmd)
            return

        self._record_observation(event, session_key, normalized_text, parsed.images)
        if not should_reply:
            return

        pending = list(self.pending_context[session_key])
        self.pending_context[session_key].clear()

        prompt_text = self._build_prompt_from_pending(pending, latest_text)
        image_candidates = self._collect_recent_images(pending)
        task = asyncio.create_task(
            self._process_message(
                event,
                session_key,
                prompt_text,
                image_candidates,
                latest_text,
            )
        )
        self.bg_tasks.add(task)
        task.add_done_callback(lambda t: self.bg_tasks.discard(t))

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.cfg.request_timeout_sec)
        delay = max(1, self.cfg.reconnect_delay_sec)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            while True:
                try:
                    logging.info("Connecting OneBot WS: %s", self.cfg.onebot_ws_url)
                    async with session.ws_connect(
                        self.cfg.onebot_ws_url,
                        headers=self._onebot_headers(),
                        heartbeat=30,
                    ) as ws:
                        logging.info("OneBot WS connected")
                        delay = max(1, self.cfg.reconnect_delay_sec)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    event = json.loads(msg.data)
                                except json.JSONDecodeError:
                                    logging.warning("OneBot non-JSON packet: %s", msg.data[:200])
                                    continue
                                if isinstance(event, dict):
                                    await self._handle_onebot_event(event)
                            elif msg.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                                break
                except Exception as exc:  # noqa: BLE001
                    logging.exception("OneBot WS error: %s", exc)

                logging.info("Reconnecting in %s seconds...", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, max(delay, self.cfg.max_reconnect_delay_sec))


async def main() -> None:
    cfg = Config()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    bridge = OpenClawOneBotBridge(cfg)
    await bridge.run()


def cli() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    cli()
