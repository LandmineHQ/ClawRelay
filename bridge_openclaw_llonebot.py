#!/usr/bin/env python3
import asyncio
import base64
import json
import logging
import mimetypes
import os
import re
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
                    images.append(MessageImage(url=url, file=file))
                    text_parts.append("[图片]")
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
                    images.append(
                        MessageImage(
                            url=params.get("url", "").strip(),
                            file=params.get("file", "").strip(),
                        )
                    )
                    text_parts.append("[图片]")
            text_parts.append(payload[last:])
            merged = "".join(text_parts)

            # llonebot 部分场景会把图片渲染成 [图片]filename.ext
            for img_name in re.findall(
                r"\[图片\]\s*([A-Za-z0-9._-]+\.(?:png|jpg|jpeg|gif|webp|bmp))",
                merged,
                flags=re.IGNORECASE,
            ):
                images.append(MessageImage(url="", file=img_name.strip()))

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
            refs = [img.url or img.file or "unknown" for img in images]
            display = f"{display} | 图片: {', '.join(refs)}"
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

    async def _onebot_action(self, action: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        assert self.session is not None
        url = f"{self.cfg.onebot_http_base.rstrip('/')}/{action}"
        try:
            async with self.session.post(
                url,
                headers=self._onebot_headers(),
                json=payload,
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
        candidates = [file_id]
        if file_id:
            lower = file_id.lower()
            upper = file_id.upper()
            if lower not in candidates:
                candidates.append(lower)
            if upper not in candidates:
                candidates.append(upper)

        for cand in candidates:
            data = await self._onebot_action("get_image", {"file": cand})
            if not data:
                continue
            url = str(data.get("url", "")).strip()
            file_name = str(data.get("file_name", "") or data.get("filename", "")).strip()
            local_file = str(data.get("file", "")).strip()
            merged_file = file_name or cand or local_file
            return MessageImage(url=url, file=merged_file)

        return None

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
        onebot_host = urlparse(self.cfg.onebot_http_base).netloc

        for img in images:
            if len(out) >= max_count:
                break
            resolved = img
            if (not resolved.url.strip()) and resolved.file.strip():
                got = await self._resolve_image_with_get_image(resolved.file.strip())
                if got is not None:
                    resolved = got

            url = resolved.url.strip()
            if not (url.startswith("http://") or url.startswith("https://")):
                logging.warning(
                    "Skip image without downloadable url (file=%s)", resolved.file or img.file
                )
                continue

            headers: dict[str, str] | None = None
            if onebot_host and urlparse(url).netloc == onebot_host and self.cfg.onebot_access_token:
                headers = {"Authorization": f"Bearer {self.cfg.onebot_access_token}"}

            try:
                async with self.session.get(url, headers=headers) as resp:
                    if resp.status >= 400:
                        logging.warning("Skip image %s: HTTP %s", url, resp.status)
                        continue
                    content = await resp.read()
                    if not content:
                        continue
                    if len(content) > max_bytes:
                        logging.warning(
                            "Skip image too large (%s bytes): %s", len(content), url
                        )
                        continue
                    mime_type = self._guess_image_mime(
                        url, resolved.file, resp.headers.get("Content-Type", "")
                    )
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
        for item in [self.cfg.openclaw_ws_url, self.cfg.openclaw_ws_fallback_url]:
            item = item.strip()
            if item and item not in urls:
                urls.append(item)

        last_err: Exception | None = None
        for url in urls:
            try:
                return await self._ask_openclaw_once(url, session_key, user_text, attachments)
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
                reply = await self._ask_openclaw(session_key, prompt_text, attachments)
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
        self._record_observation(event, session_key, normalized_text, parsed.images)

        should_reply, latest_text = self._should_reply(event, parsed)
        if not should_reply:
            return

        pending = list(self.pending_context[session_key])
        self.pending_context[session_key].clear()

        prompt_text = self._build_prompt_from_pending(pending, latest_text)
        image_candidates = self._collect_recent_images(pending)
        task = asyncio.create_task(
            self._process_message(event, session_key, prompt_text, image_candidates)
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
