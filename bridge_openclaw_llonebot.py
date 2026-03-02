#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import re
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any

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

    group_require_at: bool = env_bool("GROUP_REQUIRE_AT", True)
    group_prefix: str = os.getenv("GROUP_PREFIX", "/ai")
    group_reply_at_sender: bool = env_bool("GROUP_REPLY_AT_SENDER", True)

    self_qq: str = os.getenv("BOT_SELF_QQ", "")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


class OpenClawOneBotBridge:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session: aiohttp.ClientSession | None = None
        self.sem = asyncio.Semaphore(max(1, self.cfg.max_concurrency))
        self.bg_tasks: set[asyncio.Task[Any]] = set()
        self.seen_ids: deque[str] = deque(maxlen=2000)
        self.seen_set: set[str] = set()

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

    def _extract_text_and_mention(self, event: dict[str, Any]) -> tuple[str, bool]:
        message = event.get("message")
        self_qq = self._get_self_qq(event)
        mentioned = False

        if isinstance(message, str):
            if self_qq:
                at_token = f"[CQ:at,qq={self_qq}]"
                if at_token in message:
                    mentioned = True
                message = message.replace(at_token, "")
            message = re.sub(r"\[CQ:at,qq=\d+\]", "", message)
            text = re.sub(r"\s+", " ", message).strip()
            return text, mentioned

        if isinstance(message, list):
            parts: list[str] = []
            for seg in message:
                if not isinstance(seg, dict):
                    continue
                seg_type = seg.get("type")
                data = seg.get("data") or {}
                if seg_type == "text":
                    parts.append(str(data.get("text", "")))
                elif seg_type == "at":
                    qq = str(data.get("qq", ""))
                    if self_qq and qq == self_qq:
                        mentioned = True
            return "".join(parts).strip(), mentioned

        return "", False

    def _should_process(
        self, event: dict[str, Any], text: str, mentioned: bool
    ) -> tuple[bool, str]:
        if event.get("post_type") != "message":
            return False, text

        message_type = event.get("message_type")
        if message_type not in {"private", "group"}:
            return False, text

        user_id = event.get("user_id")
        self_id = event.get("self_id")
        if user_id is not None and self_id is not None and str(user_id) == str(self_id):
            return False, text

        if not text.strip():
            return False, text

        if message_type == "private":
            return True, text.strip()

        prefix = self.cfg.group_prefix.strip()
        if prefix and text.startswith(prefix):
            stripped = text[len(prefix) :].strip()
            return bool(stripped), stripped

        if self.cfg.group_require_at and not mentioned:
            return False, text

        return True, text.strip()

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

    async def _ask_openclaw_once(self, ws_url: str, session_key: str, user_text: str) -> str:
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

    async def _ask_openclaw(self, session_key: str, user_text: str) -> str:
        urls: list[str] = []
        for item in [self.cfg.openclaw_ws_url, self.cfg.openclaw_ws_fallback_url]:
            item = item.strip()
            if item and item not in urls:
                urls.append(item)

        last_err: Exception | None = None
        for url in urls:
            try:
                return await self._ask_openclaw_once(url, session_key, user_text)
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                logging.warning("OpenClaw connection via %s failed: %s", url, exc)

        raise RuntimeError(f"OpenClaw unavailable: {last_err}")

    async def _process_message(self, event: dict[str, Any], text: str) -> None:
        async with self.sem:
            session_key = self._build_session_key(event)
            logging.info(
                "Processing message user=%s group=%s key=%s text=%s",
                event.get("user_id"),
                event.get("group_id"),
                session_key,
                text,
            )

            try:
                reply = await self._ask_openclaw(session_key, text)
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

        text, mentioned = self._extract_text_and_mention(event)
        should_process, normalized_text = self._should_process(event, text, mentioned)
        if not should_process or not normalized_text:
            return

        task = asyncio.create_task(self._process_message(event, normalized_text))
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
