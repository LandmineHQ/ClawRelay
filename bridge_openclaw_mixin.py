import asyncio
import json
import logging
import time
import uuid
from typing import Any, cast

import aiohttp

from bridge_config import Config


class OpenClawGatewayMixin:
    cfg: Config
    session: aiohttp.ClientSession | None
    preferred_openclaw_ws_url: str
    image_support_cache_until: float
    image_support_cache_value: bool | None
    image_support_model_desc: str

    @staticmethod
    def _to_json_dict(value: object) -> dict[str, Any] | None:
        if not isinstance(value, dict):
            return None
        out: dict[str, Any] = {}
        for key, item in cast(dict[object, object], value).items():
            out[str(key)] = item
        return out

    @staticmethod
    def _json_dict_or_empty(value: object) -> dict[str, Any]:
        parsed = OpenClawGatewayMixin._to_json_dict(value)
        if parsed is None:
            return {}
        return parsed

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
    def _extract_text_from_content(content: object) -> str:
        if isinstance(content, str):
            return content.strip()
        if not isinstance(content, list):
            return ""
        content_list = cast(list[object], content)
        chunks: list[str] = []
        for raw_item in content_list:
            item = OpenClawGatewayMixin._to_json_dict(raw_item)
            if item is None:
                continue
            text_value = item.get("text")
            if item.get("type") == "text" and isinstance(text_value, str):
                chunks.append(text_value)
        return "".join(chunks).strip()

    def _openclaw_full_session_key(self, session_key: str) -> str:
        if session_key.startswith("agent:"):
            return session_key
        return f"agent:main:{session_key}"

    async def _reset_openclaw_session(self, session_key: str) -> tuple[bool, str]:
        scopes = self._scopes_list()
        if "operator.admin" not in scopes:
            scopes = [*scopes, "operator.admin"]

        urls: list[str] = []
        for item in [self.preferred_openclaw_ws_url, self.cfg.openclaw_ws_url]:
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

    async def _ws_wait_text_json(
        self, ws: aiohttp.ClientWebSocketResponse, timeout_sec: float
    ) -> dict[str, Any] | None:
        msg = await ws.receive(timeout=timeout_sec)
        if msg.type != aiohttp.WSMsgType.TEXT:
            return None
        try:
            parsed = json.loads(msg.data)
            return self._to_json_dict(parsed)
        except json.JSONDecodeError:
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
            now = asyncio.get_running_loop().time()
            if now >= deadline:
                raise TimeoutError(f"Gateway request timeout: {method}")
            remaining = max(0.1, deadline - now)
            packet = await self._ws_wait_text_json(ws, remaining)
            if not packet:
                continue
            if packet.get("type") == "res" and packet.get("id") == req_id:
                if packet.get("ok") is True:
                    payload = self._to_json_dict(packet.get("payload"))
                    if payload is not None:
                        return payload
                    empty_payload: dict[str, Any] = {}
                    return empty_payload
                err = self._json_dict_or_empty(packet.get("error"))
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
            now = asyncio.get_running_loop().time()
            if now >= deadline:
                await ws.close()
                raise TimeoutError("Gateway connect timeout")
            remaining = max(0.1, deadline - now)
            packet = await self._ws_wait_text_json(ws, remaining)
            if not packet:
                continue
            if packet.get("type") == "event" and packet.get("event") == "connect.challenge":
                req: dict[str, Any] = {
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
                err = self._json_dict_or_empty(packet.get("error"))
                await ws.close()
                raise RuntimeError(f"Gateway connect failed: {err.get('code')} {err.get('message')}")

    async def _detect_image_model_support(self) -> tuple[bool, str]:
        now = time.time()
        if self.image_support_cache_value is not None and now < self.image_support_cache_until:
            return self.image_support_cache_value, self.image_support_model_desc

        urls: list[str] = []
        for item in [self.preferred_openclaw_ws_url, self.cfg.openclaw_ws_url]:
            item = item.strip()
            if item and item not in urls:
                urls.append(item)

        last_err: Exception | None = None
        for url in urls:
            ws: aiohttp.ClientWebSocketResponse | None = None
            try:
                ws = await self._connect_gateway_for_probe(url)
                sessions = await self._gateway_ws_request(ws, "sessions.list", {})
                defaults = self._json_dict_or_empty(sessions.get("defaults"))
                model_provider = str(defaults.get("modelProvider", "")).strip()
                model = str(defaults.get("model", "")).strip()
                model_desc = f"{model_provider}/{model}" if model_provider and model else (model or "unknown")

                models_payload = await self._gateway_ws_request(ws, "models.list", {})
                models_value = models_payload.get("models")
                models = cast(list[object], models_value) if isinstance(models_value, list) else []

                supports_image = False
                for raw_model in models:
                    model_item = self._to_json_dict(raw_model)
                    if model_item is None:
                        continue
                    pid = str(model_item.get("provider", "")).strip()
                    mid = str(model_item.get("id", "")).strip()
                    if model_provider and pid and model_provider != pid:
                        continue
                    if model and not (mid == model or mid.endswith(f"/{model}")):
                        continue
                    inputs = model_item.get("input")
                    if isinstance(inputs, list):
                        input_list = cast(list[object], inputs)
                        if any(str(x).lower() == "image" for x in input_list):
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
                now = asyncio.get_running_loop().time()
                if now >= connect_deadline:
                    raise TimeoutError("OpenClaw connect handshake timeout")
                remaining = max(0.1, connect_deadline - now)
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
                        "scopes": self._scopes_list(),
                        "caps": [],
                        "commands": [],
                        "permissions": {},
                        "auth": self._openclaw_auth_params(),
                        "locale": "zh-CN",
                        "userAgent": "llonebot-openclaw-bridge/0.1.0",
                    }
                    req: dict[str, Any] = {
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
                    err = self._json_dict_or_empty(packet.get("error"))
                    raise RuntimeError(
                        f"OpenClaw connect failed: {err.get('code')} {err.get('message')}"
                    )

            run_id: str | None = None
            latest_text = ""
            done = False
            chat_acked = False
            pending_chat_packets: list[dict[str, Any]] = []
            end_deadline = asyncio.get_running_loop().time() + timeout

            chat_params: dict[str, Any] = {
                "sessionKey": session_key,
                "message": user_text,
                "deliver": False,
                "idempotencyKey": str(uuid.uuid4()),
            }
            if attachments:
                chat_params["attachments"] = attachments

            chat_req: dict[str, Any] = {
                "type": "req",
                "id": chat_req_id,
                "method": "chat.send",
                "params": chat_params,
            }
            logging.info(
                "OpenClaw chat.send via=%s key=%s attachments=%s",
                ws_url,
                session_key,
                len(attachments),
            )
            await ws.send_str(json.dumps(chat_req, ensure_ascii=False))

            while not done:
                now = asyncio.get_running_loop().time()
                if now >= end_deadline:
                    raise TimeoutError("OpenClaw chat timeout")
                remaining = max(0.1, end_deadline - now)
                packet = await self._ws_wait_text_json(ws, remaining)
                if not packet:
                    continue

                p_type = packet.get("type")
                if p_type == "res" and packet.get("id") == chat_req_id:
                    if packet.get("ok") is not True:
                        err = self._json_dict_or_empty(packet.get("error"))
                        raise RuntimeError(
                            f"OpenClaw chat.send failed: {err.get('code')} {err.get('message')}"
                        )
                    chat_acked = True
                    payload = self._json_dict_or_empty(packet.get("payload"))
                    rid = payload.get("runId")
                    if isinstance(rid, str) and rid:
                        run_id = rid
                    # chat.send ack 之前到达的 chat 事件先缓存，ack 后按 run_id 回放一次。
                    while pending_chat_packets and not done:
                        buffered = pending_chat_packets.pop(0)
                        buffered_payload = self._json_dict_or_empty(buffered.get("payload"))
                        buffered_run_id = buffered_payload.get("runId")
                        if not isinstance(buffered_run_id, str) or not buffered_run_id:
                            continue
                        if run_id is None:
                            run_id = buffered_run_id
                        if run_id != buffered_run_id:
                            continue

                        message = self._json_dict_or_empty(buffered_payload.get("message"))
                        text = self._extract_text_from_content(message.get("content"))
                        if text:
                            latest_text = text
                        if buffered_payload.get("state") == "final":
                            done = True
                    continue

                if p_type == "event" and packet.get("event") == "chat":
                    # 先等到 chat.send ack，避免并发时误绑定到其他 run 的事件流。
                    if not chat_acked:
                        pending_chat_packets.append(packet)
                        continue
                    payload = self._json_dict_or_empty(packet.get("payload"))
                    event_run_id = payload.get("runId")
                    if not isinstance(event_run_id, str) or not event_run_id:
                        continue
                    if run_id is None:
                        run_id = event_run_id
                    if run_id != event_run_id:
                        continue

                    message = self._json_dict_or_empty(payload.get("message"))
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
