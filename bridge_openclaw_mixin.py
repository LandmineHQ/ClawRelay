import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict, deque
from html import escape
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
    gateway_ws: aiohttp.ClientWebSocketResponse | None
    gateway_ws_url: str
    gateway_recv_task: asyncio.Task[Any] | None
    gateway_connect_lock: asyncio.Lock
    gateway_send_lock: asyncio.Lock
    gateway_pending_reqs: dict[str, asyncio.Future[dict[str, Any]]]
    gateway_run_payloads: defaultdict[str, deque[dict[str, Any]]]
    gateway_run_waiters: dict[str, asyncio.Future[None]]

    async def _on_gateway_run_event(
        self, run_id: str, payload: dict[str, Any], ws_url: str
    ) -> None:
        # Optional hook for bridge-level routing/dispatch logic.
        return

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
            if isinstance(text_value, str) and text_value:
                chunks.append(text_value)
                continue

            # 兼容结构化 mention 片段，避免“只有@没有纯文本”时被判空回复。
            seg_type = str(item.get("type", "")).strip().lower()
            if seg_type in {"mention", "at", "user_mention", "mention_user"}:
                mention_id = (
                    item.get("qq")
                    or item.get("userId")
                    or item.get("user_id")
                    or item.get("id")
                )
                if mention_id is not None:
                    mention_value = str(mention_id).strip()
                    if mention_value:
                        mention_text = f'<at id="{escape(mention_value, quote=True)}"/>'
                        chunks.append(mention_text)
                        continue

            for alt_key in ("markdown", "content", "value", "name"):
                alt_value = item.get(alt_key)
                if isinstance(alt_value, str) and alt_value:
                    chunks.append(alt_value)
                    break
        return "".join(chunks).strip()

    @staticmethod
    def _extract_text_from_chat_payload(payload: dict[str, Any]) -> str:
        message_raw = payload.get("message")
        if isinstance(message_raw, str) and message_raw.strip():
            return message_raw.strip()

        message = OpenClawGatewayMixin._json_dict_or_empty(message_raw)
        text = OpenClawGatewayMixin._extract_text_from_content(message.get("content"))
        if text:
            return text

        for msg_key in ("text", "response", "outputText", "result"):
            msg_value = message.get(msg_key)
            if isinstance(msg_value, str) and msg_value.strip():
                return msg_value.strip()

        direct_text = payload.get("text")
        if isinstance(direct_text, str) and direct_text.strip():
            return direct_text.strip()

        for key in ("response", "outputText", "result", "delta", "chunk", "answer"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _event_effective_payload(packet: dict[str, Any]) -> dict[str, Any]:
        payload = OpenClawGatewayMixin._json_dict_or_empty(packet.get("payload"))
        data = OpenClawGatewayMixin._json_dict_or_empty(packet.get("data"))
        merged: dict[str, Any] = {}

        for source in (payload, data, packet):
            for key in (
                "runId",
                "state",
                "event",
                "seq",
                "sessionKey",
                "message",
                "text",
                "response",
                "outputText",
                "result",
                "delta",
                "chunk",
                "answer",
            ):
                if key in merged:
                    continue
                value = source.get(key)
                if value is not None:
                    merged[key] = value
        return merged

    async def _close_shared_gateway(self) -> None:
        ws = self.gateway_ws
        self.gateway_ws = None
        self.gateway_ws_url = ""
        task = self.gateway_recv_task
        self.gateway_recv_task = None

        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:  # noqa: BLE001
                pass

        if ws is not None and not ws.closed:
            await ws.close()

    async def _shared_gateway_recv_loop(
        self, ws: aiohttp.ClientWebSocketResponse, ws_url: str
    ) -> None:
        try:
            while True:
                packet = await self._ws_wait_text_json(ws, 60.0)
                if packet is None:
                    if ws.closed:
                        break
                    continue

                p_type = packet.get("type")
                if p_type == "res":
                    req_id = str(packet.get("id", "")).strip()
                    if req_id:
                        fut = self.gateway_pending_reqs.pop(req_id, None)
                        if fut is not None and not fut.done():
                            fut.set_result(packet)
                    continue

                if p_type != "res":
                    payload = self._event_effective_payload(packet)
                    run_id = payload.get("runId")
                    if isinstance(run_id, str) and run_id:
                        queue = self.gateway_run_payloads[run_id]
                        queue.append(payload)
                        while len(queue) > 100:
                            queue.popleft()
                        waiter = self.gateway_run_waiters.pop(run_id, None)
                        if waiter is not None and not waiter.done():
                            waiter.set_result(None)
                        try:
                            await self._on_gateway_run_event(run_id, payload, ws_url)
                        except Exception as exc:  # noqa: BLE001
                            logging.warning(
                                "gateway.run_hook status=failed run_id=%s via=%s err=%s",
                                run_id,
                                ws_url,
                                exc,
                            )
        except Exception as exc:  # noqa: BLE001
            logging.warning("gateway.shared_loop status=closed via=%s err=%s", ws_url, exc)
        finally:
            if self.gateway_ws is ws:
                self.gateway_ws = None
                self.gateway_ws_url = ""
            err = RuntimeError("Shared OpenClaw gateway disconnected")
            for req_id, fut in list(self.gateway_pending_reqs.items()):
                self.gateway_pending_reqs.pop(req_id, None)
                if not fut.done():
                    fut.set_exception(err)
            for run_id, waiter in list(self.gateway_run_waiters.items()):
                self.gateway_run_waiters.pop(run_id, None)
                if not waiter.done():
                    waiter.set_result(None)
            if not ws.closed:
                await ws.close()

    async def _ensure_shared_gateway(self, ws_url: str) -> aiohttp.ClientWebSocketResponse:
        assert self.session is not None
        if (
            self.gateway_ws is not None
            and not self.gateway_ws.closed
            and self.gateway_ws_url == ws_url
        ):
            return self.gateway_ws

        async with self.gateway_connect_lock:
            if (
                self.gateway_ws is not None
                and not self.gateway_ws.closed
                and self.gateway_ws_url == ws_url
            ):
                return self.gateway_ws

            await self._close_shared_gateway()

            ws = await self.session.ws_connect(ws_url, heartbeat=25)
            connect_req_id = str(uuid.uuid4())
            deadline = asyncio.get_running_loop().time() + 15.0
            connected = False

            while not connected:
                now = asyncio.get_running_loop().time()
                if now >= deadline:
                    await ws.close()
                    raise TimeoutError("OpenClaw shared connect handshake timeout")
                remaining = max(0.1, deadline - now)
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
                    await ws.close()
                    raise RuntimeError(
                        f"OpenClaw shared connect failed: {err.get('code')} {err.get('message')}"
                    )

            self.gateway_ws = ws
            self.gateway_ws_url = ws_url
            self.gateway_recv_task = asyncio.create_task(self._shared_gateway_recv_loop(ws, ws_url))
            return ws

    async def _gateway_shared_request(
        self, ws_url: str, method: str, params: dict[str, Any], timeout_sec: float
    ) -> dict[str, Any]:
        ws = await self._ensure_shared_gateway(ws_url)
        req_id = str(uuid.uuid4())
        req: dict[str, Any] = {
            "type": "req",
            "id": req_id,
            "method": method,
            "params": params,
        }
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        self.gateway_pending_reqs[req_id] = fut

        try:
            async with self.gateway_send_lock:
                if self.gateway_ws is None or self.gateway_ws.closed or self.gateway_ws is not ws:
                    raise RuntimeError("Shared OpenClaw gateway unavailable during send")
                await ws.send_str(json.dumps(req, ensure_ascii=False))
            packet = await asyncio.wait_for(fut, timeout=timeout_sec)
        except Exception:
            pending = self.gateway_pending_reqs.pop(req_id, None)
            if pending is not None and not pending.done():
                pending.cancel()
            raise

        if packet.get("ok") is not True:
            err = self._json_dict_or_empty(packet.get("error"))
            raise RuntimeError(
                f"OpenClaw {method} failed: {err.get('code')} {err.get('message')}"
            )
        payload = self._json_dict_or_empty(packet.get("payload"))
        return payload

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
        try:
            msg = await ws.receive(timeout=timeout_sec)
        except (asyncio.TimeoutError, TimeoutError):
            return None
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

        logging.warning("openclaw.model_probe status=failed err=%s", last_err)
        return True, "unknown"

    async def _wait_shared_run_result(
        self,
        run_id: str,
        session_key: str,
        ws_url: str,
        timeout_sec: float,
        initial_payload: dict[str, Any] | None = None,
    ) -> str | None:
        latest_text = ""
        done = False
        final_wait_deadline: float | None = None
        final_text_grace_sec = 3.0
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_sec

        def consume(payload: dict[str, Any], source: str) -> None:
            nonlocal latest_text, done, final_wait_deadline
            text = self._extract_text_from_chat_payload(payload)
            if text:
                latest_text = text
                final_wait_deadline = None
            state_value = str(payload.get("state", "")).strip().lower()
            event_value = str(payload.get("event", "")).strip().lower()
            is_completion = state_value in {"final", "completed", "completion", "done"} or (
                event_value == "completion" or event_value.endswith(".completion")
            )
            if is_completion:
                if latest_text:
                    done = True
                else:
                    logging.warning(
                        "openclaw.run stage=consume status=completion_no_text source=%s key=%s run_id=%s payload_keys=%s",
                        source,
                        session_key,
                        run_id,
                        sorted(payload.keys()),
                    )
                    final_wait_deadline = loop.time() + final_text_grace_sec

        if initial_payload:
            consume(initial_payload, "ack")

        while not done:
            queue = self.gateway_run_payloads.get(run_id)
            while queue and not done:
                payload = queue.popleft()
                consume(payload, "event")

            if done:
                break

            now = loop.time()
            if now >= deadline:
                logging.warning(
                    "openclaw.run status=timeout key=%s run_id=%s via=%s",
                    session_key,
                    run_id,
                    ws_url,
                )
                break
            if final_wait_deadline is not None and now >= final_wait_deadline:
                break

            remaining = max(0.1, deadline - now)
            if final_wait_deadline is not None:
                remaining = min(remaining, max(0.1, final_wait_deadline - now))

            waiter: asyncio.Future[None] = loop.create_future()
            self.gateway_run_waiters[run_id] = waiter
            try:
                await asyncio.wait_for(waiter, timeout=remaining)
            except (asyncio.TimeoutError, TimeoutError):
                pass
            finally:
                current = self.gateway_run_waiters.get(run_id)
                if current is waiter:
                    self.gateway_run_waiters.pop(run_id, None)

        self.gateway_run_payloads.pop(run_id, None)
        self.gateway_run_waiters.pop(run_id, None)
        if latest_text:
            return latest_text
        logging.warning(
            "openclaw.run status=done_no_text key=%s run_id=%s via=%s",
            session_key,
            run_id,
            ws_url,
        )
        return None

    async def _submit_openclaw_once(
        self,
        ws_url: str,
        session_key: str,
        user_text: str,
        attachments: list[dict[str, str]],
    ) -> tuple[str | None, str | None]:
        timeout = float(self.cfg.openclaw_timeout_sec)
        chat_params: dict[str, Any] = {
            "sessionKey": session_key,
            "message": user_text,
            # 开启 deliver，确保 Gateway 产生可消费的回复事件流，便于桥接实时转发到 OneBot。
            "deliver": True,
            "idempotencyKey": str(uuid.uuid4()),
        }
        if attachments:
            chat_params["attachments"] = attachments

        logging.info(
            "openclaw.chat_send stage=submit via=%s key=%s attachments=%s",
            ws_url,
            session_key,
            len(attachments),
        )
        payload = await self._gateway_shared_request(
            ws_url,
            "chat.send",
            chat_params,
            timeout_sec=min(20.0, timeout),
        )

        run_id_value = payload.get("runId")
        run_id = str(run_id_value).strip() if isinstance(run_id_value, str) else ""
        if not run_id:
            # 兜底：某些实现可能在 ack 里直接给最终文本但不给 runId。
            text = self._extract_text_from_chat_payload(payload)
            if text:
                return None, text
            logging.warning(
                "openclaw.chat_send stage=ack status=missing_runid_and_text key=%s via=%s payload_keys=%s",
                session_key,
                ws_url,
                sorted(payload.keys()),
            )
            return None, None
        return run_id, None

    async def _trigger_openclaw_command_once(
        self,
        ws_url: str,
        session_key: str,
        command_text: str,
    ) -> tuple[str | None, str | None]:
        timeout = float(self.cfg.openclaw_timeout_sec)
        # 新版 Gateway `send` 参数模式：to/message/idempotencyKey。
        # 某些部署不支持在 sessionKey 上直接 send，此时回退到 chat.send('/new')。
        params: dict[str, Any] = {
            "to": session_key,
            "message": command_text,
            "idempotencyKey": str(uuid.uuid4()),
        }
        logging.info(
            "openclaw.send_cmd stage=submit via=%s key=%s text=%s",
            ws_url,
            session_key,
            command_text,
        )
        try:
            payload = await self._gateway_shared_request(
                ws_url,
                "send",
                params,
                timeout_sec=min(20.0, timeout),
            )
            run_id_value = payload.get("runId")
            run_id = str(run_id_value).strip() if isinstance(run_id_value, str) else ""
            if run_id:
                return run_id, None
            text = self._extract_text_from_chat_payload(payload)
            if text:
                return None, text
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "openclaw.send_cmd stage=submit status=rejected_fallback_chat_send key=%s via=%s err=%s",
                session_key,
                ws_url,
                exc,
            )

        # 回退：通过 chat.send 提交命令文本（/new），由网关命令触发器处理。
        return await self._submit_openclaw_once(ws_url, session_key, command_text, [])

    async def _trigger_openclaw_command(
        self, session_key: str, command_text: str
    ) -> tuple[str, str | None, str | None]:
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
                run_id, text = await self._trigger_openclaw_command_once(
                    url, session_key, command_text
                )
                self.preferred_openclaw_ws_url = url
                return url, run_id, text
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                if self.gateway_ws_url == url:
                    await self._close_shared_gateway()
                logging.warning(
                    "openclaw.send_cmd status=failed via=%s err_type=%s err=%r",
                    url,
                    type(exc).__name__,
                    exc,
                )

        raise RuntimeError(f"OpenClaw unavailable: {last_err}")

    async def _submit_openclaw(
        self, session_key: str, user_text: str, attachments: list[dict[str, str]]
    ) -> tuple[str, str | None, str | None]:
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
                run_id, text = await self._submit_openclaw_once(
                    url, session_key, user_text, attachments
                )
                self.preferred_openclaw_ws_url = url
                return url, run_id, text
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                if self.gateway_ws_url == url:
                    await self._close_shared_gateway()
                logging.warning(
                    "openclaw.chat_send status=failed via=%s err_type=%s err=%r",
                    url,
                    type(exc).__name__,
                    exc,
                )

        raise RuntimeError(f"OpenClaw unavailable: {last_err}")
