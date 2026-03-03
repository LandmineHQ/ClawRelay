import asyncio
import json
import logging
from collections import defaultdict, deque
from typing import Any

import aiohttp

from bridge_config import Config
from bridge_models import MessageImage, PendingObservation
from bridge_onebot_mixin import OneBotMixin
from bridge_openclaw_mixin import OpenClawGatewayMixin


class OpenClawOneBotBridge(OneBotMixin, OpenClawGatewayMixin):
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session: aiohttp.ClientSession | None = None
        self.sem = asyncio.Semaphore(max(1, self.cfg.max_concurrency))
        self.bg_tasks: set[asyncio.Task[Any]] = set()
        self.seen_ids: deque[str] = deque(maxlen=2000)
        self.seen_set: set[str] = set()
        self.gateway_ws: aiohttp.ClientWebSocketResponse | None = None
        self.gateway_ws_url: str = ""
        self.gateway_recv_task: asyncio.Task[Any] | None = None
        self.gateway_connect_lock = asyncio.Lock()
        self.gateway_send_lock = asyncio.Lock()
        self.gateway_pending_reqs: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self.gateway_run_payloads: defaultdict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self.gateway_run_waiters: dict[str, asyncio.Future[None]] = {}
        self.gateway_relay_runs: set[str] = set()
        self.gateway_run_preferred_targets: dict[str, dict[str, Any]] = {}
        self.session_onebot_routes: dict[str, dict[str, Any]] = {}
        self.pending_context: dict[str, deque[PendingObservation]] = defaultdict(
            lambda: deque(maxlen=max(2, self.cfg.context_observation_limit))
        )
        self.session_prompt_bootstrapped: set[str] = set()
        self.preferred_openclaw_ws_url: str = ""
        self.image_support_cache_until: float = 0.0
        self.image_support_cache_value: bool | None = None
        self.image_support_model_desc: str = "unknown"

    def _discard_bg_task(self, task: asyncio.Task[Any]) -> None:
        self.bg_tasks.discard(task)

    @staticmethod
    def _normalize_openclaw_session_key(session_key: str) -> str:
        key = session_key.strip()
        prefix = "agent:main:"
        if key.startswith(prefix):
            return key[len(prefix) :]
        return key

    def _bind_onebot_route(self, session_key: str, event: dict[str, Any]) -> None:
        key = self._normalize_openclaw_session_key(session_key)
        if not key:
            return
        route: dict[str, Any] = {
            "message_type": event.get("message_type"),
        }
        if event.get("self_id") is not None:
            route["self_id"] = event.get("self_id")
        if event.get("user_id") is not None:
            route["user_id"] = event.get("user_id")
        if event.get("group_id") is not None:
            route["group_id"] = event.get("group_id")
        self.session_onebot_routes[key] = route

    @staticmethod
    def _merge_hint_reply(hint: str, reply: str) -> str:
        hint_text = hint.strip()
        reply_text = reply.strip()
        if hint_text and reply_text:
            return f"{hint_text}\n{reply_text}"
        return hint_text or reply_text

    async def _relay_unsolicited_completion_to_onebot(
        self,
        run_id: str,
        initial_payload: dict[str, Any],
        ws_url: str,
    ) -> None:
        payload_session_key = str(initial_payload.get("sessionKey", "")).strip()
        normalized_payload_key = self._normalize_openclaw_session_key(payload_session_key)

        session_key = normalized_payload_key or "unknown"
        relay_ws_url = ws_url

        preferred = self.gateway_run_preferred_targets.get(run_id)
        if preferred:
            preferred_key = str(preferred.get("session_key", "")).strip()
            if preferred_key:
                session_key = preferred_key
            preferred_ws = str(preferred.get("ws_url", "")).strip()
            if preferred_ws:
                relay_ws_url = preferred_ws

        try:
            reply = await self._wait_shared_run_result(
                run_id=run_id,
                session_key=session_key,
                ws_url=relay_ws_url,
                timeout_sec=float(self.cfg.openclaw_timeout_sec),
                initial_payload=initial_payload,
            )
            preferred = self.gateway_run_preferred_targets.get(run_id)
            route_event: dict[str, Any] | None = None
            reply_hint = ""
            if preferred:
                preferred_key = str(preferred.get("session_key", "")).strip()
                if preferred_key:
                    session_key = preferred_key
                preferred_event = preferred.get("event")
                if isinstance(preferred_event, dict):
                    route_event = dict(preferred_event)
                reply_hint = str(preferred.get("reply_hint", "")).strip()
            if route_event is None:
                route_event = self.session_onebot_routes.get(session_key)
            if route_event is None:
                logging.warning(
                    "Drop OpenClaw completion without bound OneBot route: key=%s run_id=%s via=%s",
                    session_key,
                    run_id,
                    relay_ws_url,
                )
                return
            if not reply:
                logging.warning(
                    "Skip empty OpenClaw completion reply: key=%s run_id=%s via=%s",
                    session_key,
                    run_id,
                    relay_ws_url,
                )
                return
            await self._send_onebot_reply(route_event, self._merge_hint_reply(reply_hint, reply))
        except Exception as exc:  # noqa: BLE001
            logging.exception(
                "Unsolicited completion relay failed: %s (key=%s run_id=%s via=%s)",
                exc,
                session_key,
                run_id,
                relay_ws_url,
            )
            preferred = self.gateway_run_preferred_targets.get(run_id)
            route_event: dict[str, Any] | None = None
            if preferred:
                preferred_event = preferred.get("event")
                if isinstance(preferred_event, dict):
                    route_event = dict(preferred_event)
            if route_event is None:
                route_event = self.session_onebot_routes.get(session_key)
            if route_event is not None:
                try:
                    await self._send_onebot_reply(route_event, "OpenClaw暂时不可用，请稍后再试。")
                except Exception:  # noqa: BLE001
                    logging.exception("Fallback unsolicited reply send failed")
        finally:
            self.gateway_relay_runs.discard(run_id)
            self.gateway_run_preferred_targets.pop(run_id, None)

    async def _on_gateway_run_event(
        self, run_id: str, payload: dict[str, Any], ws_url: str
    ) -> None:
        if run_id in self.gateway_relay_runs:
            return
        self.gateway_relay_runs.add(run_id)
        task = asyncio.create_task(
            self._relay_unsolicited_completion_to_onebot(run_id, dict(payload), ws_url)
        )
        self.bg_tasks.add(task)
        task.add_done_callback(self._discard_bg_task)

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
            self.session_prompt_bootstrapped.discard(session_key)
            ok, err = await self._reset_openclaw_session(session_key)
            if ok:
                await self._send_onebot_reply(event, "已重置当前会话。")
            else:
                await self._send_onebot_reply(
                    event,
                    f"会话重置失败：{err}\n请检查 OpenClaw token/scopes（需要 operator.admin）。",
                )

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
                logging.info(
                    "Attachment build result: candidate_images=%s attachments=%s key=%s",
                    len(image_candidates),
                    len(attachments),
                    session_key,
                )
                reply_hint = ""

                if attachments:
                    supports_image, model_desc = await self._detect_image_model_support()
                    if not supports_image:
                        attachments = []
                        reply_hint = (
                            f"【提示】当前模型不支持 image 输入（{model_desc}），已忽略图片附件。"
                        )

                ws_url, run_id, instant_text = await self._submit_openclaw(
                    session_key,
                    prompt_text,
                    attachments,
                )
                if instant_text:
                    await self._send_onebot_reply(
                        event, self._merge_hint_reply(reply_hint, instant_text)
                    )
                    return
                if not run_id:
                    logging.warning(
                        "OpenClaw chat.send ack missing runId/text, waiting async events: key=%s via=%s",
                        session_key,
                        ws_url,
                    )
                    return

                self.gateway_run_preferred_targets[run_id] = {
                    "event": dict(event),
                    "session_key": session_key,
                    "reply_hint": reply_hint,
                    "ws_url": ws_url,
                }
                # chat.send 成功即可返回，后续由 Gateway completion 事件自动转发到 OneBot。
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
        self._bind_onebot_route(session_key, event)
        parsed = self._extract_message(event)
        parsed = await self._augment_parsed_message(event, parsed)
        normalized_text = parsed.text.strip()

        should_reply, latest_text = self._should_reply(event, parsed)
        local_cmd = self._detect_local_command(latest_text, parsed.images)
        if should_reply and local_cmd:
            await self._handle_local_command(event, session_key, local_cmd)
            return

        # Private chat: send user content directly to OpenClaw without local prompt building.
        if event.get("message_type") == "private":
            if not should_reply:
                return
            task = asyncio.create_task(
                self._process_message(
                    event,
                    session_key,
                    latest_text,
                    parsed.images,
                )
            )
            self.bg_tasks.add(task)
            task.add_done_callback(self._discard_bg_task)
            return

        self._record_observation(event, session_key, normalized_text, parsed.images)
        if not should_reply:
            return

        pending = list(self.pending_context[session_key])
        self.pending_context[session_key].clear()
        latest_line = pending[-1].line if pending else latest_text.strip()

        include_guidance = session_key not in self.session_prompt_bootstrapped
        prompt_text = self._build_prompt_from_pending(
            pending,
            latest_line,
            include_guidance=include_guidance,
        )
        self.session_prompt_bootstrapped.add(session_key)
        image_candidates = self._collect_recent_images(pending)
        task = asyncio.create_task(
            self._process_message(
                event,
                session_key,
                prompt_text,
                image_candidates,
            )
        )
        self.bg_tasks.add(task)
        task.add_done_callback(self._discard_bg_task)

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
                                    parsed = json.loads(msg.data)
                                except json.JSONDecodeError:
                                    logging.warning("OneBot non-JSON packet: %s", msg.data[:200])
                                    continue
                                if not isinstance(parsed, dict):
                                    logging.warning("OneBot non-object packet: %s", msg.data[:200])
                                    continue
                                event: dict[str, Any] = {}
                                for key, value in parsed.items():
                                    event[str(key)] = value
                                await self._handle_onebot_event(event)
                            elif msg.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                                break
                except Exception as exc:  # noqa: BLE001
                    logging.exception("OneBot WS error: %s", exc)

                logging.info("Reconnecting in %s seconds...", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, max(delay, self.cfg.max_reconnect_delay_sec))
