import asyncio
import json
import logging
import os
import re
import secrets
import time
from collections import defaultdict, deque
from html import unescape
from typing import Any

import aiohttp

from bridge_config import Config
from bridge_models import MessageImage, PairingRecord, PairingRequest, PendingObservation
from bridge_onebot_mixin import OneBotMixin
from bridge_openclaw_mixin import OpenClawGatewayMixin

try:
    from aiohttp_socks import ProxyConnector
except ModuleNotFoundError:
    ProxyConnector = None

try:
    from bridge_logging import log_io
except ModuleNotFoundError:
    def log_io(
        source: str,
        direction: str,
        content: str,
        received: Any = None,
        sent: Any = None,
        *,
        level: int = logging.INFO,
    ) -> None:
        def _fmt(value: Any, max_len: int = 1800) -> str:
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

        logging.log(
            level,
            "io source=%s dir=%s action=%s recv=%s sent=%s",
            source,
            direction,
            content,
            _fmt(received),
            _fmt(sent),
        )


class OpenClawOneBotBridge(OneBotMixin, OpenClawGatewayMixin):
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session: aiohttp.ClientSession | None = None
        self.media_session: aiohttp.ClientSession | None = None
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
        self.gateway_session_run_claim_waiters: defaultdict[
            str,
            list[tuple[asyncio.Future[tuple[str, dict[str, Any], str]], set[str]]],
        ] = defaultdict(list)
        self.gateway_relay_runs: set[str] = set()
        self.gateway_run_preferred_targets: dict[str, dict[str, Any]] = {}
        self.gateway_run_processing_markers: dict[str, dict[str, Any]] = {}
        self.gateway_run_wait_notice_tasks: dict[str, asyncio.Task[Any]] = {}
        self.gateway_run_wait_notice_stops: dict[str, asyncio.Event] = {}
        self.cleared_processing_markers: deque[str] = deque(maxlen=4000)
        self.cleared_processing_marker_set: set[str] = set()
        self.session_onebot_routes: dict[str, dict[str, Any]] = {}
        self.op_users: set[str] = set()
        self.pairing_approved: dict[str, PairingRecord] = {}
        self.pairing_pending: dict[str, PairingRequest] = {}
        self.pending_context: dict[str, deque[PendingObservation]] = defaultdict(
            lambda: deque(maxlen=max(2, self.cfg.context_observation_limit))
        )
        self.session_prompt_bootstrapped: set[str] = set()
        self.preferred_openclaw_ws_url: str = ""
        self.image_support_cache_until: float = 0.0
        self.image_support_cache_value: bool | None = None
        self.image_support_model_desc: str = "unknown"
        self.satori_reaction_disabled_until: float = 0.0
        self.satori_reaction_disable_reason: str = ""
        self.satori_default_route: dict[str, str] = {}
        self.satori_last_sn: int = 0
        self._load_ops_store()
        self._load_pairing_store()

    def _discard_bg_task(self, task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            logging.info("task status=cancelled name=%s", task.get_name())
        else:
            exc = task.exception()
            if exc is not None:
                logging.error(
                    "task status=failed name=%s err=%s",
                    task.get_name(),
                    exc,
                )
        self.bg_tasks.discard(task)

    async def _private_wait_notice_loop(
        self,
        run_id: str,
        route_event: dict[str, Any],
        stop_event: asyncio.Event,
    ) -> None:
        delays = [3, 4, 5, 6, 7, 8, 9, 10]
        idx = 0
        while True:
            wait_sec = delays[idx] if idx < len(delays) else 10
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=float(wait_sec))
                return
            except asyncio.TimeoutError:
                pass
            if stop_event.is_set():
                return
            try:
                text = "我还在处理，请耐心等待。"
                log_io(
                    source="bridge",
                    direction="bridge -> satori",
                    content="发送处理中提示",
                    received=None,
                    sent={
                        "run_id": run_id,
                        "route": self._route_log_brief(route_event),
                        "message": text,
                        "wait_sec": wait_sec,
                    },
                )
                await self._send_onebot_reply(route_event, text)
            except Exception:  # noqa: BLE001
                logging.exception("evt.wait_notice status=failed run_id=%s", run_id)
            idx += 1

    def _start_private_wait_notice(self, run_id: str, route_event: dict[str, Any]) -> None:
        if route_event.get("message_type") != "private":
            return
        self._stop_wait_notice(run_id)
        stop_event = asyncio.Event()
        self.gateway_run_wait_notice_stops[run_id] = stop_event
        task = asyncio.create_task(
            self._private_wait_notice_loop(run_id, dict(route_event), stop_event),
            name=f"private-wait-notice:{run_id}",
        )
        self.gateway_run_wait_notice_tasks[run_id] = task
        self.bg_tasks.add(task)
        task.add_done_callback(self._discard_bg_task)

    def _stop_wait_notice(self, run_id: str) -> None:
        stopper = self.gateway_run_wait_notice_stops.pop(run_id, None)
        if stopper is not None:
            stopper.set()
        task = self.gateway_run_wait_notice_tasks.pop(run_id, None)
        if task is not None and not task.done():
            task.cancel()

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
        satori_route_raw = event.get("_satori_route")
        if isinstance(satori_route_raw, dict):
            route["_satori_route"] = dict(satori_route_raw)
        self.session_onebot_routes[key] = route

    @staticmethod
    def _pairing_charset() -> str:
        # Exclude ambiguous characters: 0/O and 1/I.
        return "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"

    @staticmethod
    def _normalize_user_id(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _event_log_brief(event: dict[str, Any]) -> dict[str, Any]:
        raw_msg = str(event.get("raw_message") or "").strip()
        has_images = bool(re.search(r"<(?:img|image)\b", raw_msg, flags=re.IGNORECASE))
        if not has_images:
            message_raw = event.get("message")
            if isinstance(message_raw, list):
                for seg in message_raw:
                    if not isinstance(seg, dict):
                        continue
                    seg_type = str(seg.get("type") or "").strip().lower()
                    if seg_type in {"img", "image"}:
                        has_images = True
                        break
        return {
            "message_id": str(event.get("message_id") or "").strip(),
            "message_type": str(event.get("message_type") or "").strip(),
            "user_id": str(event.get("user_id") or "").strip(),
            "group_id": str(event.get("group_id") or "").strip(),
            "session_self_id": str(event.get("self_id") or "").strip(),
            "raw_message": raw_msg[:240] + ("…" if len(raw_msg) > 240 else ""),
            "has_images": has_images,
        }

    @staticmethod
    def _route_log_brief(event: dict[str, Any]) -> dict[str, Any]:
        route_raw = event.get("_satori_route")
        route = route_raw if isinstance(route_raw, dict) else {}
        return {
            "platform": str(route.get("platform") or "").strip(),
            "self_id": str(route.get("self_id") or "").strip(),
            "channel_id": str(route.get("channel_id") or "").strip(),
            "guild_id": str(route.get("guild_id") or "").strip(),
            "message_type": str(event.get("message_type") or "").strip(),
        }

    @staticmethod
    def _preview_text(text: str, max_len: int = 280) -> str:
        raw = re.sub(r"\s+", " ", (text or "")).strip()
        if len(raw) <= max_len:
            return raw
        return raw[: max_len - 1].rstrip() + "…"

    def _ops_store_path(self) -> str:
        path = self.cfg.op_store_path.strip()
        return path or "./.bridge_ops.json"

    def _pairing_store_path(self) -> str:
        path = self.cfg.pairing_store_path.strip()
        return path or "./.bridge_pairings.json"

    def _ops_from_env(self) -> set[str]:
        users = {
            self._normalize_user_id(uid)
            for uid in self.cfg.op_user_ids.split(",")
            if self._normalize_user_id(uid)
        }
        if not users:
            users.add("1216198007")
        return users

    def _load_ops_store(self) -> None:
        path = self._ops_store_path()
        users = set(self._ops_from_env())
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    parsed = json.load(f)
                if isinstance(parsed, dict):
                    users_value = parsed.get("users")
                    if isinstance(users_value, dict):
                        users.update(
                            self._normalize_user_id(uid)
                            for uid in users_value.keys()
                            if self._normalize_user_id(uid)
                        )
                    elif isinstance(users_value, list):
                        users.update(
                            self._normalize_user_id(uid)
                            for uid in users_value
                            if self._normalize_user_id(uid)
                        )
            self.op_users = {uid for uid in users if uid}
            if not self.op_users:
                self.op_users = {"1216198007"}
            logging.info("state.ops_store stage=load users=%s path=%s", len(self.op_users), path)
            self._save_ops_store()
        except Exception as exc:  # noqa: BLE001
            logging.warning("state.ops_store stage=load status=failed path=%s err=%s", path, exc)
            if not self.op_users:
                self.op_users = {"1216198007"}

    def _save_ops_store(self) -> None:
        path = self._ops_store_path()
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)
            payload: dict[str, Any] = {
                "version": 1,
                "users": {uid: True for uid in sorted(self.op_users)},
                "updatedAt": int(time.time()),
            }
            tmp_path = f"{path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)
        except Exception as exc:  # noqa: BLE001
            logging.warning("state.ops_store stage=save status=failed path=%s err=%s", path, exc)

    def _load_pairing_store(self) -> None:
        path = self._pairing_store_path()
        records: dict[str, PairingRecord] = {}
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    parsed = json.load(f)
                if isinstance(parsed, dict):
                    records_value = parsed.get("records")
                    if isinstance(records_value, dict):
                        for raw_key, raw_item in records_value.items():
                            if not isinstance(raw_item, dict):
                                continue
                            target_type = str(raw_item.get("targetType") or "").strip().lower()
                            target_id = str(raw_item.get("targetId") or "").strip()
                            approved_by = str(raw_item.get("approvedByUserId") or "").strip()
                            approved_at_raw = raw_item.get("approvedAt")
                            try:
                                approved_at = int(approved_at_raw) if approved_at_raw is not None else 0
                            except (TypeError, ValueError):
                                approved_at = 0
                            if target_type not in {"user", "group"} or not target_id:
                                continue
                            key = str(raw_key).strip() or f"{target_type}:{target_id}"
                            records[key] = PairingRecord(
                                target_type=target_type,
                                target_id=target_id,
                                approved_by_user_id=approved_by,
                                approved_at=approved_at or int(time.time()),
                            )
            self.pairing_approved = records
            if records:
                logging.info("state.pairing_store stage=load records=%s path=%s", len(records), path)
        except Exception as exc:  # noqa: BLE001
            logging.warning("state.pairing_store stage=load status=failed path=%s err=%s", path, exc)

    def _save_pairing_store(self) -> None:
        path = self._pairing_store_path()
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)
            payload_records: dict[str, dict[str, Any]] = {}
            for key, record in sorted(self.pairing_approved.items()):
                payload_records[key] = {
                    "targetType": record.target_type,
                    "targetId": record.target_id,
                    "approvedByUserId": record.approved_by_user_id,
                    "approvedAt": int(record.approved_at),
                }
            payload: dict[str, Any] = {
                "version": 1,
                "records": payload_records,
                "updatedAt": int(time.time()),
            }
            tmp_path = f"{path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)
        except Exception as exc:  # noqa: BLE001
            logging.warning("state.pairing_store stage=save status=failed path=%s err=%s", path, exc)

    def _pairing_code_len(self) -> int:
        return max(4, min(12, int(self.cfg.pairing_code_len)))

    def _pairing_ttl_sec(self) -> int:
        return max(60, int(self.cfg.pairing_ttl_sec))

    def _pairing_target_from_event(self, event: dict[str, Any]) -> tuple[str, str, str] | None:
        message_type = str(event.get("message_type", "")).strip()
        if message_type == "private":
            target_type = "user"
            target_id = self._normalize_user_id(event.get("user_id"))
        elif message_type == "group":
            target_type = "group"
            target_id = self._normalize_user_id(event.get("group_id"))
        else:
            return None
        if not target_id:
            return None
        return target_type, target_id, f"{target_type}:{target_id}"

    def _pairing_target_session_key(self, target_type: str, target_id: str) -> str:
        if target_type == "user":
            return f"{self.cfg.openclaw_session_prefix}:private:{target_id}"
        return f"{self.cfg.openclaw_session_prefix}:group:{target_id}"

    @staticmethod
    def _pairing_target_display(target_type: str, target_id: str) -> str:
        if target_type == "user":
            return f"私聊用户 {target_id}"
        return f"群 {target_id}"

    def _is_op_user(self, user_id: str) -> bool:
        uid = self._normalize_user_id(user_id)
        return bool(uid and uid in self.op_users)

    def _extract_pairing_candidate(self, text: str) -> str:
        code_len = self._pairing_code_len()
        normalized = re.sub(r"[^A-Za-z0-9]", "", (text or "").upper())
        if len(normalized) == code_len:
            return normalized
        token_pattern = re.compile(rf"[A-Za-z0-9]{{{code_len}}}")
        matched = token_pattern.search((text or "").upper())
        return matched.group(0) if matched else ""

    def _issue_pairing_request(
        self, target_key: str, requester_user_id: str, requester_message_type: str
    ) -> PairingRequest:
        charset = self._pairing_charset()
        code_len = self._pairing_code_len()
        code = "".join(secrets.choice(charset) for _ in range(code_len))
        req = PairingRequest(
            code=code,
            expires_at=time.time() + self._pairing_ttl_sec(),
            requester_user_id=requester_user_id,
            requester_message_type=requester_message_type,
        )
        self.pairing_pending[target_key] = req
        return req

    def _find_pending_pairing_by_code(self, code: str) -> tuple[str, PairingRequest] | None:
        now = time.time()
        for target_key, req in list(self.pairing_pending.items()):
            if req.expires_at <= now:
                self.pairing_pending.pop(target_key, None)
                continue
            if req.code == code:
                return target_key, req
        return None

    async def _ensure_target_pairing(self, event: dict[str, Any], session_key: str) -> bool:
        if not self.cfg.require_pairing:
            return True

        target = self._pairing_target_from_event(event)
        if target is None:
            return False
        target_type, target_id, target_key = target

        if target_key in self.pairing_approved:
            return True

        now = time.time()
        pending = self.pairing_pending.get(target_key)
        if pending is not None and pending.expires_at <= now:
            self.pairing_pending.pop(target_key, None)
            pending = None

        if pending is None:
            requester_user_id = self._normalize_user_id(event.get("user_id"))
            requester_message_type = str(event.get("message_type", "")).strip()
            pending = self._issue_pairing_request(target_key, requester_user_id, requester_message_type)
            self.session_prompt_bootstrapped.discard(session_key)

        op_list = ", ".join(sorted(self.op_users)) if self.op_users else "（空）"
        target_display = self._pairing_target_display(target_type, target_id)
        await self._send_onebot_reply(
            event,
            (
                f"当前会话（{target_display}）尚未通过 OP 配对审批，暂不转发到 OpenClaw。\n"
                f"配对码：{pending.code}\n"
                f"请由 OP 执行：`/pair {pending.code}`\n"
                f"配对码有效期至 {time.strftime('%H:%M:%S', time.localtime(pending.expires_at))}。\n"
                f"当前 OP：{op_list}"
            ),
        )
        return False

    @staticmethod
    def _merge_hint_reply(hint: str, reply: str) -> str:
        hint_text = hint.strip()
        reply_text = reply.strip()
        if hint_text and reply_text:
            return f"{hint_text}\n{reply_text}"
        return hint_text or reply_text

    @staticmethod
    def _format_error_detail(exc: Exception, max_len: int = 180) -> str:
        detail = str(exc).strip()
        if not detail:
            detail = repr(exc)
        detail = re.sub(r"\s+", " ", detail)
        if len(detail) > max_len:
            detail = detail[: max_len - 1].rstrip() + "…"
        return detail

    @classmethod
    def _openclaw_error_reply(cls, exc: Exception) -> str:
        return f"OpenClaw出错了，{cls._format_error_detail(exc)}"

    @staticmethod
    def _openclaw_empty_reply_notice() -> str:
        return "OpenClaw 可能出错了：gateway 返回了空回复，请稍后重试。"

    async def _notify_openclaw_empty_reply(
        self,
        run_id: str,
        route_event: dict[str, Any],
        payload: dict[str, Any],
    ) -> None:
        notice = self._openclaw_empty_reply_notice()
        log_io(
            source="bridge",
            direction="bridge -> satori",
            content="转发空回复告警",
            received=None,
            sent={
                "run_id": run_id,
                "route": self._route_log_brief(route_event),
                "message": notice,
                "payload": {
                    "event": payload.get("event"),
                    "state": payload.get("state"),
                    "seq": payload.get("seq"),
                    "sessionKey": payload.get("sessionKey"),
                },
            },
            level=logging.WARNING,
        )
        try:
            await self._send_onebot_reply(route_event, notice)
        except Exception:  # noqa: BLE001
            logging.exception("relay.unsolicited status=empty_reply_notice_failed run_id=%s", run_id)

        try:
            await self._clear_processing_emoji(self._processing_marker_from_event(route_event))
        except Exception:  # noqa: BLE001
            logging.exception("relay.unsolicited status=empty_reply_clear_failed run_id=%s", run_id)

    @staticmethod
    def _broken_image_detail_text(broken_images: list[dict[str, str]]) -> str:
        parts: list[str] = []
        for item in broken_images:
            url = str(item.get("url") or "").strip()
            reason = str(item.get("reason") or "").strip()
            if not url:
                continue
            parts.append(f"{url} ({reason or 'unknown'})")
        return "; ".join(parts)

    async def _prepare_gateway_reply_for_satori(
        self,
        *,
        run_id: str,
        session_key: str,
        ws_url: str,
        reply_hint: str,
        reply_text: str,
    ) -> str:
        current_reply = reply_text.strip()
        hint_text = reply_hint.strip()
        _ = session_key, ws_url
        outbound_text = self._merge_hint_reply(hint_text, current_reply)
        sanitized_text, broken_images = await self._literalize_inaccessible_satori_images(
            outbound_text,
            run_id=run_id,
        )
        if broken_images:
            detail = self._broken_image_detail_text(broken_images)
            logging.warning(
                "reply.image stage=probe status=literalized run_id=%s bad=%s",
                run_id,
                detail,
            )
        return sanitized_text

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
                log_io(
                    source="gateway",
                    direction="gateway -> bridge",
                    content="丢弃回复：未绑定路由",
                    received={"run_id": run_id, "payload": initial_payload},
                    sent=None,
                    level=logging.WARNING,
                )
                return
            if not reply:
                log_io(
                    source="gateway",
                    direction="gateway -> bridge",
                    content="丢弃回复：文本为空",
                    received={"run_id": run_id, "payload": initial_payload},
                    sent=None,
                    level=logging.WARNING,
                )
                await self._notify_openclaw_empty_reply(run_id, route_event, initial_payload)
                return
            outbound_text = await self._prepare_gateway_reply_for_satori(
                run_id=run_id,
                session_key=session_key,
                ws_url=relay_ws_url,
                reply_hint=reply_hint,
                reply_text=reply,
            )
            log_io(
                source="bridge",
                direction="bridge -> satori",
                content="发送回复",
                received=None,
                sent={
                    "run_id": run_id,
                    "route": self._route_log_brief(route_event),
                    "message": outbound_text,
                },
            )
            await self._send_onebot_reply(route_event, outbound_text)
        except Exception as exc:  # noqa: BLE001
            logging.exception(
                "relay.unsolicited status=failed key=%s run_id=%s via=%s err=%s",
                session_key,
                run_id,
                relay_ws_url,
                exc,
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
                    error_text = self._openclaw_error_reply(exc)
                    log_io(
                        source="bridge",
                        direction="bridge -> satori",
                        content="转发错误回复",
                        received=None,
                        sent={
                            "run_id": run_id,
                            "route": self._route_log_brief(route_event),
                            "message": error_text,
                        },
                        level=logging.WARNING,
                    )
                    await self._send_onebot_reply(route_event, error_text)
                except Exception:  # noqa: BLE001
                    logging.exception("relay.unsolicited status=fallback_reply_failed run_id=%s", run_id)
        finally:
            self.gateway_relay_runs.discard(run_id)
            self.gateway_run_preferred_targets.pop(run_id, None)
            self._stop_wait_notice(run_id)
            marker = self.gateway_run_processing_markers.pop(run_id, None)
            await self._clear_processing_emoji(marker)

    async def _on_gateway_run_event(
        self, run_id: str, payload: dict[str, Any], ws_url: str
    ) -> None:
        if run_id in self.gateway_relay_runs:
            return
        log_io(
            source="gateway",
            direction="gateway -> bridge",
            content="接收运行事件",
            received={
                "run_id": run_id,
                "state": payload.get("state"),
                "event": payload.get("event"),
                "sessionKey": payload.get("sessionKey"),
                "seq": payload.get("seq"),
            },
            sent=None,
        )
        self.gateway_relay_runs.add(run_id)
        task = asyncio.create_task(
            self._relay_unsolicited_completion_to_onebot(run_id, dict(payload), ws_url)
        )
        self.bg_tasks.add(task)
        task.add_done_callback(self._discard_bg_task)

    @staticmethod
    def _normalize_command_text(command: str) -> str:
        normalized = (command or "").strip().lower().replace("／", "/")
        if normalized.startswith("\\"):
            normalized = "/" + normalized[1:]
        return re.sub(r"\s+", " ", normalized)

    def _command_user_id(self, event: dict[str, Any]) -> str:
        return self._normalize_user_id(event.get("user_id"))

    async def _ensure_op_permission(self, event: dict[str, Any], command_name: str) -> bool:
        user_id = self._command_user_id(event)
        if self._is_op_user(user_id):
            return True
        if event.get("message_type") == "group":
            await self._send_onebot_reply(
                event, f"仅 OP 可在群聊中执行 `{command_name}`。你的 user_id={user_id or 'unknown'}。"
            )
            return False
        await self._send_onebot_reply(
            event, f"仅 OP 可执行 `{command_name}`。你的 user_id={user_id or 'unknown'}。"
        )
        return False

    @staticmethod
    def _extract_user_id_from_command(command: str) -> str:
        at_matched = re.search(
            r"<at\b[^>]*\bid\s*=\s*(?:\"|')?(\d{5,20})(?:\"|')?[^>]*\/?>",
            command or "",
            flags=re.IGNORECASE,
        )
        if at_matched:
            return at_matched.group(1)
        matched = re.search(r"\d{5,20}", command or "")
        return matched.group(0) if matched else ""

    @staticmethod
    def _normalized_command_body(command: str) -> str:
        normalized = OpenClawOneBotBridge._normalize_command_text(command)
        if normalized.startswith("/"):
            normalized = normalized[1:].strip()
        return normalized

    @staticmethod
    def _command_name(command: str) -> str:
        body = OpenClawOneBotBridge._normalized_command_body(command)
        if not body:
            return ""
        return body.split(" ", 1)[0]

    @staticmethod
    def _is_admin_command(command: str) -> bool:
        return OpenClawOneBotBridge._command_name(command) in {"pair", "op", "unpair"}

    async def _handle_pair_command(self, event: dict[str, Any], command: str) -> None:
        if not await self._ensure_op_permission(event, "/pair"):
            return
        code = self._extract_pairing_candidate(command)
        if not code:
            await self._send_onebot_reply(event, "用法：`/pair <配对码>`")
            return
        found = self._find_pending_pairing_by_code(code)
        if found is None:
            await self._send_onebot_reply(event, f"未找到可用配对码：{code}（可能已过期）。")
            return

        target_key, _req = found
        target_parts = target_key.split(":", 1)
        if len(target_parts) != 2:
            await self._send_onebot_reply(event, f"配对目标异常：{target_key}")
            return
        target_type, target_id = target_parts
        approver = self._command_user_id(event) or "unknown"
        approved_at = int(time.time())
        self.pairing_approved[target_key] = PairingRecord(
            target_type=target_type,
            target_id=target_id,
            approved_by_user_id=approver,
            approved_at=approved_at,
        )
        self.pairing_pending.pop(target_key, None)
        self._save_pairing_store()
        target_session_key = self._pairing_target_session_key(target_type, target_id)
        self.session_prompt_bootstrapped.discard(target_session_key)
        await self._send_onebot_reply(
            event,
            (
                f"配对已通过：{self._pairing_target_display(target_type, target_id)}\n"
                f"审批人：{approver}\n"
                f"审批时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(approved_at))}"
            ),
        )

    async def _handle_op_command(self, event: dict[str, Any], command: str) -> None:
        if not await self._ensure_op_permission(event, "/op"):
            return
        normalized = self._normalize_command_text(command)
        parts = [p for p in normalized.split(" ") if p]
        if len(parts) <= 1 or parts[1] in {"list", "ls"}:
            op_list = ", ".join(sorted(self.op_users)) if self.op_users else "（空）"
            await self._send_onebot_reply(event, f"当前 OP 列表：{op_list}")
            return

        action = parts[1]
        if action in {"add", "+"}:
            target_uid = self._extract_user_id_from_command(normalized)
            if not target_uid:
                await self._send_onebot_reply(event, "用法：`/op add <user_id>`")
                return
            if target_uid in self.op_users:
                await self._send_onebot_reply(event, f"user_id={target_uid} 已是 OP。")
                return
            self.op_users.add(target_uid)
            self._save_ops_store()
            await self._send_onebot_reply(event, f"已添加 OP：{target_uid}")
            return

        if action in {"del", "remove", "rm", "-"}:
            target_uid = self._extract_user_id_from_command(normalized)
            if not target_uid:
                await self._send_onebot_reply(event, "用法：`/op del <user_id>`")
                return
            if target_uid not in self.op_users:
                await self._send_onebot_reply(event, f"user_id={target_uid} 不在 OP 列表中。")
                return
            if len(self.op_users) <= 1:
                await self._send_onebot_reply(event, "至少需要保留 1 个 OP，无法删除最后一个。")
                return
            self.op_users.discard(target_uid)
            self._save_ops_store()
            await self._send_onebot_reply(event, f"已移除 OP：{target_uid}")
            return

        await self._send_onebot_reply(
            event,
            "用法：`/op list`、`/op add <user_id>`、`/op del <user_id>`",
        )

    async def _handle_unpair_command(self, event: dict[str, Any], session_key: str) -> None:
        if not await self._ensure_op_permission(event, "/unpair"):
            return
        target = self._pairing_target_from_event(event)
        if target is None:
            await self._send_onebot_reply(event, "无法识别当前会话目标，未执行取消配对。")
            return
        target_type, target_id, target_key = target
        existed = self.pairing_approved.pop(target_key, None)
        self.pairing_pending.pop(target_key, None)
        self.pending_context[session_key].clear()
        self.session_prompt_bootstrapped.discard(session_key)
        self._save_pairing_store()
        if existed is None:
            await self._send_onebot_reply(
                event,
                f"当前会话（{self._pairing_target_display(target_type, target_id)}）原本未配对，已清理待配对状态。",
            )
            return
        await self._send_onebot_reply(
            event,
            f"已移除当前会话配对：{self._pairing_target_display(target_type, target_id)}",
        )

    async def _exec_help_command(
        self, event: dict[str, Any], _session_key: str, _command_body: str
    ) -> None:
        await self._send_onebot_reply(event, self._help_text())

    async def _exec_new_command(
        self, event: dict[str, Any], session_key: str, _command_body: str
    ) -> None:
        if not await self._ensure_target_pairing(event, session_key):
            return
        # 清除桥接侧暂存上下文，并向 OpenClaw 执行 /new。
        self.pending_context[session_key].clear()
        self.session_prompt_bootstrapped.discard(session_key)
        processing_marker = await self._mark_processing_emoji(event)
        try:
            log_io(
                source="satori",
                direction="satori -> bridge",
                content="接收 /new 指令",
                received=self._event_log_brief(event),
                sent=None,
            )
            log_io(
                source="bridge",
                direction="bridge -> gateway",
                content="转发 /new 指令",
                received=None,
                sent={
                    "method": "send",
                    "to": session_key,
                    "message": "/new",
                },
            )
            ws_url, run_id, instant_text = await self._trigger_openclaw_command(
                session_key,
                "/new",
            )
            if instant_text:
                await self._send_onebot_reply(event, instant_text)
                return
            if not run_id:
                await self._send_onebot_reply(event, "已触发 /new，当前会话已开启新上下文。")
                return

            self.gateway_run_preferred_targets[run_id] = {
                "event": dict(event),
                "session_key": session_key,
                "reply_hint": "",
                "ws_url": ws_url,
            }
            self._start_private_wait_notice(run_id, event)
            if processing_marker is not None:
                self.gateway_run_processing_markers[run_id] = processing_marker
                processing_marker = None
        except Exception as exc:  # noqa: BLE001
            logging.exception("openclaw.cmd status=failed cmd=/new err=%s", exc)
            await self._send_onebot_reply(event, self._openclaw_error_reply(exc))
        finally:
            await self._clear_processing_emoji(processing_marker)

    async def _exec_pair_command(
        self, event: dict[str, Any], _session_key: str, command_body: str
    ) -> None:
        await self._handle_pair_command(event, command_body)

    async def _exec_op_command(
        self, event: dict[str, Any], _session_key: str, command_body: str
    ) -> None:
        await self._handle_op_command(event, command_body)

    async def _exec_unpair_command(
        self, event: dict[str, Any], session_key: str, _command_body: str
    ) -> None:
        await self._handle_unpair_command(event, session_key)

    def _local_command_registry(self) -> dict[str, Any]:
        return {
            "help": self._exec_help_command,
            "new": self._exec_new_command,
            "pair": self._exec_pair_command,
            "op": self._exec_op_command,
            "unpair": self._exec_unpair_command,
        }

    async def _handle_local_command(
        self, event: dict[str, Any], session_key: str, command: str
    ) -> None:
        command_body = self._normalized_command_body(command)
        if not command_body:
            return
        command_name = command_body.split(" ", 1)[0]
        handler = self._local_command_registry().get(command_name)
        if handler is None:
            return
        await handler(event, session_key, command_body)

    async def _process_message(
        self,
        event: dict[str, Any],
        session_key: str,
        prompt_text: str,
        image_candidates: list[MessageImage],
        *,
        log_satori_event: bool,
    ) -> None:
        async with self.sem:
            if log_satori_event:
                log_io(
                    source="satori",
                    direction="satori -> bridge",
                    content="接收消息事件",
                    received=self._event_log_brief(event),
                    sent=None,
                )

            processing_marker: dict[str, Any] | None = None
            try:
                processing_marker = await self._mark_processing_emoji(event)
                attachments = await self._build_image_attachments(image_candidates)
                log_io(
                    source="bridge",
                    direction="bridge -> gateway",
                    content="附件构建完成并发送 chat.send",
                    received=None,
                    sent={
                        "method": "chat.send",
                        "sessionKey": session_key,
                        "message_preview": self._preview_text(prompt_text),
                        "candidate_images": len(image_candidates),
                        "attachments": len(attachments),
                    },
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
                    outbound_text = await self._prepare_gateway_reply_for_satori(
                        run_id=run_id or "instant",
                        session_key=session_key,
                        ws_url=ws_url,
                        reply_hint=reply_hint,
                        reply_text=instant_text,
                    )
                    log_io(
                        source="bridge",
                        direction="bridge -> satori",
                        content="发送即时回复",
                        received=None,
                        sent={
                            "run_id": run_id,
                            "session_key": session_key,
                            "route": self._route_log_brief(event),
                            "message": outbound_text,
                        },
                    )
                    await self._send_onebot_reply(event, outbound_text)
                    return
                if not run_id:
                    log_io(
                        source="gateway",
                        direction="gateway -> bridge",
                        content="chat.send 已受理但 ack 无 runId/text，等待异步事件",
                        received={"session_key": session_key, "ws_url": ws_url},
                        sent=None,
                        level=logging.WARNING,
                    )
                    return

                self.gateway_run_preferred_targets[run_id] = {
                    "event": dict(event),
                    "session_key": session_key,
                    "reply_hint": reply_hint,
                    "ws_url": ws_url,
                }
                self._start_private_wait_notice(run_id, event)
                if processing_marker is not None:
                    self.gateway_run_processing_markers[run_id] = processing_marker
                    processing_marker = None
                # chat.send 成功即可返回，后续由 Gateway completion 事件自动转发到 OneBot。
            except Exception as exc:  # noqa: BLE001
                logging.exception("bridge.process status=failed err=%s", exc)
                try:
                    await self._send_onebot_reply(event, self._openclaw_error_reply(exc))
                except Exception:  # noqa: BLE001
                    logging.exception("bridge.process status=fallback_reply_failed")
            finally:
                await self._clear_processing_emoji(processing_marker)

    async def _handle_onebot_event(self, event: dict[str, Any]) -> None:
        message_id = event.get("message_id")
        message_type = str(event.get("message_type") or "").strip()
        user_id = str(event.get("user_id") or "").strip()
        group_id = str(event.get("group_id") or "").strip()
        logging.info(
            "evt.handle stage=start message_id=%s message_type=%s user_id=%s group_id=%s",
            message_id,
            message_type,
            user_id,
            group_id,
        )
        if not self._mark_seen(message_id):
            logging.info("evt.handle stage=skip reason=seen message_id=%s", message_id)
            return

        if not self._should_process_event(event):
            logging.info(
                "evt.handle stage=skip reason=not_processable message_id=%s message_type=%s",
                message_id,
                message_type,
            )
            return

        session_key = self._build_session_key(event)
        self._bind_onebot_route(session_key, event)
        parsed = self._extract_message(event)
        logging.info(
            "evt.handle stage=parsed_base message_id=%s session_key=%s text_len=%s images=%s reply_ids=%s forward_ids=%s mentioned=%s",
            message_id,
            session_key,
            len((parsed.text or "").strip()),
            len(parsed.images),
            len(parsed.reply_ids),
            len(parsed.forward_ids),
            parsed.mentioned,
        )
        try:
            logging.info("evt.handle stage=augment_begin message_id=%s", message_id)
            parsed = await self._augment_parsed_message(event, parsed)
            logging.info(
                "evt.handle stage=augment_done message_id=%s text_len=%s images=%s reply_ids=%s forward_ids=%s mentioned=%s",
                message_id,
                len((parsed.text or "").strip()),
                len(parsed.images),
                len(parsed.reply_ids),
                len(parsed.forward_ids),
                parsed.mentioned,
            )
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "evt.handle stage=augment_failed fallback=base_parsed message_id=%s err=%s",
                event.get("message_id"),
                exc,
            )
        normalized_text = parsed.text.strip()

        should_reply, latest_text = self._should_reply(event, parsed)
        local_cmd = self._detect_local_command(latest_text, parsed.images)
        logging.info(
            "evt.handle stage=trigger_decision message_id=%s should_reply=%s local_cmd=%s latest_text_len=%s mentioned=%s",
            message_id,
            should_reply,
            local_cmd or "",
            len((latest_text or "").strip()),
            parsed.mentioned,
        )
        if local_cmd and (should_reply or self._is_admin_command(local_cmd)):
            logging.info(
                "evt.handle stage=local_cmd_dispatch message_id=%s session_key=%s command=%s",
                message_id,
                session_key,
                local_cmd,
            )
            await self._handle_local_command(event, session_key, local_cmd)
            logging.info("evt.handle stage=local_cmd_done message_id=%s", message_id)
            return

        # Private chat: send user content directly to OpenClaw without local prompt building.
        if event.get("message_type") == "private":
            if not should_reply:
                logging.info(
                    "Satori private message ignored by trigger rules: key=%s text=%s",
                    session_key,
                    normalized_text[:120],
                )
                logging.info("evt.handle stage=exit reason=private_no_reply message_id=%s", message_id)
                return
            if not await self._ensure_target_pairing(event, session_key):
                logging.info("evt.handle stage=exit reason=private_not_paired message_id=%s", message_id)
                return
            task = asyncio.create_task(
                self._process_message(
                    event,
                    session_key,
                    latest_text,
                    parsed.images,
                    log_satori_event=True,
                )
            )
            logging.info(
                "evt.handle stage=private_task_created message_id=%s session_key=%s task=%s",
                message_id,
                session_key,
                task.get_name(),
            )
            self.bg_tasks.add(task)
            task.add_done_callback(self._discard_bg_task)
            return

        self._record_observation(event, session_key, parsed)
        logging.info(
            "evt.handle stage=observation_recorded message_id=%s session_key=%s should_reply=%s pending_size=%s",
            message_id,
            session_key,
            should_reply,
            len(self.pending_context[session_key]),
        )
        if not should_reply:
            logging.info("evt.handle stage=exit reason=group_no_reply message_id=%s", message_id)
            return
        if not await self._ensure_target_pairing(event, session_key):
            logging.info("evt.handle stage=exit reason=group_not_paired message_id=%s", message_id)
            return

        pending = list(self.pending_context[session_key])
        self.pending_context[session_key].clear()
        latest_line = pending[-1].line if pending else latest_text.strip()
        logging.info(
            "evt.handle stage=build_prompt message_id=%s pending_count=%s latest_line_len=%s",
            message_id,
            len(pending),
            len((latest_line or "").strip()),
        )

        include_guidance = session_key not in self.session_prompt_bootstrapped
        prompt_text = self._build_prompt_from_pending(
            pending,
            latest_line,
            include_guidance=include_guidance,
            bot_user_id=self._get_self_qq(event),
        )
        logging.info(
            "evt.handle stage=prompt_ready message_id=%s prompt_len=%s include_guidance=%s",
            message_id,
            len(prompt_text),
            include_guidance,
        )
        self.session_prompt_bootstrapped.add(session_key)
        image_candidates = self._collect_recent_images(pending)
        task = asyncio.create_task(
            self._process_message(
                event,
                session_key,
                prompt_text,
                image_candidates,
                log_satori_event=parsed.mentioned,
            )
        )
        logging.info(
            "evt.handle stage=group_task_created message_id=%s session_key=%s task=%s image_candidates=%s",
            message_id,
            session_key,
            task.get_name(),
            len(image_candidates),
        )
        self.bg_tasks.add(task)
        task.add_done_callback(self._discard_bg_task)

    async def _shutdown_runtime(self) -> None:
        for run_id in list(self.gateway_run_wait_notice_tasks.keys()):
            self._stop_wait_notice(run_id)
        pending_tasks = [task for task in self.bg_tasks if not task.done()]
        for task in pending_tasks:
            task.cancel()
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        self.bg_tasks.clear()
        try:
            await self._close_shared_gateway()
        except Exception as exc:  # noqa: BLE001
            logging.warning("runtime.shutdown stage=close_gateway status=failed err=%s", exc)
        media_session = self.media_session
        self.media_session = None
        if media_session is not None and not media_session.closed:
            try:
                await media_session.close()
            except Exception as exc:  # noqa: BLE001
                logging.warning("runtime.shutdown stage=close_media_session status=failed err=%s", exc)

    async def _satori_ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            while not ws.closed:
                await asyncio.sleep(10)
                if ws.closed:
                    break
                await ws.send_str(json.dumps({"op": 1}, ensure_ascii=False))
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001
            logging.warning("ws.ping status=stopped err=%s", exc)

    @staticmethod
    def _satori_parse_tag_attrs(attr_text: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for matched in re.finditer(r'([:\w-]+)\s*=\s*(".*?"|\'.*?\'|[^\s"\'=<>`]+)', attr_text):
            key = matched.group(1).strip()
            raw = matched.group(2).strip()
            if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
                raw = raw[1:-1]
            out[key] = unescape(raw)
        return out

    @classmethod
    def _satori_content_to_segments(cls, content: str) -> list[dict[str, Any]]:
        text = content or ""
        out: list[dict[str, Any]] = []
        tag_pattern = re.compile(r"<(/?)([A-Za-z][\w-]*)([^>]*)>")
        last = 0
        quote_depth = 0
        for matched in tag_pattern.finditer(text):
            plain = unescape(text[last : matched.start()])
            if plain and quote_depth == 0:
                out.append({"type": "text", "text": plain})
            last = matched.end()
            raw_tag = matched.group(0)
            self_closing = raw_tag.rstrip().endswith("/>")
            is_close = matched.group(1) == "/"
            tag_name = matched.group(2).lower()
            if tag_name == "quote":
                if is_close:
                    if quote_depth > 0:
                        quote_depth -= 1
                    continue
                attrs = cls._satori_parse_tag_attrs(matched.group(3) or "")
                msg_id = attrs.get("id") or ""
                if msg_id and quote_depth == 0:
                    out.append({"type": "quote", "id": msg_id})
                if not self_closing:
                    quote_depth += 1
                continue
            if tag_name in {"message", "author"}:
                if quote_depth == 0:
                    out.append({"type": "text", "text": raw_tag})
                continue
            if is_close or quote_depth > 0:
                continue
            attrs = cls._satori_parse_tag_attrs(matched.group(3) or "")
            if tag_name == "at":
                seg: dict[str, Any] = {"type": "at"}
                at_id = str(attrs.get("id") or attrs.get("qq") or "").strip()
                at_type = str(attrs.get("type") or "").strip()
                if at_id:
                    seg["id"] = at_id
                if at_type:
                    seg["at_type"] = at_type
                if len(seg) > 1:
                    out.append(seg)
                continue
            if tag_name in {"img", "image"}:
                src = attrs.get("src") or attrs.get("url") or ""
                if src:
                    out.append({"type": "img", "src": src})
                continue
        tail = unescape(text[last:])
        if tail and quote_depth == 0:
            out.append({"type": "text", "text": tail})
        return out

    def _update_satori_default_route(self, payload_body: dict[str, Any]) -> None:
        logins = payload_body.get("logins")
        if not isinstance(logins, list):
            return
        for raw_login in logins:
            if not isinstance(raw_login, dict):
                continue
            platform = str(raw_login.get("platform") or "").strip()
            user_raw = raw_login.get("user")
            user = user_raw if isinstance(user_raw, dict) else {}
            self_id = str(
                raw_login.get("self_id")
                or raw_login.get("selfId")
                or user.get("id")
                or self.cfg.satori_self_id
                or ""
            ).strip()
            if not platform:
                platform = self.cfg.satori_platform.strip()
            if platform and self_id:
                self.satori_default_route = {"platform": platform, "self_id": self_id}
                return

    def _convert_satori_event(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        event_type = str(payload.get("type") or "").strip().lower()
        if event_type != "message-created":
            return None
        message_raw = payload.get("message")
        message = message_raw if isinstance(message_raw, dict) else {}
        channel_raw = payload.get("channel") or message.get("channel")
        channel = channel_raw if isinstance(channel_raw, dict) else {}
        user_raw = payload.get("user") or message.get("user")
        user = user_raw if isinstance(user_raw, dict) else {}
        member_raw = payload.get("member") or message.get("member")
        member = member_raw if isinstance(member_raw, dict) else {}
        guild_raw = payload.get("guild") or message.get("guild")
        guild = guild_raw if isinstance(guild_raw, dict) else {}
        login_raw = payload.get("login")
        login = login_raw if isinstance(login_raw, dict) else {}
        login_user_raw = login.get("user")
        login_user = login_user_raw if isinstance(login_user_raw, dict) else {}

        channel_type_raw = channel.get("type")
        channel_type = str(channel_type_raw or "").strip().lower()

        user_id = str(
            user.get("id")
            or user.get("user_id")
            or member.get("id")
            or member.get("user_id")
            or ""
        ).strip()
        if not user_id:
            user_id = str(
                (member.get("user") or {}).get("id") if isinstance(member.get("user"), dict) else ""
            ).strip()
        sender_name = str(
            member.get("nick")
            or member.get("name")
            or user.get("name")
            or user.get("nick")
            or user_id
            or "unknown"
        ).strip()

        message_id = str(
            message.get("id")
            or message.get("message_id")
            or payload.get("id")
            or ""
        ).strip()
        content = str(message.get("content") or "").strip()
        segments = self._satori_content_to_segments(content)

        ts_raw = payload.get("timestamp") or message.get("created_at") or time.time() * 1000
        try:
            ts_value = float(ts_raw)
            # Satori timestamp is ms.
            ts_sec = int(ts_value / 1000) if ts_value > 10_000_000_000 else int(ts_value)
        except (TypeError, ValueError):
            ts_sec = int(time.time())

        platform = str(
            payload.get("platform")
            or login.get("platform")
            or self.satori_default_route.get("platform")
            or self.cfg.satori_platform
            or ""
        ).strip()
        self_id = str(
            payload.get("self_id")
            or payload.get("selfId")
            or login.get("self_id")
            or login.get("selfId")
            or login_user.get("id")
            or self.satori_default_route.get("self_id")
            or self.cfg.satori_self_id
            or ""
        ).strip()
        channel_id = str(
            channel.get("id")
            or channel.get("channel_id")
            or message.get("channel_id")
            or payload.get("channel_id")
            or ""
        ).strip()
        if channel_type in {"direct", "private", "dm"}:
            message_type = "private"
        elif channel_type in {"text", "group", "guild"}:
            message_type = "group"
        else:
            # 部分实现不提供 channel.type，按 channel_id 形态兜底识别私聊。
            lowered_channel_id = channel_id.lower()
            message_type = (
                "private"
                if lowered_channel_id.startswith("private:")
                or lowered_channel_id.startswith("dm:")
                or lowered_channel_id.startswith("direct:")
                else "group"
            )
        if not channel_id or not user_id:
            logging.warning(
                "evt.convert status=dropped reason=missing_channel_or_user keys=%s",
                sorted(payload.keys()),
            )
            return None
        group_id = str(guild.get("id") or guild.get("guild_id") or channel_id).strip()
        route = {
            "platform": platform,
            "self_id": self_id,
            "channel_id": channel_id,
            "guild_id": str(guild.get("id") or guild.get("guild_id") or "").strip(),
            "user_id": user_id,
        }

        return {
            "post_type": "message",
            "message_type": message_type,
            "self_id": self_id,
            "user_id": user_id,
            "group_id": group_id if message_type == "group" else None,
            "time": ts_sec,
            "message_id": message_id or f"satori:{channel_id}:{ts_sec}",
            "message_seq": 0,
            "sender": {
                "user_id": user_id,
                "nickname": sender_name,
                "card": sender_name if message_type == "group" else "",
            },
            "raw_message": content,
            "message": segments,
            "message_format": "satori",
            "_satori_route": route,
            "_satori_event": payload,
        }

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.cfg.request_timeout_sec)
        delay = max(1, self.cfg.reconnect_delay_sec)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            proxy_url = self.cfg.bridge_proxy_url.strip()
            if proxy_url:
                if ProxyConnector is None:
                    logging.warning(
                        "media.proxy status=disabled reason=missing_aiohttp_socks proxy=%s",
                        proxy_url,
                    )
                else:
                    self.media_session = aiohttp.ClientSession(
                        timeout=timeout,
                        connector=ProxyConnector.from_url(proxy_url),
                    )
                    logging.info("media.proxy status=enabled proxy=%s", proxy_url)
            try:
                while True:
                    try:
                        logging.info("ws stage=connect url=%s", self.cfg.satori_ws_url)
                        async with session.ws_connect(
                            self.cfg.satori_ws_url,
                            headers=self._onebot_headers(),
                            heartbeat=30,
                        ) as ws:
                            logging.info("ws stage=connected")
                            delay = max(1, self.cfg.reconnect_delay_sec)
                            identify_body: dict[str, Any] = {}
                            token = self.cfg.satori_token.strip()
                            if token:
                                identify_body["token"] = token
                            if self.satori_last_sn > 0:
                                identify_body["sn"] = self.satori_last_sn
                            # 按 Satori 协议：建立连接后主动 IDENTIFY，再接收 READY/EVENT。
                            await ws.send_str(
                                json.dumps({"op": 3, "body": identify_body}, ensure_ascii=False)
                            )
                            logging.info(
                                "ws stage=identify_sent token=%s sn=%s",
                                "set" if bool(token) else "empty",
                                identify_body.get("sn"),
                            )
                            ping_task = asyncio.create_task(self._satori_ping_loop(ws))
                            try:
                                async for msg in ws:
                                    if msg.type == aiohttp.WSMsgType.TEXT:
                                        try:
                                            parsed = json.loads(msg.data)
                                        except json.JSONDecodeError:
                                            logging.warning("ws packet=non_json data=%s", msg.data[:200])
                                            continue
                                        if not isinstance(parsed, dict):
                                            logging.warning("ws packet=non_object data=%s", msg.data[:200])
                                            continue
                                        op_raw = parsed.get("op")
                                        op: int | None
                                        try:
                                            op = int(op_raw) if op_raw is not None else None
                                        except (TypeError, ValueError):
                                            op = None
                                        body_raw = parsed.get("body")
                                        body = body_raw if isinstance(body_raw, dict) else {}

                                        # 兼容某些实现直接推送 Event（无 op/body 包裹）。
                                        if op is None and isinstance(parsed.get("type"), str):
                                            op = 0
                                            body = parsed

                                        # READY：更新默认登录路由信息。
                                        if op == 4:
                                            self._update_satori_default_route(body)
                                            logging.info("ws event=ready")
                                            continue
                                        # EVENT：处理消息事件，并更新 sn 以便断线续传。
                                        if op == 0:
                                            evt_type = str(body.get("type") or "").strip()
                                            if evt_type:
                                                logging.info(
                                                    "ws event=recv type=%s sn=%s",
                                                    evt_type,
                                                    body.get("sn"),
                                                )
                                            # Debug raw inbound event body before any conversion/handling.
                                            try:
                                                raw_event = json.dumps(
                                                    body,
                                                    ensure_ascii=False,
                                                    separators=(",", ":"),
                                                )
                                            except Exception:  # noqa: BLE001
                                                raw_event = str(body)
                                            if len(raw_event) > 12000:
                                                raw_event = raw_event[:11999].rstrip() + "…"
                                            logging.info("ws event=raw payload=%s", raw_event)
                                            sn_value = body.get("sn")
                                            try:
                                                if sn_value is not None:
                                                    self.satori_last_sn = int(sn_value)
                                            except (TypeError, ValueError):
                                                pass
                                            event = self._convert_satori_event(body)
                                            if event is None:
                                                if evt_type:
                                                    logging.info(
                                                        "ws event=ignored_by_parser type=%s sn=%s",
                                                        evt_type,
                                                        body.get("sn"),
                                                    )
                                                continue
                                            try:
                                                await self._handle_onebot_event(event)
                                            except Exception as exc:  # noqa: BLE001
                                                logging.exception(
                                                    "ws event=handle_failed_ignored type=%s sn=%s err=%s",
                                                    evt_type or "unknown",
                                                    body.get("sn"),
                                                    exc,
                                                )
                                            continue
                                        # PONG/其它 op：当前无需处理。
                                        if op not in {0, 1, 2, 4, 5, None}:
                                            logging.info("ws packet=ignored op=%s keys=%s", op_raw, sorted(parsed.keys()))
                                        continue
                                    if msg.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                                        break
                            finally:
                                ping_task.cancel()
                                try:
                                    await ping_task
                                except asyncio.CancelledError:
                                    pass
                    except asyncio.CancelledError:
                        logging.info("ws stage=stopping reason=shutdown_signal")
                        break
                    except Exception as exc:  # noqa: BLE001
                        logging.exception("ws stage=error err=%s", exc)

                    logging.info("ws stage=reconnect_wait delay_sec=%s", delay)
                    try:
                        await asyncio.sleep(delay)
                    except asyncio.CancelledError:
                        logging.info("ws stage=stopping reason=shutdown_during_reconnect_wait")
                        break
                    delay = min(delay * 2, max(delay, self.cfg.max_reconnect_delay_sec))
            finally:
                await self._shutdown_runtime()
                self.session = None
                self.media_session = None
