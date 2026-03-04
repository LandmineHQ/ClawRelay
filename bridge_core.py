import asyncio
import json
import logging
import os
import re
import secrets
import time
from collections import defaultdict, deque
from typing import Any

import aiohttp

from bridge_config import Config
from bridge_models import MessageImage, PairingRecord, PairingRequest, PendingObservation
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
        self._load_ops_store()
        self._load_pairing_store()

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
    def _pairing_charset() -> str:
        # Exclude ambiguous characters: 0/O and 1/I.
        return "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"

    @staticmethod
    def _normalize_user_id(value: Any) -> str:
        return str(value or "").strip()

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
            logging.info("Loaded OP users: %s from %s", len(self.op_users), path)
            self._save_ops_store()
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to load OP store %s: %s", path, exc)
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
            logging.warning("Failed to save OP store %s: %s", path, exc)

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
                logging.info("Loaded pairing records: %s from %s", len(records), path)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to load pairing store %s: %s", path, exc)

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
            logging.warning("Failed to save pairing store %s: %s", path, exc)

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
        cq_matched = re.search(
            r"\[cq:at,[^\]]*qq=(\d{5,20})[^\]]*\]",
            command or "",
            flags=re.IGNORECASE,
        )
        if cq_matched:
            return cq_matched.group(1)
        matched = re.search(r"\d{5,20}", command or "")
        return matched.group(0) if matched else ""

    @staticmethod
    def _is_admin_command(command: str) -> bool:
        normalized = OpenClawOneBotBridge._normalize_command_text(command)
        if re.fullmatch(r"/pair [a-z0-9]{4,12}", normalized):
            return True
        if re.fullmatch(r"/op (?:list|add \d{5,20}|del \d{5,20})", normalized):
            return True
        if normalized == "/unpair":
            return True
        return False

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

    async def _handle_local_command(
        self, event: dict[str, Any], session_key: str, command: str
    ) -> None:
        cmd = self._normalize_command_text(command)
        if cmd in {"/help", "help"}:
            await self._send_onebot_reply(event, self._help_text())
            return

        if cmd in {"/new", "new"}:
            if not await self._ensure_target_pairing(event, session_key):
                return
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
            return

        if re.fullmatch(r"/pair [a-z0-9]{4,12}", cmd):
            await self._handle_pair_command(event, command)
            return

        if re.fullmatch(r"/op (?:list|add \d{5,20}|del \d{5,20})", cmd):
            await self._handle_op_command(event, command)
            return

        if cmd in {"/unpair", "unpair"}:
            await self._handle_unpair_command(event, session_key)

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
        if local_cmd and (should_reply or self._is_admin_command(local_cmd)):
            await self._handle_local_command(event, session_key, local_cmd)
            return

        # Private chat: send user content directly to OpenClaw without local prompt building.
        if event.get("message_type") == "private":
            if not should_reply:
                return
            if not await self._ensure_target_pairing(event, session_key):
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
        if not await self._ensure_target_pairing(event, session_key):
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
