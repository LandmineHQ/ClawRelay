import base64
import json
import logging
import mimetypes
import re
from collections import deque
from datetime import datetime
from html import escape
from typing import Any
from urllib.parse import urlparse

import aiohttp

from bridge_config import Config
from bridge_models import MessageImage, ParsedMessage, PendingObservation


class OneBotMixin:
    cfg: Config
    session: aiohttp.ClientSession | None
    pending_context: dict[str, deque[PendingObservation]]
    seen_ids: deque[str]
    seen_set: set[str]
    cleared_processing_markers: deque[str]
    cleared_processing_marker_set: set[str]
    satori_reaction_disabled_until: float
    satori_reaction_disable_reason: str

    def _onebot_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = self.cfg.satori_token.strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _satori_api_base(self) -> str:
        base = self.cfg.satori_http_base.strip().rstrip("/")
        if not base.endswith("/v1"):
            base = f"{base}/v1"
        return base

    def _satori_route_from_event(self, event: dict[str, Any]) -> dict[str, str] | None:
        raw = event.get("_satori_route")
        if isinstance(raw, dict):
            channel_id = str(raw.get("channel_id") or "").strip()
            platform = str(raw.get("platform") or self.cfg.satori_platform or "").strip()
            self_id = str(raw.get("self_id") or self.cfg.satori_self_id or "").strip()
            if channel_id and platform and self_id:
                return {
                    "channel_id": channel_id,
                    "platform": platform,
                    "self_id": self_id,
                }
        return None

    async def _satori_action(
        self,
        action: str,
        payload: dict[str, Any],
        route: dict[str, str] | None,
        timeout_sec: float | None = None,
    ) -> dict[str, Any] | None:
        assert self.session is not None
        if route is None:
            logging.warning("Satori action %s missing route", action)
            return None
        headers = self._onebot_headers()
        headers["Satori-Platform"] = route["platform"]
        headers["Satori-User-ID"] = route["self_id"]
        url = f"{self._satori_api_base()}/{action}"
        try:
            async with self.session.post(
                url,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout_sec) if timeout_sec else None,
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    if action == "message.get":
                        logging.info(
                            "Satori action %s HTTP %s (will try fallback): %s",
                            action,
                            resp.status,
                            text[:200],
                        )
                    else:
                        logging.warning("Satori action %s HTTP %s: %s", action, resp.status, text[:200])
                    return None
                if not text.strip():
                    return {}
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    return {}
        except Exception as exc:  # noqa: BLE001
            if action == "message.get":
                logging.info("Satori action %s failed (will try fallback): %s", action, exc)
            else:
                logging.warning("Satori action %s failed: %s", action, exc)
            return None
        if isinstance(parsed, dict):
            return parsed
        return {}

    def _build_session_key(self, event: dict[str, Any]) -> str:
        if event.get("message_type") == "private":
            user_id = str(event.get("user_id", "unknown"))
            return f"{self.cfg.openclaw_session_prefix}:private:{user_id}"
        return f"{self.cfg.openclaw_session_prefix}:group:{event.get('group_id')}"

    def _get_self_qq(self, event: dict[str, Any]) -> str:
        if self.cfg.self_qq:
            return self.cfg.self_qq
        self_id = event.get("self_id")
        return str(self_id) if self_id is not None else ""

    @staticmethod
    def _mention_tag(target_id: str, *, mention_type: str = "") -> str:
        mention_type_value = mention_type.strip().lower()
        if mention_type_value == "all":
            return '<at type="all"/>'
        target_value = target_id.strip()
        if not target_value:
            return ""
        return f'<at id="{escape(target_value, quote=True)}"/>'

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
    def _merge_str_list(primary: list[str], secondary: list[str]) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for value in [*primary, *secondary]:
            key = str(value).strip()
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(key)
        return merged

    def _image_tag(self, img: MessageImage) -> str:
        src = img.url.strip() or img.file.strip()
        if not src:
            return "<img/>"
        return f'<img src="{escape(src, quote=True)}"/>'

    def _extract_message_from_payload(
        self, payload: Any, self_qq: str
    ) -> ParsedMessage:
        mentioned = False
        images: list[MessageImage] = []
        reply_ids: list[str] = []
        forward_ids: list[str] = []

        if isinstance(payload, list):
            text_parts: list[str] = []
            for seg in payload:
                if not isinstance(seg, dict):
                    continue
                data_raw = seg.get("data")
                data = data_raw if isinstance(data_raw, dict) else {}
                seg_type = str(seg.get("type") or data.get("type") or "").strip().lower()

                def _read(*keys: str) -> str:
                    for key in keys:
                        value = seg.get(key)
                        if value is not None:
                            return str(value)
                        value = data.get(key)
                        if value is not None:
                            return str(value)
                    return ""

                if seg_type == "text":
                    text_value = _read("text", "content", "value", "name").strip()
                    if text_value:
                        text_parts.append(text_value)
                elif seg_type == "at":
                    at_type = str(
                        data.get("at_type")
                        or data.get("mention_type")
                        or data.get("type")
                        or seg.get("at_type")
                        or seg.get("mention_type")
                        or ""
                    ).strip().lower()
                    mention_id = _read("id", "qq", "user_id", "userId").strip()
                    if at_type == "all":
                        text_parts.append(self._mention_tag("", mention_type="all"))
                        continue
                    if self_qq and mention_id == self_qq:
                        mentioned = True
                    mention_text = self._mention_tag(mention_id)
                    if mention_text:
                        text_parts.append(mention_text)
                elif seg_type in {"img", "image"}:
                    url = _read("src", "url").strip()
                    file = _read("file", "path").strip()
                    img = MessageImage(url=url, file=file)
                    images.append(img)
                    text_parts.append(self._image_tag(img))
                elif seg_type in {"reply", "quote"}:
                    ref_id = _read("id", "message_id", "messageId").strip()
                    if ref_id:
                        reply_ids.append(ref_id)
                        text_parts.append(f'<quote id="{escape(ref_id, quote=True)}"/>')
                    else:
                        text_parts.append("<quote/>")
                elif seg_type in {"forward", "longmsg"}:
                    fwd_id = _read("id", "message_id", "messageId", "resid", "res_id").strip()
                    if fwd_id:
                        forward_ids.append(fwd_id)
                        text_parts.append(f'<message id="{escape(fwd_id, quote=True)}" forward/>')
                    else:
                        text_parts.append("<message forward/>")
            text = re.sub(r"\s+", " ", "".join(text_parts)).strip()
            return ParsedMessage(
                text=text,
                mentioned=mentioned,
                images=images,
                reply_ids=reply_ids,
                forward_ids=forward_ids,
            )

        if isinstance(payload, str):
            parser = getattr(self, "_satori_content_to_segments", None)
            if callable(parser):
                parsed_segments = parser(payload)
                if isinstance(parsed_segments, list) and parsed_segments:
                    return self._extract_message_from_payload(parsed_segments, self_qq)

            text = re.sub(r"\s+", " ", payload).strip()
            return ParsedMessage(text=text, mentioned=False, images=[], reply_ids=[], forward_ids=[])

        return ParsedMessage(text="", mentioned=False, images=[], reply_ids=[], forward_ids=[])

    def _extract_message(self, event: dict[str, Any]) -> ParsedMessage:
        self_qq = self._get_self_qq(event)
        parsed_main = self._extract_message_from_payload(event.get("message"), self_qq)
        parsed_raw = self._extract_message_from_payload(event.get("raw_message"), self_qq)
        merged_images = self._merge_images(parsed_main.images, parsed_raw.images)
        merged_reply_ids = self._merge_str_list(parsed_main.reply_ids, parsed_raw.reply_ids)
        merged_forward_ids = self._merge_str_list(parsed_main.forward_ids, parsed_raw.forward_ids)
        text = parsed_main.text or parsed_raw.text
        return ParsedMessage(
            text=text,
            mentioned=parsed_main.mentioned or parsed_raw.mentioned,
            images=merged_images,
            reply_ids=merged_reply_ids,
            forward_ids=merged_forward_ids,
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
        sender_name: str, sender_qq: str, normalized_text: str, images: list[MessageImage], ts: float
    ) -> str:
        display = normalized_text or "（无文本）"
        # Keep a compact fallback line for logs/debug.
        time_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
        return f"{time_str} {sender_name}({sender_qq}): {display}"

    @staticmethod
    def _format_observation_item(obs: PendingObservation) -> str:
        display = obs.normalized_text or "（无文本）"
        time_str = datetime.fromtimestamp(obs.ts).strftime("%H:%M:%S")
        reply_ids = ", ".join(obs.reply_ids) if obs.reply_ids else "null"
        forward_ids = ", ".join(obs.forward_ids) if obs.forward_ids else "null"
        lines = [
            f"- time: {time_str}",
            f"  user_name: {obs.sender_name or 'unknown'}",
            f"  user_id: {obs.sender_id or 'unknown'}",
            f"  content: {display}",
            f"  reply_ids: {reply_ids}",
            f"  forward_ids: {forward_ids}",
        ]
        if obs.reply_blocks:
            lines.append("  reply_context:")
            for idx, block in enumerate(obs.reply_blocks, start=1):
                lines.append(f"    - block_{idx}:")
                lines.extend([f"      {piece}" for piece in block.splitlines()])
        if obs.forward_blocks:
            lines.append("  forward_context:")
            for idx, block in enumerate(obs.forward_blocks, start=1):
                lines.append(f"    - block_{idx}:")
                lines.extend([f"      {piece}" for piece in block.splitlines()])
        return "\n".join(lines)

    def _record_observation(
        self, event: dict[str, Any], session_key: str, parsed: ParsedMessage
    ) -> None:
        sender_name = self._sender_name(event)
        sender_qq = str(event.get("user_id", "unknown"))
        normalized_text = parsed.text.strip()
        images = parsed.images
        ts_raw = event.get("time")
        try:
            ts = float(ts_raw) if ts_raw is not None else datetime.now().timestamp()
        except (TypeError, ValueError):
            ts = datetime.now().timestamp()
        line = self._format_observation_line(sender_name, sender_qq, normalized_text, images, ts)
        self.pending_context[session_key].append(
            PendingObservation(
                line=line,
                normalized_text=normalized_text,
                images=list(images),
                ts=ts,
                sender_name=sender_name,
                sender_id=sender_qq,
                reply_ids=list(parsed.reply_ids),
                forward_ids=list(parsed.forward_ids),
                reply_blocks=list(parsed.reply_blocks),
                forward_blocks=list(parsed.forward_blocks),
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

        mentioned = parsed.mentioned
        effective_text = raw
        if not mentioned:
            effective_text, mentioned = self._strip_leading_plain_mention(raw)

        if not effective_text and not parsed.images:
            return False, ""

        prefix = self.cfg.group_prefix.strip()
        if prefix and effective_text.startswith(prefix):
            stripped = effective_text[len(prefix) :].strip()
            if stripped or parsed.images:
                return True, stripped

        if self.cfg.group_require_at and not mentioned:
            return False, raw

        return True, effective_text

    @staticmethod
    def _strip_leading_plain_mention(text: str) -> tuple[str, bool]:
        # Some clients send plain text like "@机器人 /new" instead of structured mention segments.
        matched = re.match(r"^\s*@[^ \t\r\n]+\s*", text)
        if not matched:
            return text, False
        return text[matched.end() :].strip(), True

    @staticmethod
    def _extract_op_target_user_id(arg: str) -> str:
        raw = (arg or "").strip()
        if re.fullmatch(r"\d{5,20}", raw):
            return raw
        at_match = re.fullmatch(
            r"<at\b[^>]*\bid\s*=\s*(?:\"|')?(\d{5,20})(?:\"|')?[^>]*\/?>",
            raw,
            flags=re.IGNORECASE,
        )
        if at_match:
            return at_match.group(1)
        return ""

    @staticmethod
    def _normalize_local_command_text(text: str) -> str:
        normalized = (text or "").strip().lower()
        normalized = normalized.replace("／", "/")
        if normalized.startswith("\\"):
            normalized = "/" + normalized[1:]
        return re.sub(r"\s+", " ", normalized)

    @staticmethod
    def _build_op_command(action: str, raw_target: str) -> str | None:
        target_uid = OneBotMixin._extract_op_target_user_id(raw_target)
        if not target_uid:
            return None
        return f"op {action} {target_uid}"

    def _detect_local_command(self, text: str, images: list[MessageImage]) -> str | None:
        if images:
            return None
        stripped = (text or "").strip()
        while True:
            next_value = re.sub(
                r"^\s*<at\b[^>]*?/>\s*",
                "",
                stripped,
                count=1,
                flags=re.IGNORECASE,
            )
            if next_value == stripped:
                break
            stripped = next_value
        stripped, _ = OneBotMixin._strip_leading_plain_mention(stripped)
        normalized = self._normalize_local_command_text(stripped)

        code_len = max(4, min(12, int(self.cfg.pairing_code_len)))
        command_specs: list[tuple[str, Any]] = [
            (r"/?new", lambda _m: "new"),
            (r"/?help", lambda _m: "help"),
            (
                rf"/?(?:pair|pairing)\s+([a-z0-9]{{{code_len}}})",
                lambda m: f"pair {m.group(1).upper()}",
            ),
            (r"/?unpair", lambda _m: "unpair"),
            (r"/?op(?:\s+(?:list|ls))?", lambda _m: "op list"),
            (r"/?op\s+(?:add|\+)\s+(.+)", lambda m: OneBotMixin._build_op_command("add", m.group(1))),
            (
                r"/?op\s+(?:del|remove|rm|-)\s+(.+)",
                lambda m: OneBotMixin._build_op_command("del", m.group(1)),
            ),
        ]
        for pattern, builder in command_specs:
            matched = re.fullmatch(pattern, normalized)
            if not matched:
                continue
            built = builder(matched)
            if built:
                return str(built)

        return None

    @staticmethod
    def _help_text() -> str:
        return (
            "可用指令（以下 `/` 前缀均可省略）：\n"
            "1) `/new`：重置当前会话上下文（开启新会话）\n"
            "2) `/pair <配对码>`：OP 审批当前待配对会话（私聊/群聊）\n"
            "3) `/unpair`：OP 移除当前会话配对（私聊/群聊）\n"
            "4) `/op list|add|del`：OP 列表管理（仅 OP 可执行）\n"
            "5) `/help`：查看指令说明"
        )

    def _build_prompt_from_pending(
        self,
        pending: list[PendingObservation],
        latest_line: str,
        include_guidance: bool = True,
        *,
        bot_user_id: str = "",
    ) -> str:
        recent = pending[-max(1, self.cfg.context_flush_limit) :]
        # 触发消息会先写入 pending，这里将历史与当前待回复消息拆开，避免重复占用 token。
        history = recent[:-1] if len(recent) > 1 else []
        latest_obs = recent[-1] if recent else None
        current_sender_id = latest_obs.sender_id if latest_obs is not None else "unknown"
        if latest_obs is not None:
            current_block = self._format_observation_item(latest_obs)
        else:
            current_block = (
                "- time: unknown\n"
                "  user_name: unknown\n"
                "  user_id: unknown\n"
                f"  content: {latest_line.strip() or '（无文本）'}\n"
                "  reply_ids: null\n"
                "  forward_ids: null"
            )

        history_lines = [self._format_observation_item(item) for item in history]
        history_block = "\n\n".join(history_lines) if history_lines else "（无）"
        bot_id = (bot_user_id or "").strip() or "unknown"

        rule_block = (
            "规则：\n"
            "1) user_name 可能包含数字（例如“我是1354987”），这不是 QQ 号。\n"
            "2) 只能使用 user_id 作为 QQ 号；如需@，仅可输出 `<at id=\"<user_id>\"/>`。\n"
            "3) 默认@发送者，除非被要求不@发送者。\n"
            f"4) 当前待回复消息发送者 user_id: {current_sender_id}\n"
            f"5) 当前机器人 user_id: {bot_id}；当 `<at id=\"{bot_id}\"/>` 出现时，表示在@你。\n"
            "6) 默认使用简洁纯文本回复；仅当用户明确要求 Markdown 时，才使用 Markdown。"
        )
        if not include_guidance:
            return (
                "历史记录（不含当前消息，每条分行结构化字段）：\n"
                + "```text\n"
                + history_block
                + "\n```\n"
                + "当前待回复消息：\n"
                + "```text\n"
                + current_block
                + "\n```\n"
                + rule_block
            )
        return (
            "你正在 QQ 群聊中对话，请结合历史继续当前话题并直接回复用户。\n"
            + "历史记录（不含当前消息，按时间顺序，每条分行结构化字段）：\n"
            + "```text\n"
            + history_block
            + "\n```\n"
            + "当前待回复消息：\n"
            + "```text\n"
            + current_block
            + "\n```\n"
            + rule_block
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
        maxlen = self.seen_ids.maxlen
        if maxlen is None:
            return True
        while len(self.seen_ids) > maxlen:
            old = self.seen_ids.popleft()
            self.seen_set.discard(old)
        return True

    async def _send_onebot_reply(self, event: dict[str, Any], text: str) -> None:
        assert self.session is not None

        route = self._satori_route_from_event(event)
        if route is None:
            raise RuntimeError("Missing Satori route in reply event")
        outgoing = (text or "").strip()
        if (
            event.get("message_type") == "group"
            and self.cfg.group_reply_at_sender
            and event.get("user_id") is not None
        ):
            uid = str(event.get("user_id") or "").strip()
            if uid and not self._contains_satori_at_user(outgoing, uid):
                outgoing = f'<at id="{uid}"/> {outgoing}'
        payload = {
            "channel_id": route["channel_id"],
            "content": outgoing,
        }
        result = await self._satori_action("message.create", payload, route, timeout_sec=12)
        if result is None:
            raise RuntimeError("Satori message.create failed")

    @staticmethod
    def _contains_satori_at_user(text: str, user_id: str) -> bool:
        uid = user_id.strip()
        if not uid:
            return False
        pattern = re.compile(
            rf"<at\b[^>]*\bid\s*=\s*(?:\"|')?{re.escape(uid)}(?:\"|')?[^>]*\/?>",
            flags=re.IGNORECASE,
        )
        return bool(pattern.search(text or ""))

    async def _mark_processing_emoji(self, event: dict[str, Any]) -> dict[str, Any] | None:
        now = datetime.now().timestamp()
        if self.satori_reaction_disabled_until > now:
            return None
        marker = self._processing_marker_from_event(event)
        if marker is None:
            return None
        payload = {
            "channel_id": marker["route"]["channel_id"],
            "message_id": marker["message_id"],
            "emoji_id": marker["emoji_id"],
        }
        result = await self._satori_action("reaction.create", payload, marker["route"], timeout_sec=8)
        if result is None:
            self.satori_reaction_disabled_until = now + 600
            self.satori_reaction_disable_reason = "reaction.create failed (platform not supported or broken)"
            logging.warning(
                "Failed to set processing emoji via reaction.create: message_id=%s emoji_id=%s; disable reaction marking for 600s",
                marker.get("message_id"),
                marker.get("emoji_id"),
            )
            return None
        return marker

    async def _clear_processing_emoji(self, marker: dict[str, Any] | None) -> None:
        if marker is None:
            return
        message_id = marker.get("message_id")
        emoji_id = marker.get("emoji_id")
        route = marker.get("route")
        if message_id is None or emoji_id is None or not isinstance(route, dict):
            return
        marker_key = f"{message_id}:{emoji_id}"
        if marker_key in self.cleared_processing_marker_set:
            return
        payload = {
            "channel_id": route.get("channel_id"),
            "message_id": message_id,
            "emoji_id": str(emoji_id),
        }
        result = await self._satori_action("reaction.delete", payload, route, timeout_sec=8)
        if result is not None:
            self.cleared_processing_marker_set.add(marker_key)
            self.cleared_processing_markers.append(marker_key)
            maxlen = self.cleared_processing_markers.maxlen
            if maxlen is not None:
                while len(self.cleared_processing_markers) > maxlen:
                    old = self.cleared_processing_markers.popleft()
                    self.cleared_processing_marker_set.discard(old)
            return

        logging.warning(
            "Failed to clear processing emoji via reaction.delete: message_id=%s emoji_id=%s",
            message_id,
            emoji_id,
        )
        # 部分实现在重复状态下会返回 failed，但视觉结果可能已生效，这里仍做去重以免重复刷接口。
        self.cleared_processing_marker_set.add(marker_key)
        self.cleared_processing_markers.append(marker_key)
        maxlen = self.cleared_processing_markers.maxlen
        if maxlen is not None:
            while len(self.cleared_processing_markers) > maxlen:
                old = self.cleared_processing_markers.popleft()
                self.cleared_processing_marker_set.discard(old)

    def _processing_marker_from_event(self, event: dict[str, Any]) -> dict[str, Any] | None:
        if event.get("message_type") != "group":
            return None
        message_id = event.get("message_id")
        if message_id is None:
            return None
        route = self._satori_route_from_event(event)
        if route is None:
            return None
        emoji_id = int(self.cfg.satori_processing_emoji_id)
        if emoji_id <= 0:
            emoji_id = 30
        return {
            "message_id": message_id,
            # LLOneBot reaction.create/delete 需要 emoji_id（字符串）。
            "emoji_id": str(emoji_id),
            "route": route,
        }

    @staticmethod
    def _compact_text(text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _truncate_text(text: str, max_len: int) -> str:
        raw = text.strip()
        if len(raw) <= max_len:
            return raw
        return raw[: max(1, max_len - 1)].rstrip() + "…"

    def _sender_name_id_from_message_record(self, record: dict[str, Any]) -> tuple[str, str]:
        sender_raw = record.get("sender")
        sender = sender_raw if isinstance(sender_raw, dict) else {}
        sender_id = str(
            record.get("user_id")
            or sender.get("user_id")
            or sender.get("qq")
            or sender.get("id")
            or "unknown"
        ).strip()
        sender_name = str(sender.get("card") or sender.get("nickname") or sender_id).strip() or sender_id
        return sender_name, sender_id or "unknown"

    def _parse_message_record_payload(self, record: dict[str, Any], self_qq: str) -> ParsedMessage:
        parsed_main = self._extract_message_from_payload(record.get("message"), self_qq)
        parsed_raw = self._extract_message_from_payload(record.get("raw_message"), self_qq)
        return ParsedMessage(
            text=parsed_main.text or parsed_raw.text,
            mentioned=parsed_main.mentioned or parsed_raw.mentioned,
            images=self._merge_images(parsed_main.images, parsed_raw.images),
            reply_ids=self._merge_str_list(parsed_main.reply_ids, parsed_raw.reply_ids),
            forward_ids=self._merge_str_list(parsed_main.forward_ids, parsed_raw.forward_ids),
        )

    def _build_reply_context_block(self, record: dict[str, Any], self_qq: str) -> tuple[str, list[MessageImage]]:
        sender_name, sender_id = self._sender_name_id_from_message_record(record)
        parsed = self._parse_message_record_payload(record, self_qq)
        content = self._truncate_text(self._compact_text(parsed.text or "（无文本）"), 300)
        block = (
            "[引用消息]\n"
            f"- user_name: {sender_name}\n"
            f"- user_id: {sender_id}\n"
            f"- content: {content}"
        )
        return block, parsed.images

    def _build_forward_context_block(
        self, forward_id: str, payload: dict[str, Any], self_qq: str
    ) -> tuple[str, list[MessageImage]]:
        nodes_raw = payload.get("messages")
        nodes = nodes_raw if isinstance(nodes_raw, list) else []
        out_images: list[MessageImage] = []
        lines: list[str] = [f"[合并转发消息 id={forward_id}]"]
        max_nodes = 6
        for idx, raw_node in enumerate(nodes[:max_nodes], start=1):
            if not isinstance(raw_node, dict):
                continue
            sender_raw = raw_node.get("sender")
            sender = sender_raw if isinstance(sender_raw, dict) else {}
            node_name = str(sender.get("name") or sender.get("nickname") or sender.get("card") or "unknown")
            node_id = str(
                sender.get("user_id")
                or sender.get("uin")
                or sender.get("qq")
                or sender.get("id")
                or "unknown"
            ).strip()
            content_payload = raw_node.get("content")
            if content_payload is None:
                content_payload = raw_node.get("message")
            parsed = self._extract_message_from_payload(content_payload, self_qq)
            out_images = self._merge_images(out_images, parsed.images)
            node_content = self._truncate_text(self._compact_text(parsed.text or "（无文本）"), 240)
            lines.append(
                f"- node_{idx}:\n"
                f"  user_name: {node_name}\n"
                f"  user_id: {node_id or 'unknown'}\n"
                f"  content: {node_content}"
            )
        remaining = len(nodes) - min(len(nodes), max_nodes)
        if remaining > 0:
            lines.append(f"- 其余 {remaining} 条转发节点已省略")
        return "\n".join(lines), out_images

    async def _onebot_payload_to_satori_content(
        self,
        payload: Any,
        route: dict[str, str],
        *,
        depth: int,
        seen_forward_ids: set[str],
    ) -> str:
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return ""
            return escape(text)
        if isinstance(payload, list):
            out: list[str] = []
            for seg in payload:
                rendered = await self._onebot_segment_to_satori_element(
                    seg,
                    route,
                    depth=depth,
                    seen_forward_ids=seen_forward_ids,
                )
                if rendered:
                    out.append(rendered)
            return "".join(out)
        if isinstance(payload, dict):
            # OneBot-like segment object.
            if "type" in payload or "data" in payload:
                return await self._onebot_segment_to_satori_element(
                    payload,
                    route,
                    depth=depth,
                    seen_forward_ids=seen_forward_ids,
                )
            # Wrapper-like payload.
            for key in ("content", "message"):
                if key in payload:
                    return await self._onebot_payload_to_satori_content(
                        payload.get(key),
                        route,
                        depth=depth,
                        seen_forward_ids=seen_forward_ids,
                    )
            for key in ("text", "value", "name"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return escape(value.strip())
        return ""

    async def _onebot_segment_to_satori_element(
        self,
        seg: Any,
        route: dict[str, str],
        *,
        depth: int,
        seen_forward_ids: set[str],
    ) -> str:
        if not isinstance(seg, dict):
            return escape(str(seg))
        data_raw = seg.get("data")
        data = data_raw if isinstance(data_raw, dict) else {}
        seg_type = str(seg.get("type") or data.get("type") or "").strip().lower()

        def _read(*keys: str) -> str:
            for key in keys:
                value = seg.get(key)
                if value is not None:
                    return str(value)
                value = data.get(key)
                if value is not None:
                    return str(value)
            return ""

        if seg_type == "text":
            return escape(_read("text", "content", "value", "name"))
        if seg_type == "at":
            mention_type = _read("at_type", "mention_type").strip().lower()
            target = _read("id", "qq", "user_id", "userId").strip()
            if mention_type in {"all", "here"}:
                return f'<at type="{escape(mention_type, quote=True)}"/>'
            if target.lower() == "all":
                return '<at type="all"/>'
            if target:
                return f'<at id="{escape(target, quote=True)}"/>'
            return ""
        if seg_type in {"img", "image"}:
            src = _read("src", "url", "file", "path").strip()
            if src:
                return f'<img src="{escape(src, quote=True)}"/>'
            return "<img/>"
        if seg_type in {"reply", "quote"}:
            msg_id = _read("id", "message_id", "messageId").strip()
            if msg_id:
                return f'<quote id="{escape(msg_id, quote=True)}"/>'
            return "<quote/>"
        if seg_type in {"forward", "longmsg"}:
            msg_id = _read("id", "message_id", "messageId", "resid", "res_id").strip()
            if msg_id:
                nested = await self._resolve_nested_forward_content(
                    route,
                    msg_id,
                    depth=depth + 1,
                    seen_forward_ids=seen_forward_ids,
                )
                if nested:
                    return nested
                return f'<message id="{escape(msg_id, quote=True)}" forward/>'
            return "<message forward/>"
        fallback_text = _read("text", "content", "value", "name")
        if fallback_text:
            return escape(fallback_text)
        return ""

    async def _resolve_nested_forward_content(
        self,
        route: dict[str, str],
        message_id: str,
        *,
        depth: int,
        seen_forward_ids: set[str],
    ) -> str:
        forward_id = message_id.strip()
        if not forward_id:
            return ""
        if depth > 4:
            logging.warning(
                "Nested forward depth limit reached: channel_id=%s message_id=%s depth=%s",
                route.get("channel_id"),
                forward_id,
                depth,
            )
            return ""
        if forward_id in seen_forward_ids:
            logging.warning(
                "Nested forward cycle detected: channel_id=%s message_id=%s",
                route.get("channel_id"),
                forward_id,
            )
            return ""
        seen_forward_ids.add(forward_id)
        try:
            data, _ = await self._satori_message_get_with_forward_fallback(
                route,
                forward_id,
                depth=depth,
                seen_forward_ids=seen_forward_ids,
            )
            if not isinstance(data, dict):
                return ""
            content = str(data.get("content") or "").strip()
            return content
        finally:
            seen_forward_ids.discard(forward_id)

    async def _build_satori_forward_message_from_internal(
        self,
        route: dict[str, str],
        forward_id: str,
        payload: dict[str, Any],
        *,
        depth: int,
        seen_forward_ids: set[str],
    ) -> dict[str, Any] | None:
        messages_raw = payload.get("messages")
        messages = messages_raw if isinstance(messages_raw, list) else []
        if not messages:
            return None

        parts: list[str] = ["<message forward>"]
        node_count = 0
        for raw_node in messages:
            if not isinstance(raw_node, dict):
                continue
            node_count += 1
            content_payload = raw_node.get("content")
            if content_payload is None:
                content_payload = raw_node.get("message")
            rendered = await self._onebot_payload_to_satori_content(
                content_payload,
                route,
                depth=depth,
                seen_forward_ids=seen_forward_ids,
            )
            node_msg_id = str(raw_node.get("message_id") or raw_node.get("id") or "").strip()
            msg_open = (
                f'<message id="{escape(node_msg_id, quote=True)}">'
                if node_msg_id
                else "<message>"
            )
            parts.append(msg_open)
            if rendered:
                parts.append(rendered)
            parts.append("</message>")

        if node_count <= 0:
            return None
        parts.append("</message>")
        content = "".join(parts)
        return {
            "id": forward_id,
            "message_id": forward_id,
            "content": content,
            "user": {
                "id": "forward",
                "name": "合并转发",
            },
        }

    async def _satori_message_get_with_forward_fallback(
        self,
        route: dict[str, str],
        message_id: str,
        *,
        depth: int = 0,
        seen_forward_ids: set[str] | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        payload = {
            "channel_id": route["channel_id"],
            "message_id": message_id,
        }
        data = await self._satori_action("message.get", payload, route, timeout_sec=8)
        if isinstance(data, dict):
            return data, False

        logging.info(
            "Satori message.get fallback triggered: channel_id=%s message_id=%s -> internal/onebot11/get_forward_msg",
            route.get("channel_id"),
            message_id,
        )
        internal_raw = await self._satori_action(
            "internal/onebot11/get_forward_msg",
            {"message_id": message_id},
            route,
            timeout_sec=10,
        )
        if not isinstance(internal_raw, dict):
            logging.warning(
                "Satori message.get fallback failed: internal response invalid, channel_id=%s message_id=%s",
                route.get("channel_id"),
                message_id,
            )
            return None, False

        payload_candidates: list[dict[str, Any]] = []
        data_raw = internal_raw.get("data")
        if isinstance(data_raw, dict):
            payload_candidates.append(data_raw)
        payload_candidates.append(internal_raw)
        seen = set(seen_forward_ids or set())
        seen.add(message_id)
        for candidate in payload_candidates:
            msg_raw = candidate.get("messages")
            if not isinstance(msg_raw, list) or not msg_raw:
                continue
            synthesized = await self._build_satori_forward_message_from_internal(
                route,
                message_id,
                candidate,
                depth=depth,
                seen_forward_ids=seen,
            )
            if isinstance(synthesized, dict):
                node_ids: list[str] = []
                for item in msg_raw:
                    if not isinstance(item, dict):
                        continue
                    node_id = str(item.get("message_id") or item.get("id") or "").strip()
                    if node_id:
                        node_ids.append(node_id)
                preview = re.sub(r"\s+", " ", str(synthesized.get("content") or "")).strip()
                if len(preview) > 240:
                    preview = preview[:239].rstrip() + "…"
                logging.info(
                    "Satori message.get fallback hit: channel_id=%s message_id=%s forward_nodes=%s node_ids=%s content_preview=%s",
                    route.get("channel_id"),
                    message_id,
                    len(msg_raw),
                    node_ids,
                    preview,
                )
                return synthesized, True
        logging.warning(
            "Satori message.get fallback failed: no forward messages, channel_id=%s message_id=%s",
            route.get("channel_id"),
            message_id,
        )
        return None, False

    async def _augment_parsed_message(self, event: dict[str, Any], parsed: ParsedMessage) -> ParsedMessage:
        route = self._satori_route_from_event(event)
        if route is None:
            return parsed

        self_qq = self._get_self_qq(event)
        quote_fallback_map = self._extract_satori_quote_fallback_map(event, self_qq)
        quote_id_set = self._extract_satori_quote_id_set(event)
        history_index: dict[str, dict[str, Any]] | None = None
        reply_blocks: list[str] = []
        forward_blocks: list[str] = []
        merged_images = list(parsed.images)

        # 引用消息：按 message_id 拉取并结构化到上下文中。
        for reply_id in parsed.reply_ids[:2]:
            try:
                data, _ = await self._satori_message_get_with_forward_fallback(
                    route,
                    reply_id,
                    seen_forward_ids={reply_id},
                )
            except Exception as exc:  # noqa: BLE001
                logging.warning(
                    "Quote fallback chain failed but ignored: channel_id=%s message_id=%s err=%s",
                    route.get("channel_id"),
                    reply_id,
                    exc,
                )
                data = None
            if not isinstance(data, dict):
                if history_index is None:
                    history_index = await self._satori_history_message_index(route, limit=200)
                history_data = history_index.get(reply_id)
                if isinstance(history_data, dict):
                    block, images = self._build_satori_message_context_block(
                        "引用消息",
                        reply_id,
                        history_data,
                        self_qq,
                    )
                    reply_blocks.append(block)
                    merged_images = self._merge_images(merged_images, images)
                    continue
                fallback = quote_fallback_map.get(reply_id)
                if fallback is not None:
                    block, images = self._build_parsed_context_block(
                        "引用消息",
                        reply_id,
                        fallback,
                    )
                    reply_blocks.append(block)
                    merged_images = self._merge_images(merged_images, images)
                    continue
                status = (
                    "引用目标可能为转发/特殊消息，当前 Satori 实现无法取回正文"
                    if reply_id in quote_id_set
                    else "无法拉取引用消息内容"
                )
                reply_blocks.append("[引用消息]\n" f"- message_id: {reply_id}\n" f"- status: {status}")
                continue
            block, images = self._build_satori_message_context_block("引用消息", reply_id, data, self_qq)
            reply_blocks.append(block)
            merged_images = self._merge_images(merged_images, images)

        # 转发消息：优先按 id 尝试 message.get；若平台不支持则至少保留结构化 id。
        for forward_id in parsed.forward_ids[:2]:
            try:
                data, _ = await self._satori_message_get_with_forward_fallback(
                    route,
                    forward_id,
                    seen_forward_ids={forward_id},
                )
            except Exception as exc:  # noqa: BLE001
                logging.warning(
                    "Forward fallback chain failed but ignored: channel_id=%s message_id=%s err=%s",
                    route.get("channel_id"),
                    forward_id,
                    exc,
                )
                data = None
            if isinstance(data, dict):
                block, images = self._build_satori_message_context_block("转发消息", forward_id, data, self_qq)
                forward_blocks.append(block)
                merged_images = self._merge_images(merged_images, images)
                continue
            if history_index is None:
                history_index = await self._satori_history_message_index(route, limit=200)
            history_data = history_index.get(forward_id)
            if isinstance(history_data, dict):
                block, images = self._build_satori_message_context_block(
                    "转发消息",
                    forward_id,
                    history_data,
                    self_qq,
                )
                forward_blocks.append(block)
                merged_images = self._merge_images(merged_images, images)
                continue
            fallback = quote_fallback_map.get(forward_id)
            if fallback is not None:
                block, images = self._build_parsed_context_block(
                    "转发消息",
                    forward_id,
                    fallback,
                )
                forward_blocks.append(block)
                merged_images = self._merge_images(merged_images, images)
                continue
            status = (
                "转发目标可能为转发/特殊消息，当前 Satori 实现无法取回节点详情"
                if forward_id in quote_id_set
                else "当前平台不支持按该 id 拉取转发节点详情"
            )
            forward_blocks.append(
                "[转发消息]\n"
                f"- message_id: {forward_id}\n"
                f"- status: {status}"
            )

        text_out = parsed.text
        if not text_out.strip():
            if parsed.reply_ids:
                text_out = f'<quote id="{escape(parsed.reply_ids[0], quote=True)}"/>'
            elif parsed.forward_ids:
                text_out = f'<message id="{escape(parsed.forward_ids[0], quote=True)}" forward/>'

        return ParsedMessage(
            text=text_out,
            mentioned=parsed.mentioned,
            images=merged_images,
            reply_ids=list(parsed.reply_ids),
            forward_ids=list(parsed.forward_ids),
            reply_blocks=reply_blocks,
            forward_blocks=forward_blocks,
        )

    async def _satori_history_message_index(
        self,
        route: dict[str, str],
        *,
        limit: int = 200,
    ) -> dict[str, dict[str, Any]]:
        payload = {
            "channel_id": route["channel_id"],
            "limit": max(20, min(200, int(limit))),
        }
        data = await self._satori_action("message.list", payload, route, timeout_sec=10)
        if not isinstance(data, dict):
            return {}
        records_raw = data.get("data")
        records = records_raw if isinstance(records_raw, list) else []
        out: dict[str, dict[str, Any]] = {}
        for item in records:
            if not isinstance(item, dict):
                continue
            msg_id = str(item.get("id") or item.get("message_id") or "").strip()
            if not msg_id:
                continue
            out[msg_id] = item
        return out

    def _extract_satori_quote_fallback_map(
        self,
        event: dict[str, Any],
        self_qq: str,
    ) -> dict[str, ParsedMessage]:
        out: dict[str, ParsedMessage] = {}
        evt_raw = event.get("_satori_event")
        evt = evt_raw if isinstance(evt_raw, dict) else {}
        msg_raw = evt.get("message")
        msg = msg_raw if isinstance(msg_raw, dict) else {}
        content = str(msg.get("content") or "").strip()
        if not content:
            return out

        parse_attrs = getattr(self, "_satori_parse_tag_attrs", None)
        parse_segments = getattr(self, "_satori_content_to_segments", None)
        quote_pattern = re.compile(r"<quote\b([^>]*)>(.*?)</quote>", flags=re.IGNORECASE | re.DOTALL)
        for matched in quote_pattern.finditer(content):
            attr_text = matched.group(1) or ""
            attrs: dict[str, str] = {}
            if callable(parse_attrs):
                maybe_attrs = parse_attrs(attr_text)
                if isinstance(maybe_attrs, dict):
                    attrs = {str(k): str(v) for k, v in maybe_attrs.items()}
            quote_id = str(attrs.get("id") or attrs.get("message_id") or "").strip()
            if not quote_id:
                continue
            quote_inner = matched.group(2) or ""
            payload = parse_segments(quote_inner) if callable(parse_segments) else quote_inner
            parsed = self._extract_message_from_payload(payload, self_qq)
            out[quote_id] = parsed
        return out

    def _extract_satori_quote_id_set(self, event: dict[str, Any]) -> set[str]:
        out: set[str] = set()
        evt_raw = event.get("_satori_event")
        evt = evt_raw if isinstance(evt_raw, dict) else {}
        msg_raw = evt.get("message")
        msg = msg_raw if isinstance(msg_raw, dict) else {}
        content = str(msg.get("content") or "").strip()
        if not content:
            return out
        parse_attrs = getattr(self, "_satori_parse_tag_attrs", None)
        if not callable(parse_attrs):
            return out
        for matched in re.finditer(r"<quote\b([^>]*)/?>", content, flags=re.IGNORECASE):
            attrs = parse_attrs(matched.group(1) or "")
            if not isinstance(attrs, dict):
                continue
            quote_id = str(attrs.get("id") or attrs.get("message_id") or "").strip()
            if quote_id:
                out.add(quote_id)
        return out

    def _build_parsed_context_block(
        self,
        tag: str,
        message_id: str,
        parsed: ParsedMessage,
        *,
        user_name: str = "unknown",
        user_id: str = "unknown",
        status: str = "",
    ) -> tuple[str, list[MessageImage]]:
        preview = self._truncate_text(self._compact_text(parsed.text or "（无文本）"), 300)
        reply_ids = ", ".join(parsed.reply_ids) if parsed.reply_ids else "null"
        forward_ids = ", ".join(parsed.forward_ids) if parsed.forward_ids else "null"
        lines = [
            f"[{tag}]",
            f"- message_id: {message_id}",
            f"- user_name: {user_name}",
            f"- user_id: {user_id}",
            f"- content: {preview}",
            f"- reply_ids: {reply_ids}",
            f"- forward_ids: {forward_ids}",
        ]
        if status:
            lines.append(f"- status: {status}")
        return "\n".join(lines), parsed.images

    def _build_satori_message_context_block(
        self, tag: str, message_id: str, payload: dict[str, Any], self_qq: str
    ) -> tuple[str, list[MessageImage]]:
        user_raw = payload.get("user")
        user = user_raw if isinstance(user_raw, dict) else {}
        member_raw = payload.get("member")
        member = member_raw if isinstance(member_raw, dict) else {}
        member_user_raw = member.get("user")
        member_user = member_user_raw if isinstance(member_user_raw, dict) else {}
        sender_id = str(
            user.get("id")
            or member_user.get("id")
            or user.get("user_id")
            or member_user.get("user_id")
            or "unknown"
        ).strip()
        sender_name = str(
            member.get("nick")
            or member.get("name")
            or user.get("name")
            or user.get("nick")
            or sender_id
            or "unknown"
        ).strip()
        content = str(payload.get("content") or "").strip()
        parser = getattr(self, "_satori_content_to_segments", None)
        parsed = (
            self._extract_message_from_payload(parser(content), self_qq)
            if callable(parser)
            else self._extract_message_from_payload(content, self_qq)
        )
        block, images = self._build_parsed_context_block(
            tag,
            message_id,
            parsed,
            user_name=sender_name or "unknown",
            user_id=sender_id or "unknown",
        )
        return block, images

    async def _download_image(self, url: str) -> tuple[bytes | None, str]:
        assert self.session is not None
        satori_host = urlparse(self.cfg.satori_http_base).netloc
        headers: dict[str, str] = {"User-Agent": "Mozilla/5.0"}
        token = self.cfg.satori_token.strip()
        if satori_host and urlparse(url).netloc == satori_host and token:
            headers["Authorization"] = f"Bearer {token}"

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
            url = img.url.strip()
            if not url:
                logging.warning("Skip image without url (file=%s)", img.file)
                continue
            if not (url.startswith("http://") or url.startswith("https://")):
                logging.warning("Skip image without downloadable url (file=%s)", img.file)
                continue

            try:
                content, ctype = await self._download_image(url)
                if not content:
                    logging.warning("Skip image download failed (file=%s url=%s)", img.file, url)
                    continue
                if len(content) > max_bytes:
                    logging.warning("Skip image too large (%s bytes): %s", len(content), url)
                    continue

                mime_type = self._guess_image_mime(url, img.file, ctype)
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
