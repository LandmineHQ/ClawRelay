import base64
import json
import logging
import mimetypes
import os
import re
from collections import deque
from datetime import datetime
from html import escape, unescape
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
                    logging.warning("Satori action %s HTTP %s: %s", action, resp.status, text[:200])
                    return None
                if not text.strip():
                    return {}
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    return {}
        except Exception as exc:  # noqa: BLE001
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
    def _mention_tag(qq: str) -> str:
        qq_value = qq.strip()
        if not qq_value:
            return ""
        return f"[CQ:at,qq={qq_value}]"

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
        reply_ids: list[str] = []
        forward_ids: list[str] = []

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
                    qq = str(data.get("qq", "")).strip()
                    if self_qq and qq == self_qq:
                        mentioned = True
                    mention_text = self._mention_tag(qq)
                    if mention_text:
                        text_parts.append(mention_text)
                elif seg_type == "image":
                    url = str(data.get("url", "")).strip()
                    file = str(data.get("file", "")).strip()
                    img = MessageImage(url=url, file=file)
                    images.append(img)
                    text_parts.append(self._image_tag(img))
                elif seg_type in {"reply", "quote"}:
                    ref_id = str(data.get("id") or data.get("message_id") or "").strip()
                    if ref_id:
                        reply_ids.append(ref_id)
                elif seg_type in {"forward", "longmsg"}:
                    fwd_id = str(
                        data.get("id")
                        or data.get("message_id")
                        or data.get("resid")
                        or data.get("res_id")
                        or ""
                    ).strip()
                    if fwd_id:
                        forward_ids.append(fwd_id)
            text = re.sub(r"\s+", " ", "".join(text_parts)).strip()
            return ParsedMessage(
                text=text,
                mentioned=mentioned,
                images=images,
                reply_ids=reply_ids,
                forward_ids=forward_ids,
            )

        if isinstance(payload, str):
            text_parts: list[str] = []
            last = 0
            for matched in re.finditer(r"\[CQ:([a-zA-Z0-9_]+)(?:,([^\]]*))?\]", payload):
                text_parts.append(payload[last : matched.start()])
                last = matched.end()
                cq_type = matched.group(1)
                params = self._parse_cq_params(matched.group(2) or "")
                if cq_type == "at":
                    qq = params.get("qq", "").strip()
                    if self_qq and qq == self_qq:
                        mentioned = True
                    mention_text = self._mention_tag(qq)
                    if mention_text:
                        text_parts.append(mention_text)
                    continue
                if cq_type == "image":
                    img = MessageImage(
                        url=params.get("url", "").strip(),
                        file=params.get("file", "").strip(),
                    )
                    images.append(img)
                    text_parts.append(self._image_tag(img))
                    continue
                if cq_type in {"reply", "quote"}:
                    ref_id = str(params.get("id", "")).strip()
                    if ref_id:
                        reply_ids.append(ref_id)
                    continue
                if cq_type in {"forward", "longmsg"}:
                    fwd_id = str(
                        params.get("id")
                        or params.get("message_id")
                        or params.get("resid")
                        or params.get("res_id")
                        or ""
                    ).strip()
                    if fwd_id:
                        forward_ids.append(fwd_id)
                    continue
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
            return ParsedMessage(
                text=text,
                mentioned=mentioned,
                images=images,
                reply_ids=reply_ids,
                forward_ids=forward_ids,
            )

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
        return (
            f"- time: {time_str}\n"
            f"  user_name: {obs.sender_name or 'unknown'}\n"
            f"  user_id: {obs.sender_id or 'unknown'}\n"
            f"  content: {display}"
        )

    def _record_observation(
        self, event: dict[str, Any], session_key: str, normalized_text: str, images: list[MessageImage]
    ) -> None:
        sender_name = self._sender_name(event)
        sender_qq = str(event.get("user_id", "unknown"))
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
        # Some clients send plain text like "@机器人 /new" instead of CQ at segment.
        matched = re.match(r"^\s*@[^ \t\r\n]+\s*", text)
        if not matched:
            return text, False
        return text[matched.end() :].strip(), True

    @staticmethod
    def _extract_op_target_user_id(arg: str) -> str:
        raw = (arg or "").strip()
        if re.fullmatch(r"\d{5,20}", raw):
            return raw
        cq_match = re.fullmatch(r"\[cq:at,[^\]]*qq=(\d{5,20})[^\]]*\]", raw, flags=re.IGNORECASE)
        if cq_match:
            return cq_match.group(1)
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
                r"^\s*\[CQ:at,qq=[^\]]+\]\s*",
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
        self, pending: list[PendingObservation], latest_line: str, include_guidance: bool = True
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
                f"  content: {latest_line.strip() or '（无文本）'}"
            )

        history_lines = [self._format_observation_item(item) for item in history]
        history_block = "\n\n".join(history_lines) if history_lines else "（无）"

        rule_block = (
            "规则：\n"
            "1) user_name 可能包含数字（例如“我是1354987”），这不是 QQ 号。\n"
            "2) 只能使用 user_id 作为 QQ 号；如需@，仅可输出 `<at id=\"<user_id>\"/>`。\n"
            "3) 默认@发送者，除非被要求不@发送者。\n"
            f"4) 当前待回复消息发送者 user_id: {current_sender_id}\n"
            "5) 默认使用简洁纯文本回复；仅当用户明确要求 Markdown 时，才使用 Markdown。"
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
        outgoing = self._normalize_outgoing_mentions(text)
        if (
            event.get("message_type") == "group"
            and self.cfg.group_reply_at_sender
            and event.get("user_id") is not None
        ):
            uid = str(event.get("user_id") or "").strip()
            if uid and f'<at id="{uid}"/>' not in outgoing:
                outgoing = f'<at id="{uid}"/> {outgoing}'
        payload = {
            "channel_id": route["channel_id"],
            "content": outgoing,
        }
        result = await self._satori_action("message.create", payload, route, timeout_sec=12)
        if result is None:
            raise RuntimeError("Satori message.create failed")

    @staticmethod
    def _contains_cq_at(text: str) -> bool:
        return bool(re.search(r"\[CQ:at,qq=[^\]]+\]", text, flags=re.IGNORECASE))

    @staticmethod
    def _contains_cq_at_user(text: str, user_id: str) -> bool:
        qq = user_id.strip()
        if not qq:
            return False
        pattern = rf"\[CQ:at,qq={re.escape(qq)}\]"
        return bool(re.search(pattern, text, flags=re.IGNORECASE))

    @staticmethod
    def _normalize_outgoing_mentions(text: str) -> str:
        normalized = text or ""
        pattern = re.compile(
            r"\[CQ:at,qq=([^\],]+)(?:,[^\]]*)?\]|\[@(?:qq=)?([0-9]+)\]|\[@all\]|(<at\s+[^>]*?/?>)",
            flags=re.IGNORECASE,
        )
        out: list[str] = []
        last = 0
        for matched in pattern.finditer(normalized):
            plain = normalized[last : matched.start()]
            if plain:
                out.append(escape(plain))
            if matched.group(3):
                out.append(matched.group(3))
                last = matched.end()
                continue
            token = matched.group(0).lower()
            qq = (matched.group(1) or matched.group(2) or "").strip()
            if token == "[@all]" or qq.lower() == "all":
                out.append('<at type="all"/>')
            elif qq:
                out.append(f'<at id="{escape(qq)}"/>')
            last = matched.end()
        tail = normalized[last:]
        if tail:
            out.append(escape(tail))
        return "".join(out)

    async def _onebot_action(
        self, action: str, payload: dict[str, Any], timeout_sec: float | None = None
    ) -> dict[str, Any] | None:
        # 保留旧方法名，便于核心代码复用；Satori 下不再提供 OneBot 扩展查询能力。
        _ = (action, payload, timeout_sec)
        return None

    async def _mark_processing_emoji(self, event: dict[str, Any]) -> dict[str, Any] | None:
        marker = self._processing_marker_from_event(event)
        if marker is None:
            return None
        payload = {
            "channel_id": marker["route"]["channel_id"],
            "message_id": marker["message_id"],
            "emoji": marker["emoji"],
        }
        result = await self._satori_action("reaction.create", payload, marker["route"], timeout_sec=8)
        if result is None:
            logging.warning(
                "Failed to set processing emoji via reaction.create: message_id=%s emoji=%s emoji_id=%s",
                marker.get("message_id"),
                marker.get("emoji"),
                marker.get("emoji_id"),
            )
            return None
        return marker

    async def _clear_processing_emoji(self, marker: dict[str, Any] | None) -> None:
        if marker is None:
            return
        message_id = marker.get("message_id")
        emoji = marker.get("emoji")
        route = marker.get("route")
        if message_id is None or emoji is None or not isinstance(route, dict):
            return
        marker_key = f"{message_id}:{emoji}"
        if marker_key in self.cleared_processing_marker_set:
            return
        payload = {
            "channel_id": route.get("channel_id"),
            "message_id": message_id,
            "emoji": emoji,
        }
        result = await self._satori_action("reaction.clear", payload, route, timeout_sec=8)
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
            "Failed to clear processing emoji via reaction.clear: message_id=%s emoji=%s emoji_id=%s",
            message_id,
            emoji,
            marker.get("emoji_id"),
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
            # Satori reaction 参数使用 emoji 字段，这里固定传 QQ 表情 id（字符串形式）。
            "emoji": str(emoji_id),
            "emoji_id": emoji_id,
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

    async def _onebot_get_forward_msg(self, forward_id: str) -> dict[str, Any] | None:
        candidates = [
            {"message_id": forward_id},
            {"id": forward_id},
            {"resid": forward_id},
            {"res_id": forward_id},
        ]
        best: dict[str, Any] | None = None
        for payload in candidates:
            data = await self._onebot_action("get_forward_msg", payload, timeout_sec=10)
            if data is None:
                continue
            best = data
            messages = data.get("messages")
            if isinstance(messages, list) and messages:
                return data
        return best

    async def _augment_parsed_message(self, event: dict[str, Any], parsed: ParsedMessage) -> ParsedMessage:
        _ = event
        # Satori 协议下不依赖 OneBot 的 get_msg/get_forward_msg 补查，直接使用事件原始内容。
        return parsed

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
