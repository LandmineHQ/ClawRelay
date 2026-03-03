import base64
import json
import logging
import mimetypes
import os
import re
from collections import deque
from datetime import datetime
from html import unescape
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

    def _onebot_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.cfg.onebot_access_token:
            headers["Authorization"] = f"Bearer {self.cfg.onebot_access_token}"
        return headers

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
        sender_name: str, sender_qq: str, normalized_text: str, images: list[MessageImage], ts: float
    ) -> str:
        display = normalized_text or "（无文本）"
        # if images:
        #     refs = [os.path.basename(img.file.strip()) if img.file.strip() else "unknown" for img in images]
        #     display = f"{display} | 图片哈希: {', '.join(refs)}"
        time_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
        return f"{time_str} {sender_name}({sender_qq}): {display}"

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
    def _detect_local_command(text: str, images: list[MessageImage]) -> str | None:
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
        stripped = stripped.strip().lower()
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

    def _build_prompt_from_pending(
        self, pending: list[PendingObservation], latest_text: str, include_guidance: bool = True
    ) -> str:
        recent = pending[-max(1, self.cfg.context_flush_limit) :]
        # 触发消息会先写入 pending，这里将历史与当前待回复消息拆开，避免重复占用 token。
        history = recent[:-1] if len(recent) > 1 else []
        latest_from_pending = recent[-1].normalized_text.strip() if recent else ""
        latest = latest_text.strip() or latest_from_pending or "（用户发送了图片）"
        history_lines = [f"{i + 1}. {item.line}" for i, item in enumerate(history)]
        history_block = "\n".join(history_lines) if history_lines else "（无）"
        if not include_guidance:
            return (
                "历史记录（不含当前消息）：\n"
                + "```text\n"
                + history_block
                + "\n```\n"
                + "当前待回复消息：\n"
                + "```text\n"
                + latest
                + "\n```"
            )
        return (
            "你正在 QQ 群聊中对话，请结合历史继续当前话题并直接回复用户。\n"
            + "历史记录（不含当前消息，按时间顺序）：\n"
            + "```text\n"
            + history_block
            + "\n```\n"
            + "当前待回复消息：\n"
            + "```text\n"
            + latest
            + "\n```\n"
            + "说明：消息中的@以 OneBot CQ 码表示（例如 `[CQ:at,qq=123456]`）。"
            + "如果需要@某人，请在回复中输出对应的 CQ 码，或使用 `[@123456]`。"
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

        message_type = event.get("message_type")
        endpoint: str
        payload: dict[str, Any]
        if message_type == "private":
            endpoint = "send_private_msg"
            payload = {"user_id": event.get("user_id"), "message": text, "auto_escape": False}
        else:
            endpoint = "send_group_msg"
            group_text = self._normalize_outgoing_mentions(text)
            if (
                self.cfg.group_reply_at_sender
                and event.get("user_id") is not None
                and not self._contains_cq_at(group_text)
            ):
                group_text = f"[CQ:at,qq={event['user_id']}] {group_text}"
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
                parsed = json.loads(body)
            except json.JSONDecodeError:
                logging.warning("OneBot non-JSON response: %s", body[:200])
                return
            if not isinstance(parsed, dict):
                logging.warning("OneBot non-object response: %s", body[:200])
                return
            data = parsed
            if data.get("status") != "ok" and data.get("retcode") not in (0, None):
                raise RuntimeError(f"OneBot send failed: {data}")

    @staticmethod
    def _contains_cq_at(text: str) -> bool:
        return bool(re.search(r"\[CQ:at,qq=[^\]]+\]", text, flags=re.IGNORECASE))

    @staticmethod
    def _normalize_outgoing_mentions(text: str) -> str:
        # Allow model to use lightweight mention form like [@123456] / [@qq=123456].
        out = re.sub(r"\[@(?:qq=)?(\d+)\]", r"[CQ:at,qq=\1]", text, flags=re.IGNORECASE)
        out = re.sub(r"\[@all\]", "[CQ:at,qq=all]", out, flags=re.IGNORECASE)
        return out

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
                parsed = json.loads(text)
        except Exception as exc:  # noqa: BLE001
            logging.warning("OneBot action %s failed: %s", action, exc)
            return None

        if not isinstance(parsed, dict):
            logging.warning("OneBot action %s non-object response: %s", action, text[:200])
            return None
        data = parsed
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
