# llonebot <-> OpenClaw Bridge

这个脚本把 `llonebot11 (OneBot v11)` 和 `OpenClaw Gateway (WebSocket RPC)` 连起来，实现 QQ 对话走 OpenClaw。

默认配置按你的当前环境写好：

- `llonebot HTTP`: `http://127.0.0.1:3000`
- `llonebot WS`: `ws://127.0.0.1:3001`
- `OpenClaw Gateway WS`: 主地址 `ws://127.0.0.1:18790`，自动回退 `ws://127.0.0.1:18789`

## 1. 安装依赖

```bash
UV_CACHE_DIR=/storage/llonebot/.cache/uv uv sync
```

## 2. 启动桥接

```bash
UV_CACHE_DIR=/storage/llonebot/.cache/uv uv run start
```

## 3. 可选环境变量

- `ONEBOT_HTTP_BASE`：默认 `http://127.0.0.1:3000`
- `ONEBOT_WS_URL`：默认 `ws://127.0.0.1:3001`
- `ONEBOT_ACCESS_TOKEN`：OneBot token（默认已内置你提供的 token，也可覆盖）
- `OPENCLAW_WS_URL`：默认 `ws://127.0.0.1:18790`
- `OPENCLAW_WS_FALLBACK_URL`：默认 `ws://127.0.0.1:18789`
- `OPENCLAW_GATEWAY_TOKEN`：Gateway token（默认写入了当前机器 OpenClaw 配置中的 token，可覆盖）
- `OPENCLAW_GATEWAY_PASSWORD`：Gateway password（如你使用密码鉴权）
- `OPENCLAW_CLIENT_ID`：默认 `gateway-client`
- `OPENCLAW_CLIENT_MODE`：默认 `backend`
- `OPENCLAW_ROLE`：默认 `operator`
- `OPENCLAW_SCOPES`：默认 `operator.read,operator.write`
- `OPENCLAW_TIMEOUT_SEC`：OpenClaw 单次问答超时，默认 `180`
- `OPENCLAW_SESSION_PREFIX`：会话前缀，默认 `qq`
- `GROUP_REQUIRE_AT`：群聊是否必须 @ 机器人才触发，默认 `true`
- `GROUP_PREFIX`：群聊命令前缀，默认 `/ai`（可与@并存）
- `GROUP_REPLY_AT_SENDER`：群聊回复时是否 @ 发送者，默认 `true`
- `MAX_CONCURRENCY`：并发处理消息数，默认 `3`
- `CONTEXT_OBSERVATION_LIMIT`：每会话保留的“待冲刷”观察消息数，默认 `30`
- `CONTEXT_FLUSH_LIMIT`：触发回复时发送给 OpenClaw 的最近观察条数，默认 `12`
- `MAX_IMAGE_ATTACHMENTS`：每次最多上传图片附件数，默认 `3`
- `MAX_IMAGE_DOWNLOAD_BYTES`：单张图片下载上限（字节），默认 `6291456`

## 4. 触发规则

- 私聊：直接对话
- 群聊：满足以下任一条件会触发
  - `@机器人`
  - 以 `GROUP_PREFIX`（默认 `/ai`）开头
- 内建指令：
  - `/new`：重置当前会话（桥接会调用 OpenClaw `sessions.reset`）
  - `/help`：查看指令说明
- 群聊未触发时也会记录该用户消息到会话上下文，等下次触发时一并带入
- 图片消息会尝试从 OneBot 图片 URL 下载并作为附件发给 OpenClaw
- 当消息里只有图片文件名（无 URL）时，会按 OneBot v11 调用 `get_msg` / `get_image` 补全图片信息
- 会话文本中图片会标记为 `[图片:文件哈希]`，避免多张图都只显示 `[图片]`
- 若桥接检测到 OpenClaw 当前默认模型不支持 `image` 输入，会直接提示切换模型（避免“已发图却被当成没图”）
- 若模型不支持图片，会尝试使用 OneBot `ocr_image` 进行 OCR 降级，再把 OCR 文本发给 OpenClaw
