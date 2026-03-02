# llonebot <-> OpenClaw Bridge

这个脚本把 `llonebot11 (OneBot v11)` 和 `OpenClaw Gateway (WebSocket RPC)` 连起来，实现 QQ 对话走 OpenClaw。

默认配置按你的当前环境写好：

- `llonebot HTTP`: `http://127.0.0.1:3000`
- `llonebot WS`: `ws://127.0.0.1:3001`
- `OpenClaw Gateway WS`: 主地址 `ws://127.0.0.1:18790`，自动回退 `ws://127.0.0.1:18789`

## 1. 安装依赖

```bash
/storage/llonebot/.venv/bin/python -c "import aiohttp; print('aiohttp ok')"
```

## 2. 启动桥接

```bash
/storage/llonebot/.venv/bin/python bridge_openclaw_llonebot.py
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

## 4. 触发规则

- 私聊：直接对话
- 群聊：满足以下任一条件会触发
  - `@机器人`
  - 以 `GROUP_PREFIX`（默认 `/ai`）开头
