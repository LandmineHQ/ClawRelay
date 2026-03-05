# llonebot <-> OpenClaw Bridge

这个脚本把 `Satori` 和 `OpenClaw Gateway (WebSocket RPC)` 连起来，实现 QQ 对话走 OpenClaw。

默认配置按你的当前环境写好：

- `Satori HTTP`: `http://127.0.0.1:5600`
- `Satori WS`: `ws://127.0.0.1:5600/v1/events`
- `OpenClaw Gateway WS`: `ws://127.0.0.1:18789`

## 代码结构

- `bridge_openclaw_llonebot.py`：启动入口（`main/cli`）
- `bridge_core.py`：桥接主流程编排（收消息、触发命令、上下文冲刷、调用两侧模块）
- `bridge_onebot_mixin.py`：Satori 事件适配、Satori API 调用、图片下载与附件构建
- `bridge_openclaw_mixin.py`：OpenClaw Gateway 连接、请求、模型能力探测
- `bridge_config.py`：配置与环境变量解析
- `bridge_models.py`：共享数据结构（消息、图片、上下文观测）

## 1. 安装依赖

```bash
uv sync
```

## 2. 启动桥接

```bash
uv run start
```

## 3. 可选环境变量

- `SATORI_HTTP_BASE`：默认 `http://127.0.0.1:5600`
- `SATORI_WS_URL`：默认 `ws://127.0.0.1:5600/v1/events`
- `SATORI_TOKEN`：Satori token（默认沿用你原有 token，可覆盖）
- `SATORI_PLATFORM`：默认 `chronocat`
- `SATORI_SELF_ID`：可选，机器人账号 ID
- `SATORI_PROCESSING_EMOJI_ID`：处理中表态 id，默认 `30`（QQ 奋斗）
- `OPENCLAW_WS_URL`：默认 `ws://127.0.0.1:18789`
- `OPENCLAW_GATEWAY_TOKEN`：Gateway token（默认写入了当前机器 OpenClaw 配置中的 token，可覆盖）
- `OPENCLAW_GATEWAY_PASSWORD`：Gateway password（如你使用密码鉴权）
- `OPENCLAW_CLIENT_ID`：默认 `gateway-client`
- `OPENCLAW_CLIENT_MODE`：默认 `backend`
- `OPENCLAW_ROLE`：默认 `operator`
- `OPENCLAW_SCOPES`：默认 `operator.read,operator.write`
- `OPENCLAW_TIMEOUT_SEC`：OpenClaw 单次问答超时，默认 `180`
- `OPENCLAW_SESSION_PREFIX`：会话前缀，默认 `llonebot`
- `REQUIRE_PAIRING`：是否开启 OP 审批配对门禁（私聊/群聊均生效），默认 `true`
- `PAIRING_CODE_LEN`：配对码长度（4~12），默认 `8`
- `PAIRING_TTL_SEC`：配对码有效期（秒），默认 `3600`
- `PAIRING_STORE_PATH`：配对审批记录持久化路径（记录 user/group 目标及审批人），默认 `./.bridge_pairings.json`
- `OP_USER_IDS`：初始 OP 列表（逗号分隔 user_id），默认 `1216198007`
- `OP_STORE_PATH`：OP 列表持久化路径，默认 `./.bridge_ops.json`
- `GROUP_REQUIRE_AT`：群聊是否必须 @ 机器人才触发，默认 `true`
- `GROUP_PREFIX`：群聊命令前缀，默认 `/ai`（可与@并存）
- `GROUP_REPLY_AT_SENDER`：群聊回复时是否 @ 发送者，默认 `false`
- `MAX_CONCURRENCY`：并发处理消息数，默认 `3`
- `CONTEXT_OBSERVATION_LIMIT`：每会话保留的“待冲刷”观察消息数，默认 `30`
- `CONTEXT_FLUSH_LIMIT`：触发回复时发送给 OpenClaw 的最近观察条数，默认 `12`
- `MAX_IMAGE_ATTACHMENTS`：每次最多上传图片附件数，默认 `3`
- `MAX_IMAGE_DOWNLOAD_BYTES`：单张图片下载上限（字节），默认 `6291456`

## 4. 触发规则

- 配对门禁（私聊/群聊统一）：
  - 若 `REQUIRE_PAIRING=true`，首次触发会返回配对码
  - 必须由 OP 执行 `/pair <配对码>` 审批后，当前目标会话才会转发到 OpenClaw
  - 私聊按 `user_id` 维度配对，群聊按 `group_id` 维度配对
  - 配对结果会持久化到 `PAIRING_STORE_PATH`，并记录审批人 `user_id`
- 群聊：满足以下任一条件会触发
  - `@机器人`
  - 以 `GROUP_PREFIX`（默认 `/ai`）开头
- 内建指令：
  - 说明：以下指令均支持带 `/` 或不带 `/`
  - `/new`：重置当前会话（桥接会调用 OpenClaw `sessions.reset`）
  - `/pair <配对码>`：OP 审批配对（私聊/群聊）
  - `/unpair`：OP 取消当前会话配对（私聊/群聊）
  - `/op list|add|del`：OP 列表查看/增删（仅 OP 可执行；群聊也仅 OP 可执行）
  - `/help`：查看指令说明
- 群聊未触发时也会记录该用户消息到会话上下文，等下次触发时一并带入
- 图片消息会尝试从 Satori 消息里的图片 URL 下载并作为附件发给 OpenClaw
- 会话文本中图片会保留为 Satori 原生 `<img src="..."/>` 片段
- 若桥接检测到 OpenClaw 当前默认模型不支持 `image` 输入，会在回复前增加一行提示并忽略图片附件
