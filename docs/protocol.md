# 客户端 ↔ 网关 协议

传输：WebSocket，一帧一个包。两种编码提供同一套语义，由客户端握手时协商：

| 编码 | 子协议 | 帧类型 | 说明 |
| --- | --- | --- | --- |
| JSON | `gate.json.v1` | 文本 | 默认。好抓包、好调试，任何客户端都能手写 |
| protobuf | `gate.pb.v1` | 二进制 | 信封小一半左右，载荷可以是原始字节 |

编码是**连接级**的，一个 gate 上两种客户端可以共存。实现见 [src/framework/protocol/codec.ts](../src/framework/protocol/codec.ts)（接口 + JSON）、[src/framework/protocol/protobufCodec.ts](../src/framework/protocol/protobufCodec.ts)、[src/framework/protocol/codecs.ts](../src/framework/protocol/codecs.ts)（协商）。

下面先讲语义（两种编码共通），最后讲两种编码各自的落地细节。

字段名统一用缩写——每个包都要付这个字节代价。

| 字段 | 含义 |
| --- | --- |
| `t` | 包类型，见下表 |
| `id` | 请求 id，由客户端分配，**必须 ≥ 1**（0 保留表示"无请求"），`Response` 原样回带 |
| `cmd` | 命令名，格式 `service.action`，正则 `^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)+$` |
| `d` | 业务负载，网关**从不解析**；可以是 JSON 值，也可以是不透明字节 |
| `seq` | 下行序号，按会话自增（服务端 → 客户端） |
| `ack` | 客户端已处理的最大 `seq`（客户端 → 服务端） |
| `cseq` | 上行序号，按会话自增（客户端 → 服务端），用于去重 |
| `e` | 错误码 |
| `m` | 人类可读的错误描述，**不要用于程序判断** |

## 包类型

| `t` | 名称 | 方向 | 载荷 |
| --- | --- | --- | --- |
| 1 | `Auth` | ↑ | `token`（JWT）, `device?` |
| 2 | `AuthAck` | ↓ | `uid`, `sid`, `rt`, `ts`, `rw`, `hb` |
| 3 | `Resume` | ↑ | `sid`, `rt`, `ack` |
| 4 | `ResumeAck` | ↓ | `uid`, `sid`, `ts`, `replay`, `cack`, `seq`, `rt`, `rw`, `hb`, `resync?`, `redirect?` |
| 5 | `Heartbeat` | ↑ | `ack?` |
| 6 | `HeartbeatAck` | ↓ | `ts`（服务器时间，可用于校准时钟） |
| 7 | `Request` | ↑ | `id`, `cmd`, `d?`, `cseq?` |
| 8 | `Response` | ↓ | `id`, `seq`, `d?`, `e?`, `m?` |
| 9 | `Notify` | ↑ | `cmd`, `d?`, `cseq?` |
| 10 | `Push` | ↓ | `seq`, `cmd`, `d?` |
| 11 | `Kick` | ↓ | `reason`, `m?`, `resumable?` |
| 12 | `Error` | ↓ | `e`, `m?`, `id?` |

`AuthAck` / `ResumeAck` 里的：

- `rt` —— 重连凭证（明文只在客户端保存，服务端存 sha256）。**每次重连成功都会换发新的**，旧的立即失效。
- `rw` —— 重连窗口（ms）：断线后多久内还能 `Resume`。
- `hb` —— 建议的心跳间隔（ms）。
- `seq` —— 该网关在此会话上已发出的最大下行序号。仅在 `resync:true` 时有意义，客户端据此重置自己的计数器。
- `cack` —— 网关已接受的最大 `cseq`，客户端只需重发大于它的请求。
- `resync` —— 会话在**另一个** gate 上恢复：身份还在，但重放缓冲留在了原节点，客户端必须重新拉取游戏状态。
- `redirect` —— 持有重放缓冲的那个 gate 的地址，能直连的客户端可以选择连回去。

## 错误码

| 码 | 含义 |
| --- | --- |
| 1001 | `BadRequest` 包格式非法 |
| 1002 | `Unauthenticated` 未登录就发业务包 |
| 1003 | `AuthFailed` JWT 校验失败 |
| 1004 | `AlreadyAuthenticated` 重复登录同一连接 |
| 1005 | `ResumeFailed` 重连失败（凭证错/已过期/会话不存在） |
| 1006 | `RouteNotFound` 没有匹配的路由 |
| 1007 | `ServiceUnavailable` 目标服务无存活节点 |
| 1008 | `ServiceTimeout` 后端超时未回 |
| 1009 | `RateLimited` 上行限流 |
| 1010 | `PayloadTooLarge` 超过 `WS_MAX_PAYLOAD` |
| 1011 | `DuplicateSeq` 重复的序号 |
| 1500 | `Internal` 网关内部错误 |

## WebSocket 关闭码

私有区间 4000–4999：

| 码 | 含义 | 客户端应做什么 |
| --- | --- | --- |
| 4001 | 认证超时（连上不发 `Auth`） | 重连并登录 |
| 4002 | 认证失败 | **不要重试**，回登录流程换 token |
| 4003 | 顶号被踢 | **不要重试**，提示"账号在其他设备登录" |
| 4004 | 管理员/服务踢出 | 不要重试 |
| 4005 | 重连失败 | 清掉 `sid`/`rt`，用 token 重新登录 |
| 4006 | 限流 / 背压过大 | 退避后重连 |
| 4007 | 协议错误 | 修客户端 |
| 4008 | 服务端停机 | 立刻重连（会落到别的 gate 并 `Resume`） |
| 4009 | 空闲超时（心跳丢失） | 重连 + `Resume` |
| 4010 | 节点连接数已满 | 退避后重连 |
| 4011 | 同一会话被更新的连接取代 | 忽略（这条连接已经不是当前的了） |

## 客户端状态机

```
                    ┌──────────┐
        ┌──────────▶│   idle   │
        │           └────┬─────┘
        │                │ connect
        │                ▼
        │        ┌───────────────┐   有 sid+rt ?
        │        │  connecting   │──────┬──────────┐
        │        └───────────────┘     否│         │是
        │                                ▼         ▼
        │                          ┌────────┐ ┌────────┐
        │                          │  Auth  │ │ Resume │
        │                          └───┬────┘ └───┬────┘
        │                              │AuthAck   │ResumeAck
        │                              ▼          ▼
        │                          ┌──────────────────┐
        │  socket 断 / 4008 / 4009 │      ready       │
        └──────────────────────────┤ 心跳 / 收发业务包 │
                                   └────────┬─────────┘
                     4002/4003/4004 (终止)  │
                                            ▼
                                       ┌─────────┐
                                       │ closed  │
                                       └─────────┘
```

关键规则：

1. **只在没有 `sid`/`rt` 时才发 `Auth`**；有就发 `Resume`，否则会触发顶号把自己踢下线。
2. 收到 `Response`/`Push` 时，`seq <= lastRecvSeq` 的**直接丢弃**（重放可能与已收到的重叠）。
3. `Resume` 的 `ack` 填 `lastRecvSeq`；心跳也带 `ack`，让网关及时释放缓冲。
4. 重连成功后，只重发 `cseq > cack` 的在途请求。
5. `resync:true` 时把 `lastRecvSeq` 重置为 `ResumeAck.seq`，并重新拉取游戏状态。
6. `Kick` 的 `resumable:true`（停机）→ 重连；否则清掉会话回登录页。

7. 编码在连接建立时就定了，重连时用同一种；`GateClient` 的 `codec` 选项即此。

参考实现：[src/client/gateClient.ts](../src/client/gateClient.ts)。

## 编码细节

### 协商

优先级：**offered 子协议** → **`?codec=`** → **`WS_DEFAULT_CODEC`**。

- 子协议命中时，服务端在握手响应里回显选中的那个，客户端可以确认；
- 客户端只 offer 了服务端不认识的子协议时，服务端不选任何子协议，按默认编码服务 —— 按 RFC 6455，一个真的要求子协议的客户端此时必须自行断开，所以不会出现"要 protobuf 却收到 JSON"；
- `?codec=` 指定了未启用的编码时，握手直接返回 **HTTP 400**（查询参数没有客户端侧兜底，必须服务端拒绝）；
- 别名：`pb`、`proto` 都等价于 `protobuf`。

### JSON 编码

一帧一个 JSON 对象，`t` 字段是包类型。不透明字节载荷用 base64 承载，并置 `b: 1` 标记：

```json
{ "t": 10, "seq": 7, "cmd": "game.state", "d": "CAEQAg==", "b": 1 }
```

没有 `b` 时 `d` 就是普通 JSON 值。

### protobuf 编码

schema 是 [proto/gate.proto](../proto/gate.proto)，`package gate.v1`。信封用 `oneof`，**oneof 的 tag 本身就是包类型**，所以没有单独的 `t` 字段；tag 1–7 都只占一个字节：

```proto
message ClientEnvelope {
  oneof body { Auth auth = 1; Resume resume = 2; Heartbeat heartbeat = 3;
               Request request = 4; Notify notify = 5; }
}
message ServerEnvelope {
  oneof body { AuthAck auth_ack = 1; ResumeAck resume_ack = 2; HeartbeatAck heartbeat_ack = 3;
               Response response = 4; Push push = 5; Kick kick = 6; Error error = 7; }
}
```

**`d` 是 `bytes`**：网关不解析游戏载荷这一点没有因为换编码而改变。客户端和后端服务共享自己的 schema，加消息不用动网关、不用改 gate.proto。

proto3 没有字段存在性，所以下列默认值一律按"缺省"处理：

| 字段 | 0 / 空 的含义 |
| --- | --- |
| `Request.id` | 非法（必须 ≥ 1），解码时报错 |
| `cseq` | 客户端不参与上行去重 |
| `Heartbeat.ack` | 尚未收到任何下行包 |
| `d`（零长度） | 无载荷（JSON 编码同样按此处理，保证两种编码下服务端行为一致） |
| `Response.e` | `ErrorCode.Ok`，即成功 |
| `resync` / `resumable` | false |
| `Error.id` | 该错误不归属于某个具体请求 |

未知字段会被跳过，所以新客户端加了字段也不会打挂老网关（有测试覆盖）。

`src/framework/protocol/pb/descriptor.ts` 是由 `gate.proto` 生成并**提交进仓库**的 JSON 描述符：运行时 `Root.fromJSON` 直接加载，不读文件，`src/` / `dist/` / 容器镜像里的路径都不用操心。改完 `.proto` 跑 `npm run proto:gen`，忘了跑会有单元测试报漂移。

### 跨编码的载荷转换

服务端返回什么，取决于它自己（`Buffer` 或 JSON 值），与客户端用什么编码无关。网关负责转换：

| 服务返回 | → protobuf 客户端 | → JSON 客户端 |
| --- | --- | --- |
| `Buffer` | `d` = 原始字节，`d_json` = false | `d` = base64 字符串，`b` = 1 |
| JSON 值 | `d` = UTF-8 JSON 字节，`d_json` = **true** | `d` = 原样 JSON |

客户端方向同理。`d_json` 就是告诉 protobuf 客户端"这段字节是 JSON，别喂给你的 protobuf 解析器"。

## 集群内部消息

gate ↔ 后端服务，同样走 Redis pub/sub，格式见 [src/framework/protocol/internal.ts](../src/framework/protocol/internal.ts)。

集群内部一直是 JSON（编码只是客户端与网关之间的事）。载荷走两个互斥字段：`d` 放 JSON 载荷，`db` 放不透明字节的 base64。别直接读这两个字段，用 [src/framework/protocol/payload.ts](../src/framework/protocol/payload.ts) 的 `toInternal` / `fromInternal`；`ServiceNode` 已经帮你分好了 `ctx.payload`（JSON）和 `ctx.payloadBytes`（Buffer）。

上行 `<prefix>:svc:<service>:<nodeId>`：

| `k` | 说明 |
| --- | --- |
| `req` | 客户端请求，服务必须回 `resp` |
| `notify` | 客户端单向通知 |
| `session` | 会话生命周期：`online` / `suspended` / `resumed` / `offline` |

下行 `<prefix>:node:<gateId>`（或 `<prefix>:node:all`）：

| `k` | 说明 |
| --- | --- |
| `resp` | 请求应答，按 `sid` + `id` 投递 |
| `push` | 定向推送，按 `sid` 或 `uid` |
| `multicast` | 一批 `uid`，各 gate 各自过滤 |
| `broadcast` | 全服广播（发到 `node:all`） |
| `kick` | 终止会话；gate 之间的顶号踢人也用这个 |

## Redis Key 布局

前缀由 `REDIS_KEY_PREFIX` 决定（默认 `gate`）。

| Key | 类型 | 内容 |
| --- | --- | --- |
| `<p>:sess:<uid>` | string | 当前账号所有者 `{uid,sid,gate,addr,since}`，TTL 由 gate 心跳续期 |
| `<p>:sid:<sid>` | string | `sid → uid` 反查 |
| `<p>:resume:<sid>` | string | 重连凭证 `{uid,sid,gate,addr,hash}`，TTL = 重连窗口 |
| `<p>:nodes` | hash | `gateId → {addr,load,ts}` |
| `<p>:svc:<service>:nodes` | hash | `nodeId → {addr,load,ts}` |
| `<p>:bind:<uid>:<service>` | string | 粘性绑定的后端 nodeId |

三段 Lua 脚本（[src/framework/redis/sessionRegistry.ts](../src/framework/redis/sessionRegistry.ts)）保证正确性：

- `claim` —— 写入新所有者并返回旧所有者，一次原子操作。这是顶号在多 gate 下不会互踢的根本原因。
- `release` —— 仅当 `sid` **和** `gate` 都匹配时才删除，会话迁移后老节点不会误删新记录。
- `touch` —— 同样双重比对后续期；返回"已被抢走"时，gate 主动断掉本地会话。
