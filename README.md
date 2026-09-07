# gate-ts

游戏服网关（gate）服务，TypeScript + Node.js 实现。负责终结客户端 WebSocket 连接、校验 JWT、维护会话，并按消息类型把流量转发到 game / chat 等后端服务。

网关本身**不含游戏逻辑**：消息体（`d` 字段）从不被解析，加一条新消息只需要加一条路由。

## 需求对照

| 需求 | 实现 |
| --- | --- |
| 1. TS 编写，Node 运行 | TypeScript 5 strict，Node ≥ 18，零编译期魔法 |
| 2. 客户端 WebSocket | [`ws`](https://github.com/websockets/ws)，**JSON / protobuf 双编码**（握手时协商），服务端 ping/pong 保活 + 背压保护 |
| 3. gate 多开、水平扩展 | 进程完全对等，状态在 Redis；加机器即扩容，无需客户端改动、无需会话粘滞 |
| 4. 连接时校验 JWT | `Auth` 包携带其他服务签发的 token；支持 HS256 / RS256 / ES256、iss/aud/exp 校验 |
| 5. 维护连接 + 按类型转发 | 路由表按 `cmd` 前缀/精确匹配映射到服务，玩家对后端节点粘性绑定 |
| 6. 断线重连 | 会话在掉线后保留（默认 60s），重连后按序列号重放丢失的下行包，上行包去重防重复执行 |
| 7. 顶号踢人 | Redis Lua 原子抢占账号所有权，跨 gate 精确踢掉旧会话 |
| 8. 允许用 Redis | Redis 承担会话注册表 + 节点发现 + 集群消息总线（pub/sub），无其他中间件 |
| 9. 框架自选 | 自研网关骨架 + `ws` / `ioredis` / `jsonwebtoken` / `pino` / `protobufjs`，依赖 6 个 |

## 架构

```
                    ┌──────────────┐
   clients ────────▶│  LB (nginx)  │   least_conn，无需 sticky
                    └──────┬───────┘
                  ┌────────┴────────┐
                  ▼                 ▼
            ┌──────────┐      ┌──────────┐
            │  gate-1  │      │  gate-2  │   ← 对等，可任意增减
            └────┬─────┘      └─────┬────┘
                 │   ①会话注册表      │
                 │   ②节点发现        │
                 │   ③消息总线        │
                 └────────┬──────────┘
                          ▼
                    ┌───────────┐
                    │   Redis   │
                    └─────┬─────┘
                 ┌────────┴────────┐
                 ▼                 ▼
           ┌──────────┐      ┌──────────┐
           │   game   │      │   chat   │   ← 各自也可多开
           │  node×N  │      │  node×N  │
           └──────────┘      └──────────┘
```

Redis 承担三件事：

1. **会话注册表** —— `sess:<uid>` 记录"这个账号当前由哪个 gate 的哪个会话持有"，是顶号判定的唯一依据；
2. **节点发现** —— gate 与后端服务各自心跳注册，转发时只挑活着的节点；
3. **消息总线** —— gate 订阅 `node:<gateId>`（自己的收件箱）和 `node:all`（全局广播）两个频道；上行按服务节点定向 publish，扇出为零。

## 快速开始

```bash
npm install
cp .env.example .env          # 至少改掉 JWT_SECRET
docker compose up -d redis    # 或用你本地已有的 redis
```

四个终端分别起：

```bash
npm run dev            # gate: ws://127.0.0.1:7000/ws, admin :8000
npm run mock:game      # 模拟 game 服
npm run mock:chat      # 模拟 chat 服
npm run client         # 冒烟客户端：登录 → 转发 → 推送
```

多开一个 gate 验证水平扩展：

```bash
GATE_ID=gate-2 WS_PORT=7001 ADMIN_PORT=8001 ADVERTISE_ADDR=127.0.0.1:7001 npm run dev
```

整套 docker 编排（2 gate + nginx + game + chat + redis）：

```bash
docker compose up --build     # 客户端连 ws://127.0.0.1:7080/ws
```

## 协议

两种编码，一套语义：**JSON**（默认，好调试）和 **protobuf**（省带宽），由客户端在握手时协商，同一个 gate 上两种客户端可以共存。完整包结构见 [`docs/protocol.md`](docs/protocol.md)，类型定义见 [src/protocol/packet.ts](src/protocol/packet.ts)，protobuf schema 见 [proto/gate.proto](proto/gate.proto)。

| 值 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| 1 / 2 | `Auth` / `AuthAck` | ↑ / ↓ | JWT 登录，返回 `sid` + 重连凭证 `rt` |
| 3 / 4 | `Resume` / `ResumeAck` | ↑ / ↓ | 断线重连，返回将重放的包数 |
| 5 / 6 | `Heartbeat` / `HeartbeatAck` | ↑ / ↓ | 保活，顺带上报已收到的 `seq` |
| 7 / 8 | `Request` / `Response` | ↑ / ↓ | 一问一答，`id` 对应 |
| 9 | `Notify` | ↑ | 单向上行，无回包 |
| 10 | `Push` | ↓ | 服务端主动推送 |
| 11 | `Kick` | ↓ | 会话终止（顶号 / 封禁 / 停机） |
| 12 | `Error` | ↓ | 协议或传输层错误 |

### 编码选择

protobuf 只用在**信封**上，`d` 字段仍然是 `bytes` —— 网关照旧不解析游戏消息体，客户端和后端服务共享自己的 schema，加消息不用动网关。

客户端有两种方式指定编码，优先级从高到低：

```js
// 1. WebSocket 子协议（推荐）：服务端会回显选中的协议，不可能协商错
new WebSocket('ws://gate:7000/ws', 'gate.pb.v1')   // 或 'gate.json.v1'

// 2. 查询参数（某些引擎不方便设子协议）
new WebSocket('ws://gate:7000/ws?codec=pb')

// 3. 都不指定 → 用 WS_DEFAULT_CODEC
```

**明确要求一个未启用的编码会被拒绝握手**（HTTP 400），而不是悄悄降级 —— 一个 protobuf 客户端收到 JSON 会一路错到很深的地方才炸。子协议方式还有 RFC 6455 的客户端侧兜底：服务端没选中任何子协议时，客户端必须自己断开。

载荷（`d`）的跨编码规则，保证服务端行为与客户端编码无关：

| 载荷 | 发给 protobuf 客户端 | 发给 JSON 客户端 |
| --- | --- | --- |
| 二进制（服务返回 `Buffer`） | 原样放进 `bytes` | base64 字符串 + `b:1` 标记 |
| JSON（服务返回对象） | UTF-8 JSON 放进 `bytes` + `d_json:true` | 原样 |

空载荷与零长度载荷不可区分（proto3 如此），两种编码统一按"无载荷"处理。

实测单包大小（`d` 为 16 字节二进制载荷时，JSON 还要额外付 base64 的代价）：

| 包 | JSON | protobuf | 省 |
| --- | --- | --- | --- |
| `Request`（16B 二进制载荷） | 84 | 37 | 56% |
| `Request`（无载荷） | 48 | 20 | 58% |
| `Response`（16B 二进制载荷） | 66 | 26 | 61% |
| `Push`（16B 二进制载荷） | 76 | 35 | 54% |
| `Heartbeat` | 19 | 5 | 74% |
| `HeartbeatAck` | 26 | 9 | 65% |
| `Auth`（200B token） | 218 | 206 | 6% |

改 [proto/gate.proto](proto/gate.proto) 后要跑 `npm run proto:gen` 重新生成描述符（有单元测试防止两者漂移）。描述符是提交进仓库的，运行时不读 `.proto` 文件，省掉 `src/` / `dist/` / 镜像之间的路径麻烦。

登录与转发：

```
client                     gate                      game
  │── Auth{token} ─────────▶│
  │                         │ 验证 JWT，Redis 原子抢占账号
  │◀── AuthAck{sid,rt,rw} ──│
  │── Request{id:1,         │
  │      cmd:"game.move"} ─▶│ 查路由表 → game，挑粘性节点
  │                         │── req ──────────────────▶│
  │                         │◀── resp ─────────────────│
  │◀── Response{id:1,seq:1}─│
  │◀── Push{seq:2} ─────────│◀── push ─────────────────│
```

### 断线重连

下行包（`Response` / `Push`）都带自增 `seq`，网关按会话保留最近 N 个（默认 256）在环形缓冲里；上行包带自增 `cseq`。

```
   ×  网络断开
  │── (socket 断) ────────▶│ 会话转入 suspended，继续缓冲下行包
  │                        │ 60s 内可重连，超时才真正销毁
  │── Resume{sid,rt,ack:3}▶│ 校验 rt（sha256 比对，常量时间）
  │◀── ResumeAck{replay:2, │
  │        cack:7, rt:新的}─│ 重连凭证一次一换
  │◀── Push{seq:4} ────────│ 重放客户端漏掉的包
  │◀── Push{seq:5} ────────│
  │── Request{cseq:8} ────▶│ 客户端只重发 cseq > cack 的请求
```

两个方向都不会重复：

- **下行**：客户端上报 `ack`，网关只重放 `seq > ack` 的包；客户端也会丢弃 `seq` 不前进的包。
- **上行**：网关记录 `lastCSeq`，重发的旧包直接丢弃**不再转发给后端**——它的回包本来就在重放缓冲里。集成测试 `does not execute a request twice` 就是在断线瞬间重发请求，验证后端计数器只 +1。

如果客户端重连落到了**另一个** gate（L4 负载均衡下很常见）：新 gate 凭 Redis 里的重连凭证接管会话身份，但重放缓冲留在了老节点，于是 `ResumeAck` 带上 `resync:true`，客户端据此重新拉一次游戏状态。序列号从 0 重新计数，`seq` 字段告知客户端新基线。

滚动重启也走这条路：gate 停机时故意把重连凭证留在 Redis 里，客户端收到 `Kick{resumable:true}` 后重连到别的节点继续玩，**不掉线**。

### 顶号踢人

账号所有权由一段 Lua 脚本原子写入，"最后写入者获胜"：

```
玩家在 gate-1 在线                    同账号在 gate-2 登录
        │                                     │
        │                          claim(uid) ─┤ Lua: 写新所有者，返回旧所有者
        │                                     │
        │◀── kick{duplicate_login} ───────────┤ 通过 node:gate-1 频道
        │  Kick 包 + close 4003                │
        ▼                                     ▼
      下线                                  在线
```

两台 gate 同时收到同一账号的登录时，Redis 单线程执行保证只有一个能拿到"旧所有者"，不会互踢。此外每个 gate 会周期性 `touch` 自己持有的会话，发现所有权已被抢走（`stolen`）就主动断掉本地连接——网络分区恢复后的兜底。

`release` / `touch` 的 Lua 同时比对 `sid` **和** `gate`，所以会话迁移后老节点的清理动作绝不会误删新节点刚写的记录。

## 消息路由

路由表决定"哪类消息发给哪个服务"，支持前缀与精确匹配，精确优先、长前缀优先：

```json
{
  "routes": [
    { "prefix": "game.", "service": "game" },
    { "prefix": "game.pvp.", "service": "battle" },
    { "prefix": "chat.", "service": "chat" },
    { "cmd": "game.slow", "service": "game", "timeoutMs": 20000 },
    { "prefix": "rank.", "service": "rank", "sticky": false }
  ]
}
```

用 `ROUTES_FILE=./routes.json` 指定文件，或 `ROUTES='[...]'` 内联。`gate.` 前缀保留给网关自己（`gate.ping` / `gate.time` / `gate.whoami`）。

- `sticky`（默认 true）：同一 uid 固定路由到同一后端节点，有状态服务的内存态因此不会漂移。绑定记在 Redis，**跨 gate 生效**——玩家换 gate 重连后仍落到原来那个 game 节点。节点挂了则用 rendezvous hashing 重新挑选，只有 1/N 的玩家受影响。
- `sticky: false`：无状态服务按请求随机分摊。
- `timeoutMs`：上行请求超时，超时回 `ServiceTimeout` 而不是让客户端干等。

## 后端服务接入

服务侧用 `ServiceNode`，不需要知道任何 gate 的地址：

```ts
import { ServiceNode, ServiceError } from 'gate-ts/src/sdk/serviceNode';

const node = new ServiceNode({ service: 'game' });

node
  .on('game.enter', async (ctx) => {
    // ctx: { uid, sid, gate, cmd, payload, payloadBytes?, meta: { ip, device } }
    const state = await loadPlayer(ctx.uid);
    return { state };                       // 返回值即 Response.d
  })
  .on('game.sync', (ctx) => {
    // protobuf 客户端的载荷在 payloadBytes 里，用你自己的 schema 解
    const req = SyncReq.decode(ctx.payloadBytes!);
    return Buffer.from(SyncResp.encode(build(req)).finish());  // 返回 Buffer 即二进制回包
  })
  .on('game.move', (ctx) => {
    if (!valid(ctx.payload)) throw new ServiceError('bad move', 1001);  // → 错误回包
    return move(ctx.uid, ctx.payload);
  })
  .on('chat.', (ctx) => { /* 前缀匹配 */ })
  .onSession((ctx) => {
    // online / suspended / resumed / offline，用来预加载或落盘玩家状态
  });

await node.start();

// 主动下行，全都是集群范围的：
await node.pushToSession(ctx.gate, ctx.sid, 'game.tick', { t: 1 });  // 最快，无需查表
await node.pushToUid('player-7', 'mail.new', { n: 1 });              // 自动找到所在 gate
await node.multicast(['a', 'b'], 'guild.msg', { text: 'hi' });       // 各 gate 各自过滤
await node.broadcast('sys.notice', { text: '维护公告' });
await node.kick('player-7', 'admin', 'cheating');                    // 封号踢人
```

同一 service 起多个进程（不同 `nodeId`）即后端扩容，网关自动发现并按粘性分摊。

## 客户端 SDK

`GateClient` 实现了重连协议的客户端那一半——保存 `sid`/`rt`、跟踪 `seq`、断线后只重发未被接受的请求：

```ts
const client = new GateClient({
  url: 'ws://gate:7000/ws',
  token: jwtFromLoginServer,
  codec: 'protobuf',                   // 默认 'json'
});

client.on('ready', ({ resumed, resync }) => {
  if (resync) refetchGameState();      // 换 gate 恢复，需要重新同步
});
client.on('push', (cmd, payload) => handle(cmd, payload));
client.on('kick', (k) => { if (!k.resumable) backToLogin(); });

await client.connect();
const res = await client.request('game.move', { dx: 1 });
client.notify('game.heartbeat', {});

// protobuf 客户端可以直接收发自己 schema 编出来的字节，网关原样透传
const reply = await client.request<Buffer>('game.move', MoveMsg.encode(move).finish());
```

代码基于 `ws`（Node）；浏览器端只需把 WebSocket 构造函数换掉，协议逻辑不变。

## 运维接口

管理端口（默认 `WS_PORT + 1000`）只该暴露在内网：

| 接口 | 说明 |
| --- | --- |
| `GET /healthz` | 存活探针 |
| `GET /readyz` | 就绪探针（Redis 可用且未在停机中） |
| `GET /metrics` | Prometheus 文本格式，含在线会话、认证/重连/顶号计数、上行延迟 |
| `GET /stats` | 本节点聚合状态 JSON |
| `POST /admin/kick` | `{"uid":"...","reason":"admin"}`，集群范围踢人（自动转发到所属 gate） |
| `POST /admin/push` | `{"uid":"...","cmd":"...","d":{}}`，运维/联调推送 |
| `POST /admin/drain` | 触发优雅停机 |

`POST` 接口在设置了 `ADMIN_TOKEN` 时需要 `Authorization: Bearer <token>`。

## 配置

全部通过环境变量，见 [.env.example](.env.example) 的完整注释。最常改的几项：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `GATE_ID` | `<hostname>-<port>` | **每个进程必须唯一**，集群消息按它寻址 |
| `ADVERTISE_ADDR` | `<hostname>:<port>` | 对外地址，跨节点重连重定向用 |
| `JWT_SECRET` / `JWT_PUBLIC_KEY_FILE` | — | 二者必须有一个；生产建议用非对称密钥，网关只需公钥 |
| `SESSION_RESUME_WINDOW_MS` | 60000 | 断线后可重连的时长 |
| `SESSION_REPLAY_BUFFER` | 256 | 每会话保留的下行包数（内存 ↔ 重连成功率的取舍） |
| `SESSION_CROSS_GATE_RESUME` | true | 允许换 gate 重连；L4 负载均衡下必须开 |
| `LIMIT_MSGS_PER_SEC` / `LIMIT_BURST` | 30 / 60 | 单连接上行限流（令牌桶） |
| `WS_TRUST_PROXY` | false | 从 `X-Forwarded-For` 取客户端 IP，**仅在自己的 LB 后开启** |
| `WS_CODECS` | `json,protobuf` | 本节点提供的编码；客户端只能从这里面选 |
| `WS_DEFAULT_CODEC` | 列表第一个 | 客户端没表态时用哪个 |
| `BACKEND_REQUEST_TIMEOUT_MS` | 8000 | 上行请求超时 |
| `ROUTES_FILE` / `ROUTES` | 内置 game/chat | 路由表 |

## 测试

```bash
npm test               # 单元 + 集成（集成需要 redis，无 redis 会自动跳过）
npm run typecheck
npm run proto:gen      # 改过 proto/gate.proto 之后

# 两种编码的压测对比（--admin 会采样网关自己的字节计数器）
npm run load -- --clients 150 --rate 6 --seconds 10 --codec json --admin http://127.0.0.1:8000
npm run load -- --clients 150 --rate 6 --seconds 10 --codec pb   --admin http://127.0.0.1:8000
```

集成测试 [test/integration/cluster.test.ts](test/integration/cluster.test.ts) 在同进程里起**两个真实 gate + 两个真实后端服务 + 真实 Redis**，覆盖：JWT 通过/过期/伪造、路由命中与未命中、后端无节点、后端抛异常、上行超时、**同 gate 与跨 gate 顶号**、重连重放、请求不重复执行、**跨 gate 迁移**、伪造重连凭证、重连窗口过期、定向推送/组播/广播、服务端踢人、管理接口鉴权、**滚动重启不掉线**。

[test/integration/protobuf.test.ts](test/integration/protobuf.test.ts) 单独覆盖编码：子协议协商、JSON 与 protobuf 客户端同 gate 共存、二进制载荷双向原样透传、跨编码的载荷转换、广播同时到达两种客户端、protobuf 会话的重连重放、只开 JSON 的节点拒绝 protobuf 客户端。编码本身还有 [test/unit/codec.test.ts](test/unit/codec.test.ts) 的一致性测试——同一张包用例表跑过**每一种编码**的双向往返，另有未知字段前向兼容和描述符漂移检查。

本机参考数据（M 系列笔记本，2 gate + 2 game 节点 + Redis 全在同一台）：300 并发连接、1200 req/s 目标压力下实测 1071 req/s，p50 3ms / p95 6ms / p99 8ms，零失败零重连。

同一套压测下换编码（150 连接 × 6 req/s，载荷是 JSON，所以省的纯粹是信封）：

| 编码 | 字节/请求 | p95 | p99 |
| --- | --- | --- | --- |
| JSON | 178 | 8ms | 10ms |
| protobuf | 132 (−26%) | 6ms | 8ms |

## 生产部署注意

- **负载均衡**：nginx 配置见 [deploy/nginx.conf](deploy/nginx.conf)。用 `least_conn` 即可，不要 `ip_hash`——跨节点重连本来就支持，粘滞只会让流量倾斜。`proxy_read_timeout` 要远大于心跳间隔。
- **Redis**：会话注册表丢了等于全员掉线，生产上用主从 + Sentinel，并设 `maxmemory-policy noeviction`（这些 key 不能被淘汰）。当前实现假设单实例/主从，未适配 Cluster 的跨 slot 限制。
- **停机顺序**：`SIGTERM` → 停止接受新连接 → 从节点注册表摘除 → 等在途回包 → 发 `Kick{resumable}` → 释放会话。把 LB 的摘除时间留够（`SHUTDOWN_GRACE_MS`）。
- **JWT**：网关只验签不签发。建议 token TTL 短（分钟级）+ 非对称密钥；网关不校验吊销列表，需要的话在 `JwtVerifier` 后加一次 Redis 黑名单查询（`jti` 已解析出来）。
- **编码选择**：内网带宽不紧张、要方便抓包排查就用 JSON；移动端弱网、包频高（帧同步、位置同步）用 protobuf，并把游戏载荷也做成 protobuf——那样省的不只是信封。两者可以同时开着灰度迁移。
- **可以再加的东西**：msgpack / flatbuffers（`Codec` 是可替换接口，加一个实现 + 一个子协议名即可，一致性测试会自动覆盖它）、按 `cmd` 的细粒度限流、Redis Streams 替代 pub/sub 以获得投递保证、gate 间直连 gRPC 替代 Redis 总线（延迟更低）。

## 目录结构

```
proto/           protobuf schema（gate.proto，信封定义）
src/
  protocol/      包定义、编解码（JSON / protobuf）、协商、集群内部消息格式
    pb/          由 gate.proto 生成的描述符（提交进仓库，运行时不读文件）
  auth/          JWT 校验
  net/           WebSocket 接入层、连接对象（保活/背压/限流）
  session/       会话对象、重放缓冲、会话管理器（登录/重连/顶号/停机）
  router/        路由表、后端客户端（粘性绑定/超时跟踪）
  redis/         连接池、key 布局、会话注册表(Lua)、节点注册表、消息总线
  metrics/       计数器、管理与探针 HTTP 服务
  sdk/           ServiceNode（后端接入）、GateClient（客户端）
  gate.ts        编排：把上面这些接起来，处理每一个包
tools/           token 签发、模拟后端、冒烟客户端、压测、proto 代码生成
test/            单元 + 集成
deploy/          nginx 配置
```
