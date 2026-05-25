# JD HotKey 架构与通信详解

## 一、项目定位

京东开源的热key探测框架，用于对突发性热点数据（热商品、热用户、热接口等）进行毫秒级精准探测，并将热数据推送到所有服务端 JVM 内存，减轻后端存储压力。历经京东 618、双11 大促考验。

## 二、整体架构（4 个核心模块 + 1 示例模块）

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Dashboard  │     │   Worker    │     │    Etcd     │
│  (控制台)     │◄───►│  (计算节点)  │◄───►│ (配置中心)   │
└─────────────┘     └──────┬──────┘     └──────┬──────┘
                           │ Netty长连接          │ Watch
                    ┌──────▼──────┐        ┌─────▼──────┐
                    │   Client    │────────►│   Etcd     │
                    │ (客户端SDK)  │◄────────│  规则/热key  │
                    └─────────────┘         └────────────┘
```

## 三、模块详解

### 3.1 common — 公共模块

- **数据模型**：`HotKeyModel`（热key）、`HotKeyMsg`（Netty通信消息）、`KeyCountModel`（命中计数）
- **枚举**：`KeyType`（REDIS_KEY/REQUEST_PATH/BLACK_LIST/OTHER）、`MessageType`（8种消息类型）
- **序列化**：同时支持 FastJson 和 Protostuff（基于Protobuf），Netty编解码器 `MsgEncoder`/`MsgDecoder`
- **配置中心**：`IConfigCenter` 接口 + `JdEtcdClient` 实现，封装 etcd 操作（put/get/delete/watch/lease）
- **KeyRule**：规则定义 — key模式、前缀匹配、时间窗口、阈值、缓存时长

### 3.2 client — 客户端 SDK

业务应用引入的依赖，核心职责：**采集 key 访问 → 上报 worker → 接收热key → 本地缓存**

- **启动入口**：`ClientStarter`（Builder模式，配置 appName、etcd地址、缓存大小、推送间隔）
- **本地计数**：双缓冲收集器（`TurnKeyCollector`/`TurnCountCollector`），每500ms批量上报，避免阻塞
- **Netty 通信**：`NettyClient` 连接 worker，30s 心跳保活，断线自动重连（`WorkerRetryConnector`）
- **负载均衡**：`key.hashCode() % workerCount` 将同一 key 路由到同一 worker
- **热key接收**：`NettyClientHandler` 收到 `RESPONSE_NEW_KEY` 后，通过 `DefaultNewKeyListener` 写入本地 Caffeine 缓存
- **etcd 交互**：从 etcd 获取 worker 列表、监听规则变更、监听手动添加的热key
- **公开 API**（`JdHotKeyStore`）：

| 方法 | 用途 |
|------|------|
| `isHotKey(key)` | 判断是否热key，非热则上报探测 |
| `get(key)` | 从本地缓存取值 |
| `smartSet(key, value)` | 仅热key才缓存 |
| `getValue(key)` | isHotKey + get 整合 |
| `remove(key)` | 删除并通知集群 |

### 3.3 worker — 计算节点

核心职责：**汇聚 key 访问量 → 滑动窗口判定 → 推送热key给所有客户端**

- **Netty 服务端**：`NodesServer` 接收客户端连接，消息经 Filter 链处理后入队
- **热key判定算法**：`SlidingWindow` 滑动窗口
  - 时间分片，窗口内总访问量超过阈值即判定为热key
  - 每个key一个独立的滑动窗口实例（Caffeine缓存管理）
- **处理流水线**：Client消息 → Filter链 → 队列 → 多线程消费 → KeyListener → 滑动窗口检测 → 推送
- **推送机制**：`AppServerPusher` 每10ms批量推送热key给对应 app 的所有客户端连接
- **etcd 交互**：
  - 注册自身（带lease自动续约）
  - 监听规则和白名单变更
  - 定期上报连接数、缓存大小等指标
- **配置**：netty.port、thread.count（消费线程数，默认CPU/2）、caffeineMaxMinutes

### 3.4 dashboard — 控制台

Web 管理界面，核心职责：**规则管理 + 集群监控 + 热key查询**

- **技术栈**：Spring Boot + MyBatis + etcd + Caffeine
- **Netty 服务端**：端口 11112，接收 worker 上报的热key数据
- **规则管理**：为每个 app 配置热key规则（key前缀、阈值、窗口、缓存时长），写入 etcd + 数据库
- **集群监控**：通过 etcd watch 自动发现 worker 上下线，展示连接数、缓存大小等实时指标
- **热key记录**：历史热key查询、实时命中统计、Excel 导出
- **权限管理**：用户按 app 隔离，JWT 认证，变更审计日志

### 3.5 sample — 示例应用

演示如何接入 client SDK，包含 `ClientStarter` 初始化和 `JdHotKeyStore` 使用示例。

---

## 四、通信方式总览

| 通信链路 | 协议 | 方式 | 用途 |
|---------|------|------|------|
| Client → Worker | **Netty TCP 长连接** | 请求-响应+服务端推送 | 上报key、接收热key |
| Worker → Client | **Netty TCP 长连接** | 服务端主动推送 | 推送热key通知 |
| Worker → Dashboard | **Netty TCP 长连接** | 请求-响应 | 上报热key数据、统计数据 |
| Client → Etcd | **gRPC (HTTP/2)** | Watch监听 + 主动拉取 | 发现worker、获取规则、监听手动热key |
| Worker → Etcd | **gRPC (HTTP/2)** | Watch监听 + 注册+上报 | 注册自身、监听规则、上报指标 |
| Dashboard → Etcd | **gRPC (HTTP/2)** | Watch监听 + 读写 | 管理规则、写入手动热key、监控集群 |

---

## 五、Netty 消息协议

所有 Netty 通信使用统一的 `HotKeyMsg` 消息，序列化方式为 **Protostuff**（基于 Protobuf）：

```
┌──────────┬──────────────┬─────────────┬──────────┬───────────────┬────────────────┐
│ magic(4B)│ messageType  │  appName    │   body   │ hotKeyModels  │ keyCountModels │
│ 0x12fcf76│  (byte)      │  (String)   │ (String) │  (List)       │   (List)       │
└──────────┴──────────────┴─────────────┴──────────┴───────────────┴────────────────┘
帧分隔符: $(* *)$    最大包长: 4MB
```

**MessageType 枚举（8种）**：

| 类型 | 值 | 方向 | 用途 |
|------|---|------|------|
| `APP_NAME` | 1 | Client→Worker | 连接建立后上报自己的appName |
| `REQUEST_NEW_KEY` | 2 | Client→Worker | 上报待探测的key集合 |
| `RESPONSE_NEW_KEY` | 3 | Worker→Client | 推送检测到的热key |
| `PING` | 4 | 双向 | 心跳探测（30s间隔） |
| `PONG` | 5 | 双向 | 心跳响应 |
| `EMPTY` | 6 | Worker→Client | 空消息，保持连接 |
| `REQUEST_HIT_COUNT` | 7 | Client→Worker | 上报命中计数统计 |
| `REQUEST_HOT_KEY` | 8 | Worker→Dashboard | 上报热key数据给控制台 |

---

## 六、完整通信流程（按时间线）

### 6.1 阶段1：启动与注册

```
┌──────────────────────────────────────────────────────────────────┐
│  Worker 启动                                                      │
│  1. 启动 Netty Server (默认端口)                                   │
│  2. 连接 Etcd，注册自身节点                                        │
│     PUT /jd/workers/{workerPath}/{hostname}  (带 lease 自动续约)   │
│  3. Watch Etcd 规则变更: /jd/rules/{appName}                      │
│  4. Watch Etcd 白名单变更: /jd/whiteList/{appName}                 │
│  5. 连接 Dashboard Netty Server (端口11112)                        │
├──────────────────────────────────────────────────────────────────┤
│  Client 启动                                                      │
│  1. 连接 Etcd                                                    │
│  2. 从 Etcd 拉取 worker 列表: GET /jd/workers/{appName}           │
│  3. 逐个建立 Netty 连接到每个 Worker                               │
│     连接后立即发送 APP_NAME 消息标识身份                             │
│  4. 从 Etcd 拉取规则: GET /jd/rules/{appName}                     │
│  5. Watch Etcd worker 变更 (上下线自动感知)                         │
│  6. Watch Etcd 规则变更                                            │
│  7. Watch Etcd 手动热key: /jd/hotKeys/{appName}/                  │
├──────────────────────────────────────────────────────────────────┤
│  Dashboard 启动                                                   │
│  1. 启动 Netty Server (端口11112，接收 Worker 连接)                 │
│  2. 连接 Etcd                                                    │
│  3. Watch Etcd 所有变更 (规则/worker/热key/统计)                    │
└──────────────────────────────────────────────────────────────────┘
```

### 6.2 阶段2：运行时 key 探测（核心流程）

```
时间轴 ──────────────────────────────────────────────────────────►

  t=0ms     业务线程调用 JdHotKeyStore.isHotKey("sku__123")
            │
            ├─ 本地 Caffeine 缓存未命中 → 该key非热key
            ├─ 将 key 写入 TurnKeyCollector (双缓冲map)
            └─ 返回 false（不阻塞业务线程）

  t=500ms   PushScheduler 定时触发
            │
            ├─ 切换双缓冲，取出半秒内积累的所有key
            ├─ 按 key.hashCode() % workerCount 分组
            │  同一key路由到同一Worker（一致性hash）
            └─ 批量发送 REQUEST_NEW_KEY 消息到各Worker
               ┌──────────────────────────────────────┐
               │  NettyKeyPusher.send()                │
               │  key="sku__123" → hash → channel1    │
               │  key="user__456" → hash → channel2   │
               │  批量 writeAndFlush(hotKeyMsg)        │
               └──────────────────────────────────────┘

  Worker收到REQUEST_NEW_KEY
            │
            ├─ 经 Filter 链处理:
            │   1. HotKeyFilter: 检查是否已在热key缓存
            │   2. KeyCounterFilter: 检查是否为命中计数
            │   3. 判断白名单 → 白名单key直接跳过
            │
            ├─ 通过 Filter 后入队 → 多线程消费
            │
            ├─ KeyListener 处理:
            │   1. 查找该key对应的规则
            │   2. 在滑动窗口中累加计数
            │   3. 判定: 窗口内访问量 ≥ 阈值？
            │
            ├─ 未达阈值 → 等待后续key继续累加
            │
            └─ 达到阈值! 标记为热key
               │
               ├─ 写入 Caffeine 缓存（避免重复探测）
               ├─ 写入 Etcd: /jd/hotKeys/{appName}/{key}
               └─ 通知所有 Pusher

  t=510ms   AppServerPusher 定时触发 (每10ms)
            │
            └─ 批量推送 RESPONSE_NEW_KEY 到该appName下所有Client连接
               ┌──────────────────────────────────────────┐
               │  Worker → Client1: RESPONSE_NEW_KEY       │
               │                  HotKeyModel:             │
               │                    key="sku__123"         │
               │                    keyType=REDIS_KEY      │
               │                    remove=false           │
               │                                        │
               │  Worker → Client2: RESPONSE_NEW_KEY       │
               │  Worker → Client3: RESPONSE_NEW_KEY       │
               │  ... (推送到该app所有客户端)               │
               └──────────────────────────────────────────┘

  Client收到RESPONSE_NEW_KEY
            │
            ├─ NettyClientHandler.channelRead0() 接收
            ├─ 发布事件到 Guava EventBus
            ├─ DefaultNewKeyListener 处理:
            │   1. 查找 key 对应的规则（获取缓存时长）
            │   2. 写入本地 Caffeine 缓存（带TTL过期）
            └─ 后续请求 isHotKey("sku__123") 直接返回 true

  t=520ms   Worker 同时上报 Dashboard
            │
            └─ 通过 Worker→Dashboard Netty连接
               发送 REQUEST_HOT_KEY 消息（热key数据+统计）
```

### 6.3 阶段3：统计上报

```
每10秒触发:
  Client                              Worker
    │                                    │
    ├─ TurnCountCollector 切换缓冲        │
    ├─ 汇总: 每条规则的总访问数/热key访问数 │
    ├─ 发送 REQUEST_HIT_COUNT            │
    │  ┌─────────────────────────┐       │
    │  │ KeyCountModel:          │       │
    │  │   ruleKey="sku__"       │       │
    │  │   totalHitCount=50000   │       │
    │  │   hotHitCount=30000     │       │
    │  └─────────────────────────┘       │
    │                                    ├─ CounterConsumer 聚合
    │                                    ├─ 上报 Etcd: /jd/keyHitCount/{appName}
    │                                    └─ 上报 Dashboard
```

### 6.4 阶段4：热key过期与删除

```
方式1: 自然过期
  Caffeine 缓存 TTL 到期 → 自动淘汰 → 等待下次探测周期重新判定

方式2: 主动删除
  Dashboard 人工删除 → 写入 Etcd (带 #[DELETE]# 标记)
                      → Client Watch 感知 → 移除本地缓存

方式3: Worker 检测到 key 降温
  → 发送 RESPONSE_NEW_KEY (remove=true)
  → Client 收到后移除本地缓存
```

---

## 七、Etcd 数据目录结构

```
/jd/
├── workers/{appName}/{hostname}      # Worker注册节点（带lease，掉线自动删除）
├── rules/{appName}                    # 热key规则（JSON数组）
├── whiteList/{appName}                # 白名单key
├── hotKeys/{appName}/{key}            # 当前热key（带TTL自动过期）
├── keyHitCount/{appName}              # 命中计数统计
├── dashboards/{hostname}              # Dashboard注册节点
└── clients/{appName}/{hostname}       # Client注册节点
```

---

## 八、关键通信特性

| 特性 | 实现方式 |
|------|---------|
| **长连接保活** | Netty IdleStateHandler，30s 无读写发 PING，对端回 PONG |
| **断线重连** | `WorkerRetryConnector` 定时扫描，发现断连 channel 自动重连 |
| **负载均衡** | `key.hashCode() % workerCount`，同key同worker，保证计数准确 |
| **背压控制** | Worker 内部用队列 + 多线程消费，削峰填谷 |
| **批量发送** | Client 500ms攒批，Worker 10ms攒批推送，减少网络开销 |
| **序列化** | Protostuff（Protobuf风格），比JSON性能更高，包体更小 |
| **服务发现** | Worker注册到Etcd带lease，Client Watch感知上下线 |
| **配置分发** | 规则变更写入Etcd → 各组件Watch实时感知 |

---

## 九、关键设计决策

- **为何 worker 集中计算**：客户端机器数千台，单机访问量极低无法本地判定，必须汇聚全集群数据
- **为何 worker 直接推送**：比经 etcd 中转少一跳，延迟更低；etcd 单点无法支撑50万/s推送
- **为何选 etcd**：支持 key 过期自动删除 + 回调通知，性能优于 ZooKeeper
- **worker 挂掉影响**：hash 重分配最多丢失一个窗口期数据，极热 key 不受影响

---

## 十、一句话总结数据流向

**请求链路**：业务线程 → `JdHotKeyStore` → 双缓冲攒批 → Netty → Worker → 滑动窗口判定

**推送链路**：Worker 热key触发 → Netty → 所有Client → Caffeine本地缓存 → 后续请求直接命中
