# JD HotKey 项目架构文档

## 1. 项目概述

**JD HotKey** 是京东开源的高性能、实时热键探测与分发框架。它能够在毫秒级内探测突发热数据（如热点商品、恶意用户、热点接口），并将热键推送到所有应用服务器的 JVM 内存中，从而大幅降低后端存储层的压力。

**核心特性：**
- 实时热键探测（毫秒级）
- 自动推送到所有应用服务器本地缓存
- 高性能（8 核单 Worker 可处理 16 万 key/秒）
- 水平可扩展
- 业务隔离（按 appName 隔离规则和热键）
- 规则化探测配置

**生产验证：**
- 京东 618、双 11 大促验证
- 日处理数十亿 key
- 推荐 Worker 与应用服务器比例 1:1000

---

## 2. 项目结构

```
jd-hotkey/
├── client/              # 客户端 SDK，供业务应用集成
├── common/              # 公共模块：模型、工具类、Netty 编解码、Etcd 配置
├── dashboard/           # 控制台：规则配置、监控、管理
├── worker/              # Worker 服务端：热键聚合计算与探测
├── sample/              # 示例应用，演示客户端用法
├── pom.xml              # 父 POM
└── README.md
```

### 模块职责

| 模块 | 职责 |
|------|------|
| **client** | 业务应用集成 SDK，提供热键判断与本地缓存能力 |
| **common** | 共享模型、Netty 编解码器、Etcd 配置中心交互、工具类 |
| **dashboard** | Web 控制台，管理热键规则、监控热键、管理用户和 Worker 集群 |
| **worker** | 核心计算节点，聚合 key 计数并判断是否为热键 |
| **sample** | 示例应用，展示客户端集成方式 |

---

## 3. 核心依赖与中间件

| 技术 | 版本 | 用途 |
|------|------|------|
| **Spring Boot** | 2.2.1 | Worker 和 Dashboard 的应用框架 |
| **Netty** | 4.1.42 | 客户端与 Worker 之间的高性能网络通信 |
| **Etcd** (etcd-java) | 0.0.16 | 服务发现、配置中心、Worker 协调 |
| **Caffeine** | 2.8.0 | 高性能本地缓存（替代 Guava Cache） |
| **Guava** | - | EventBus 事件总线，用于客户端内部事件驱动（规则变更、Worker 变更、热键推送通知） |
| **Disruptor** | 3.4.2 | ~~无锁环形缓冲区~~ Worker 内部高吞吐线程间通信（**注：经验证，此依赖仅存在于 Worker 模块，Client/Client JAR 中未包含。Client 端使用双缓冲 ConcurrentHashMap 替代 Disruptor**） |
| **Protostuff** | 1.7.4 | 高性能二进制序列化 |
| **Snappy** | 1.1.7.3 | ~~快速压缩/解压缩~~ **注：经验证，common 模块 POM 中声明了 snappy-java 依赖，但实际编码中 MsgEncoder/MsgDecoder 仅使用 Protostuff 序列化 + 分隔符帧，Netty Pipeline 中未添加 Snappy 压缩 Handler，Snappy 属于声明但未实际使用的依赖** |
| **FastJSON** | 1.2.83 | JSON 序列化（安全修复版本） |
| **Hutool** | 5.1.0 | 工具类库 |
| **JJWT** | 0.9.1 | JWT 令牌生成与验证，Dashboard 用户认证 |
| **Apache POI** | 4.0.1 | Excel 导出，Dashboard 数据报表 |
| **PageHelper** | - | MyBatis 分页插件，Dashboard 列表分页 |
| **Thymeleaf** | - | 模板引擎，Dashboard 页面渲染 |
| **MySQL** | - | Dashboard 数据持久化 |
| **MyBatis** | - | Dashboard ORM |

---

## 4. 整体架构与数据流

### 4.1 架构全景

```
                        ┌─────────────┐
                        │   Dashboard  │
                        │  (Web 控制台) │
                        └──────┬──────┘
                               │ 规则下发 / 热键记录
                               ↓
                        ┌─────────────┐
                        │    Etcd      │
                        │ (配置/协调中心)│
                        └──────┬──────┘
                               │ 服务发现 / 规则变更通知
          ┌────────────────────┼────────────────────┐
          ↓                    ↓                    ↓
   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
   │   Worker 1   │   │   Worker 2   │   │   Worker N   │
   │ (热键计算节点) │   │ (热键计算节点) │   │ (热键计算节点) │
   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
          │ Netty 推送      │ Netty 推送      │ Netty 推送
          ↓                  ↓                  ↓
   ┌──────────────────────────────────────────────────┐
   │              应用服务器集群 (Client SDK)            │
   │  ┌─────────┐  ┌─────────┐  ┌─────────┐           │
   │  │ App A   │  │ App B   │  │ App C   │  ...      │
   │  │Caffeine │  │Caffeine │  │Caffeine │           │
   │  │本地缓存  │  │本地缓存  │  │本地缓存  │             │
   │  └─────────┘  └─────────┘  └─────────┘           │
   └──────────────────────────────────────────────────┘
```

### 4.2 热键探测核心流程

```
┌─────────────────┐
│   业务应用请求    │
└────────┬────────┘
         │ 1. 查询本地 Caffeine 缓存
         │    命中 → 直接返回（纳秒级）
         │    未命中 ↓
┌────────┴─────────────────┐
│  JdHotKeyStore.isHotKey() │
│  - 将 key 放入待发送队列   │
└────────┬─────────────────┘
         │ 2. 批量发送（500ms 周期）
         │    Hash 路由到对应 Worker
         ↓
┌─────────────────────────────────┐
│  Worker Netty Server            │
│  Filter Chain:                  │
│    AppNameFilter → HeartBeatFilter│
│    → HotKeyFilter → KeyCounterFilter│
└────────┬────────────────────────┘
         │ 3. 写入 Disruptor RingBuffer（仅 Worker 模块）
         ↓
┌─────────────────────────────────┐
│  KeyConsumer (多线程消费)        │
│  → KeyListener.newKey()         │
│    - 获取/创建滑动窗口            │
│    - 累加当前时间片计数            │
│    - 判断是否达到阈值              │
└────────┬────────────────────────┘
         │ 4. 判定为热键
         ↓
┌─────────────────────────────────┐
│  Pusher 推送                     │
│  ├─ AppServerPusher → 推送客户端 │
│  └─ DashboardPusher → 推送控制台 │
└─────────────────────────────────┘
         │ 5. 客户端收到热键通知
         ↓
┌─────────────────────────────────┐
│  所有客户端本地缓存该热键            │
│  后续请求直接命中本地缓存            │
└─────────────────────────────────┘
```

---

## 5. 网络通信协议（Netty）

### 5.1 消息类型

| 类型 | 值 | 方向 | 用途 |
|------|----|------|------|
| APP_NAME | 1 | Client→Worker | 注册应用名称 |
| REQUEST_NEW_KEY | 2 | Client→Worker | 发送 key 进行热键探测 |
| RESPONSE_NEW_KEY | 3 | Worker→Client | 推送探测到的热键 |
| REQUEST_HIT_COUNT | 7 | Client→Worker | 上报命中统计 |
| REQUEST_HOT_KEY | 8 | Worker→Dashboard | 发送热键到控制台 |
| PING | 4 | Client→Worker | 心跳检测 |
| PONG | 5 | Worker→Client | 心跳响应 |
| EMPTY | 6 | - | 空消息 |

### 5.2 编解码

- **序列化：** Protostuff（高性能二进制序列化）
- **帧分隔符：** `\r\n`
- **最大帧长度：** `Constant.MAX_LENGTH`
- **编码器：** `MsgEncoder`（common 模块）
- **解码器：** `MsgDecoder`（common 模块）

### 5.3 客户端连接管理

- 每个 Worker 维护一个 Netty 长连接
- Hash 路由：`hash(key) % workerCount`，相同 key 始终路由到同一 Worker
- 心跳机制：每 30 秒发送 PING（IdleStateHandler）
- 断线重连：`WorkerRetryConnector` 负责自动重连

---

## 6. Etcd 配置中心

### 6.1 路径结构

```
/jd/
├── apps/              # 已注册的应用名称
├── workers/           # Worker 注册信息（临时节点）
├── dashboard/         # Dashboard 注册信息
├── rules/             # 各应用的热键探测规则
├── whiteList/         # 白名单 key（忽略探测）
├── count/             # 各应用的客户端连接数
├── hotkeys/           # 探测到的热键（带 TTL）
├── keyRecords/        # 热键记录（供 Dashboard 查询）
├── caffeineSize/      # Caffeine 缓存大小配置
├── totalKeyCount/     # Worker key 处理统计
├── bufferPool/        # 直接内存缓冲区配置
├── keyHitCount/       # 热键命中/未命中统计
├── logOn/             # 日志开关
├── clearCfg/          # 历史数据清理配置（天数）
└── appCfg/            # 应用专属配置
```

### 6.2 关键机制

- **临时节点：** Worker 和 Dashboard 使用临时节点注册，宕机后自动摘除
- **Watch 机制：** 客户端监听规则、Worker 列表等变化，实时感知
- **TTL：** 热键节点设置 TTL，过期自动清理
- **强一致性：** Etcd 保证顺序一致性

---

## 7. 核心算法 — 滑动窗口

**位置：** `worker.tool.SlidingWindow`

### 7.1 设计

- 使用 AtomicLong 数组实现线程安全计数
- 环形缓冲区（大小 = 2 × windowSize）
- 每个时间片有可配置的时长

### 7.2 算法步骤

```
1. 收到 key 请求
2. 获取/创建该 key 的滑动窗口
3. 将计数累加到当前时间片
4. 清除过期时间片（超出窗口范围）
5. 求和所有活跃时间片的计数
6. 若总和 ≥ 阈值 → 判定为热键！
7. 推送热键到所有客户端并缓存（带 TTL）
```

### 7.3 规则配置（KeyRule）

| 参数 | 说明 | 示例 |
|------|------|------|
| key | key 前缀或完整 key | `product-` |
| prefix | 是否前缀匹配 | true |
| interval | 时间窗口大小（秒） | 2 |
| threshold | 热键判定阈值 | 10 |
| duration | 缓存持续时间（秒） | 60 |
| desc | 规则描述 | 商品热键 |

**规则匹配逻辑：**
1. 精确匹配优先
2. 无精确匹配则检查前缀规则
3. 通配符 `*` 匹配所有 key（默认规则）

---

## 8. 各模块详细设计

### 8.1 Client 模块

#### 启动入口：ClientStarter

```java
ClientStarter starter = new ClientStarter.Builder()
    .setAppName("my-app")
    .setEtcdServer("http://etcd:2379")
    .setPushPeriod(500L)       // 批量推送周期（ms）
    .setCaffeineSize(200000)   // 本地缓存大小
    .build();
starter.startPipeline();
```

**初始化流程：**
1. 设置 Caffeine 最大容量
2. 初始化 Etcd 配置中心连接
3. 启动定时 key 推送任务
4. 启动 Worker 重连器
5. 注册 EventBus 订阅者：
   - `WorkerChangeSubscriber`：处理 Worker 变更
   - `ReceiveNewKeySubscribe`：监听热键推送
   - `KeyRuleHolder`：监听规则变更

#### 核心 API：JdHotKeyStore

| 方法 | 用途 |
|------|------|
| `boolean isHotKey(String key)` | 判断 key 是否为热键，非热键则发送探测 |
| `Object get(String key)` | 获取热键缓存值（不触发探测） |
| `void smartSet(String key, Object value)` | 仅在 key 已为热键时设置值 |
| `Object getValue(String key)` | 获取值，非热键则发送探测 |
| `void remove(String key)` | 移除本地缓存并通知集群 |
| `void forceSet(String key, Object value)` | 强制设置值（无视热键状态） |

#### 内部组件

```
ClientStarter
├── EtcdStarter          # 监听 Etcd 变更
├── PushSchedulerStarter # 批量推送 key 到 Worker
├── WorkerInfoHolder     # 维护已连接 Worker 列表
├── KeyRuleHolder        # 缓存探测规则
├── CacheFactory         # 管理本地缓存
└── EventBusCenter       # 事件驱动架构（Guava EventBus）
```

### 8.2 Worker 模块

#### 启动与初始化

**Spring Boot 应用：** `WorkerApplication`

**初始化流程（InitStarter）：**
1. 初始化线程池（大小根据 CPU 核心数自动检测）
2. 启动 Netty Server（默认端口 11111）
3. 在 Etcd 注册 Worker（临时节点）
4. 启动 Etcd Watcher 监听规则、Worker 列表等

**配置项（application.yml）：**
```yaml
netty:
  port: 11111
  heartBeat: 10
  timeOut: 5000
thread:
  count: 0          # 0 = 自动检测 CPU 核心数
caffeine:
  minutes: 1
disruptor:
  bufferSize: 2     # 必须为 2 的幂，实际大小 = 2^bufferSize
etcd:
  server: http://localhost:2379
  workerPath: default
```

#### Key 处理管线

**Netty Filter Chain：**
```
NodesServerHandler
├── AppNameFilter     # 验证应用名称
├── HeartBeatFilter   # 处理 PING/PONG
├── HotKeyFilter      # 处理 remove 请求
└── KeyCounterFilter  # 委托给 KeyProducer
```

**Disruptor 模式：**（仅 Worker 模块，Client 端未使用）
- 高性能无锁线程间通信
- 环形缓冲区，大小可配置（2 的幂）
- 多消费者（线程数 = CPU 核心数）
- 生产者：`KeyProducer`（来自 Netty 线程）
- 消货者：`KeyConsumer`（处理并探测）

#### 热键推送器

| 推送器 | 批量周期 | 用途 |
|--------|---------|------|
| AppServerPusher | 10ms | 高频推送热键到所有客户端 |
| DashboardPusher | 1s | 低频推送热键记录到控制台 |

### 8.3 Common 模块

**核心模型：**

| 模型 | 用途 |
|------|------|
| HotKeyModel | 热键数据（appName, key, keyType, createTime, duration, remove） |
| HotKeyMsg | Netty 消息封装（type, HotKeyModel 列表） |
| KeyRule | 探测规则配置（prefix, interval, threshold, duration） |
| KeyCountModel | 带 count 的 key，用于批量上报 |

**工具类：**
- `HotKeyPathTool`：Etcd 路径工具
- `IdGenerater`：唯一 ID 生成器
- `NettyIpUtil`：IP 地址工具
- `ProtostuffUtils`：序列化封装

### 8.4 Dashboard 模块

**技术栈：** Spring Boot + Thymeleaf + MyBatis + MySQL

**核心功能：**
1. **用户管理：** 创建用户并关联 appName
2. **规则配置：** 为各应用定义热键探测规则
3. **热键监控：** 实时查看探测到的热键
4. **统计信息：** 命中/未命中率、Worker 性能
5. **Worker 集群管理：** 查看已连接的 Worker 节点
6. **历史清理：** 可配置的数据保留策略

**主要 Controller：**

| Controller | 功能 |
|-----------|------|
| AppCfgController | 应用配置管理 |
| RuleController | 热键规则管理 |
| KeyController | 热键查询 |
| WorkerController | Worker 集群信息 |
| UserController | 用户管理 |
| ClearController | 数据清理 |
| ChangeLogController | 变更审计日志 |

---

## 9. Worker 集群与高可用

### 9.1 Worker 注册

- 每个 Worker 在 Etcd 注册为临时节点：`/jd/workers/{workerPath}/{ip:port}`
- Worker 宕机后，Etcd 在 TTL 内自动摘除节点
- 客户端 Watch Worker 列表变化，实时感知

### 9.2 Key 路由

- **Hash 路由：** `hash(key) % workerCount`
- 相同 key 始终路由到同一 Worker，确保计数准确
- Worker 宕机后，key 自动重新哈希到剩余 Worker

### 9.3 故障处理

| 故障场景 | 处理方式 |
|---------|---------|
| Worker 宕机 | Etcd 自动摘除，客户端重连剩余 Worker，key 重哈希 |
| 客户端宕机 | Worker 移除对应 Channel，停止推送 |
| Etcd 宕机 | 已有规则和 Worker 列表在本地缓存，短暂容忍 |
| 网络分区 | Netty 心跳超时断开，自动重连 |

### 9.4 扩展性

- **水平扩展：** 增加 Worker 节点即可
- **线性性能提升：** 每个 Worker 处理 key 的子集
- **推荐比例：** 1 个 Worker 对应 1000 个应用服务器

---

## 10. 性能特征

### 10.1 Worker 性能基准

| 配置 | 吞吐量 |
|------|--------|
| 8 核 | 160,000 key/秒 |
| 16 核 | 300,000+ key/秒（稳定） |
| 混合推送 | 100,000 推送/秒 |
| 纯推送 | 400,000 ~ 600,000 推送/秒 |

### 10.2 性能优化手段

| 手段 | 说明 |
|------|------|
| Disruptor 无锁队列 | Worker 内部高吞吐线程间通信（Client 端未使用，采用双缓冲替代） |
| 滑动窗口 | 高效的基于时间维度的计数算法 |
| 批量处理 | 客户端批量发送 key，Worker 批量推送热键 |
| Caffeine 本地缓存 | 高性能 W-TinyLFU 缓存淘汰策略 |
| Netty NIO | 非阻塞网络 I/O |
| Protostuff 序列化 | 高速二进制序列化 |
| Hash 路由 | 相同 key 始终到同一 Worker，避免跨节点计数 |

### 10.3 客户端开销

- CPU 占用极低（批量发送、本地缓存）
- 可配置推送周期（默认 500ms）
- 内存占用取决于 Caffeine 大小（默认 200,000）
- Netty 异步非阻塞调用

---

## 11. 典型应用场景

### 11.1 热点商品缓存

**问题：** 突发流量导致热门商品请求压垮 Redis

**方案：** 探测热商品 key，缓存到所有服务器 JVM 内存

**效果：** Redis 流量降低 50%+（京东生产数据）

### 11.2 恶意用户检测

**问题：** 刷单/爬虫用户高频请求

**方案：** 探测热用户 ID，在入口层限流

**效果：** 保护系统免受暴力攻击

### 11.3 热点接口保护

**问题：** 特定 API 突发流量

**方案：** 探测热接口 key，触发熔断/降级

**效果：** 流量尖峰下系统稳定

### 11.4 黑名单本地缓存

**问题：** 黑名单查询频繁访问 Redis

**方案：** 推送黑名单条目为热键

**效果：** 本地缓存命中，无网络开销

---

## 12. 使用示例

### 12.1 热键判断（限流场景）

```java
if (JdHotKeyStore.isHotKey("user_" + userId)) {
    // 该用户为热用户，触发限流
    return "请稍后再试";
}
// 正常处理
```

### 12.2 热数据缓存

```java
Object product = JdHotKeyStore.getValue("product_" + productId);
if (product != null) {
    return product;  // 本地缓存命中
}
// 从数据库/Redis 获取
product = fetchFromDB(productId);
JdHotKeyStore.smartSet("product_" + productId, product);
return product;
```

### 12.3 Spring Boot 集成

```java
@Component
public class HotKeyInit {
    @Value("${etcd.server}")
    private String etcdServer;

    @Value("${spring.application.name}")
    private String appName;

    @PostConstruct
    public void init() {
        ClientStarter starter = new ClientStarter.Builder()
            .setAppName(appName)
            .setEtcdServer(etcdServer)
            .setCaffeineSize(200000)
            .build();
        starter.startPipeline();
    }
}
```

---

## 13. 设计决策与权衡

| 决策 | 原因 |
|------|------|
| Worker 聚合计数（非客户端本地） | 单机无法感知全局热度，需集中聚合 |
| 自研 Worker + 滑动窗口（非 Redis 聚合） | Redis 无法支撑 15 万+ 写/秒，且延迟更高 |
| Etcd（非 ZooKeeper） | ZK 在高负载下性能不稳定，Etcd 支持 TTL 和 gRPC |
| Netty 直推（非 Etcd Pub/Sub） | 5000+ 客户端Watcher 会导致 Etcd 过载，直推延迟更低 |
| 不持久化 key 计数 | 持久化拖慢探测速度；热键本身就是高频的，少量丢失可接受 |

---

## 14. 运维与监控

### 14.1 关键指标

- Worker：每秒处理 key 总数
- Worker：每秒探测到的热键数
- 客户端：缓存命中/未命中率
- 客户端：本地缓存大小
- Etcd：集群健康状态

### 14.2 日志管理

- 通过 Etcd 路径 `/jd/logOn` 控制日志开关
- 热键探测在 INFO 级别记录
- 大促期间可关闭日志以降低 I/O

### 14.3 容量规划

| 资源 | 建议 |
|------|------|
| Worker | 起步 1 个 Worker / 1000 个应用服务器 |
| Caffeine 大小 | 取决于热键基数，默认 200,000 |
| Etcd | 标准集群（3~5 节点）保证可靠性 |
| 网络 | 确保客户端与 Worker 之间低延迟 |
