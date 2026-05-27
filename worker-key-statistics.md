# Worker 端接收、统计待测 Key 全流程详解

## 一、整体架构与数据流

JD HotKey 采用 **Client-Worker-Dashboard** 三层架构，Client 负责采集上报，Worker 负责计算判定，Dashboard 负责展示监控。

```
                              etcd（配置中心/服务发现）
                               │
                    ┌──────────┼──────────┐
                    │          │          │
                    ▼          ▼          ▼
               Client[]    Worker[]   Dashboard
                    │          │          │
                    │  REQUEST_NEW_KEY    │
                    │ ──────────────────>│
                    │  (待测key, 500ms)   │
                    │                    │
                    │  REQUEST_HIT_COUNT │
                    │ ──────────────────>│
                    │  (命中统计, 10s)    │
                    │                    │
                    │  RESPONSE_NEW_KEY  │
                    │ <──────────────────│
                    │  (热key推送, 10ms)  │
                    │                    │
```

### 消息类型（MessageType）

| 类型 | 方向 | 说明 |
|------|------|------|
| `APP_NAME` | Client → Worker | 连接建立时上报应用名 |
| `REQUEST_NEW_KEY` | Client → Worker | 待测 key 批量上报 |
| `REQUEST_HIT_COUNT` | Client → Worker | key 命中计数统计上报 |
| `RESPONSE_NEW_KEY` | Worker → Client | 热key判定结果推送 |
| `PING / PONG` | 双向 | 心跳保活 |

---

## 二、Client 端采集与上报（Worker 接收的前置流程）

### 2.1 入口：业务线程调用 isHotKey / getValue

```java
// JdHotKeyStore.java
public static boolean isHotKey(String key) {
    if (!inRule(key)) return false;           // 1. 规则过滤
    boolean isHot = isHot(key);                // 2. 查本地Caffeine缓存
    if (!isHot) {
        HotKeyPusher.push(key, null);          // 3a. 非热key → 上报待测
    } else {
        if (isNearExpire(valueModel)) {
            HotKeyPusher.push(key, null);      // 3b. 即将过期 → 重新上报
        }
    }
    KeyHandlerFactory.getCounter()             // 4. 命中计数
        .collect(new KeyHotModel(key, isHot));
    return isHot;
}
```

**关键判断逻辑：**
- **规则内过滤**：只有匹配 `KeyRule` 的 key 才会上报，不匹配的直接跳过
- **非热 key 一定上报**：需要 Worker 判定
- **热 key 即将过期也上报**：提前续期，避免缓存空窗期（阈值 < 2 秒）

### 2.2 规则匹配：KeyRuleHolder

规则匹配优先级：**全匹配 > 前缀匹配 > 通配符(\*)**

```java
private static KeyRule findRule(String key) {
    KeyRule prefix = null;
    KeyRule common = null;
    for (KeyRule keyRule : KEY_RULES) {
        if (key.equals(keyRule.getKey()))      return keyRule;  // 全匹配
        if (keyRule.isPrefix() && key.startsWith(keyRule.getKey())) prefix = keyRule;  // 前缀
        if ("*".equals(keyRule.getKey()))      common = keyRule;  // 通配
    }
    return prefix != null ? prefix : common;
}
```

### 2.3 待测 Key 收集：TurnKeyCollector（双缓冲）

```
                    collect() 写入
                         │
                         ▼
              ┌─────────────────────┐
              │   AtomicLong = 0    │──── 写 map0（偶数）
              │   AtomicLong = 1    │──── 写 map1（奇数）
              └─────────────────────┘
                         │
               lockAndGetResult() 读取
                         │
                         ▼
              翻转计数器，读取对侧 map，清空
```

**详细步骤：**

1. 业务线程调用 `collect(HotKeyModel)`，根据 `atomicLong` 奇偶选择写入 map0 或 map1
2. 使用 `putIfAbsent` 去重，若 key 已存在则 `add(count)` 合并计数
3. 定时器每 500ms 触发 `lockAndGetResult()`：
   - `atomicLong.addAndGet(1)` 翻转
   - 读取**对侧** map（此时对侧不再被写入）
   - 清空对侧 map
   - 返回本次待发送的 key 列表

**为什么用两个 map？**
- 写入线程（业务线程）和读取线程（定时器线程）操作不同的 map，**无需加锁**
- 翻转的瞬间最多有一个周期的数据在两个 map 中，这是可接受的

### 2.4 命中计数收集：TurnCountCollector

与 TurnKeyCollector 同样的双缓冲机制，但存储粒度不同：

```
存储格式：ruleKey + 分隔符 + 时间戳
例如：
  pin_2020-06-24 09:10:24 → HitCount{totalHitCount=10, hotHitCount=3}
  sku_2020-06-24 09:10:24 → HitCount{totalHitCount=123, hotHitCount=80}
```

每个 `HitCount` 内部使用 **LongAdder** 计数：
- `totalHitCount`：该规则 key 在该秒的总访问次数
- `hotHitCount`：其中命中热 key 的次数

**自适应并行转换**：当待转换数据量 > 5000 时，自动从 `syncConvert`（for 循环）切换到 `parallelConvert`（parallelStream），大数据量下显著提速。

### 2.5 定时调度：PushSchedulerStarter

```java
// 每500ms推送待测key
scheduleAtFixedRate(() -> {
    List<HotKeyModel> list = KeyHandlerFactory.getCollector().lockAndGetResult();
    if (notEmpty(list)) {
        KeyHandlerFactory.getPusher().send(APP_NAME, list);
        collectHK.finishOnce();
    }
}, 0, 500, MILLISECONDS);

// 每10秒推送命中计数
scheduleAtFixedRate(() -> {
    List<KeyCountModel> list = KeyHandlerFactory.getCounter().lockAndGetResult();
    if (notEmpty(list)) {
        KeyHandlerFactory.getPusher().sendCount(APP_NAME, list);
        collectHK.finishOnce();
    }
}, 0, 10, SECONDS);
```

### 2.6 发送到 Worker：NettyKeyPusher

```
                  待发送的 HotKeyModel 列表
                         │
                         ▼
              ┌─────────────────────┐
              │  按 key.hashCode()  │
              │  分组到不同 Channel  │
              └─────────────────────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          Channel-0   Channel-1   Channel-2
          (Worker-0)  (Worker-1)  (Worker-2)
              │          │          │
              ▼          ▼          ▼
          REQUEST_NEW_KEY 请求（Protostuff序列化）
```

**关键细节：**
- Worker 列表保存在 `WorkerInfoHolder` 中（`CopyOnWriteArrayList<Server>`）
- 通过 `key.hashCode() % worker数量` 决定发往哪个 Worker，**同一 key 始终路由到同一 Worker**
- Worker 地址按字典序排序，保证 hashCode 取模的一致性
- 每个请求设置统一的 `createTime`，Worker 据此判断延迟
- 使用 `channel.writeAndFlush().sync()` 同步发送，确保消息刷出

---

## 三、Worker 端接收与统计（核心流程）

> 注意：Worker 端源码不在本项目依赖中，以下基于 JD HotKey 开源框架的设计和 Client 端代码逆向推导。

### 3.1 Netty 接收与过滤链

```
Client 请求
    │
    ▼
NodesServer（Netty Server）
    │
    ▼
┌──────────────────────────────────────┐
│          Filter Chain                │
│                                      │
│  AppNameFilter  ── 校验 appName      │
│       │                              │
│  HeartBeatFilter ── 处理 PING/PONG  │
│       │                              │
│  HotKeyFilter ── 处理热key推送       │
│       │                              │
│  KeyCounterFilter ── 处理命中计数    │
│                                      │
└──────────────────────────────────────┘
    │
    ▼
业务处理
```

Worker 通过责任链模式处理不同类型的消息：
- `REQUEST_NEW_KEY` → 进入滑动窗口统计
- `REQUEST_HIT_COUNT` → 聚合到计数统计
- `PING` → 回复 PONG

### 3.2 滑动窗口统计：SlidingWindow

这是 Worker 端判定热 key 的核心算法。

```
时间轴:    t0    t1    t2    t3    t4    t5    t6    t7
           ├─────┼─────┼─────┼─────┼─────┼─────┼─────┤
数组:      [  5 ] [  3 ] [ 12 ] [  8 ] [  0 ] [  0 ] [  0 ] [  0 ]
                                         ↑
                                    当前窗口起点
```

**核心数据结构：**

```java
// 2倍窗口大小的 AtomicLong 数组
AtomicLong[] timeSlices = new AtomicLong[timeSliceSize * 2];
```

**为什么用 2 倍？**

奇偶交替读写，避免清空操作与读取操作的竞争：
- 偶数周期：写入 `timeSlices[0..N-1]`，读取 `timeSlices[N..2N-1]`
- 奇数周期：写入 `timeSlices[N..2N-1]`，读取 `timeSlices[0..N-1]`

**统计流程：**

```
1. key 到达，查找该 key 对应的 SlidingWindow（Caffeine缓存）
   └─ 不存在则创建

2. 计算当前时间片索引
   └─ index = (currentTime - startTime) / sliceInterval

3. 在当前时间片原子累加计数
   └─ timeSlices[index].addAndGet(count)

4. 计算窗口内总计数
   └─ 遍历当前窗口覆盖的所有时间片，求和

5. 判定是否为热 key
   └─ 总计数 >= 阈值 → 标记为热 key
```

**窗口滑动与过期清理：**
- 每次写入时检查当前时间片，如果已进入新的时间片，将旧时间片清零
- 利用 CAS 操作保证并发安全
- 窗口外的旧数据自动过期，无需额外清理线程

### 3.3 热 Key 判定与推送

```
SlidingWindow 统计
       │
       ▼
  总计数 >= 阈值？
       │
   Yes │   No → 不处理
       │
       ▼
  标记为热 key
       │
       ▼
  ┌────────────────────┐
  │  TurnKeyCollector   │  （Worker端的双缓冲收集器）
  │  收集热 key 列表    │
  └────────────────────┘
       │
       ▼  每 10ms
  AppServerPusher
       │
       ▼
  RESPONSE_NEW_KEY → 推送到所有已连接的 Client
```

### 3.4 Client 接收热 Key 并更新本地缓存

```
NettyClientHandler.channelRead0()
       │
       ▼  RESPONSE_NEW_KEY
EventBusCenter.post(ReceiveNewKeyEvent)
       │
       ▼
ReceiveNewKeySubscribe.newKeyComing()
       │
       ▼
DefaultNewKeyListener.newKey()
       │
       ├── remove = true → 删除本地缓存
       │
       └── remove = false → 写入本地 Caffeine 缓存
           └── ValueModel.defaultValue(key) → 设置过期时间
           └── JdHotKeyStore.setValueDirectly(key, valueModel)
```

### 3.5 命中计数聚合与上报 Dashboard

Worker 收到 Client 的 `REQUEST_HIT_COUNT` 后：

```
Client 每10秒上报 KeyCountModel
  - ruleKey: 规则key前缀
  - totalHitCount: 总访问次数
  - hotHitCount: 热key访问次数
       │
       ▼
Worker 聚合（按 appName + ruleKey 维度）
       │
       ▼
每 10 秒上报到 Dashboard（通过 etcd 或 HTTP）
  - 总访问量趋势
  - 热key命中率
  - 各规则维度的统计
```

---

## 四、全流程时序图

```
Client业务线程     Client定时线程      Netty通道       Worker定时线程     Worker统计线程     Dashboard
     │                 │                 │                 │                 │               │
     │ isHotKey(key)   │                 │                 │                 │               │
     │───┐             │                 │                 │                 │               │
     │   │ 规则匹配     │                 │                 │                 │               │
     │   │ 查本地缓存   │                 │                 │                 │               │
     │   │ collect()   │                 │                 │                 │               │
     │◀──┘             │                 │                 │                 │               │
     │                 │                 │                 │                 │               │
     │        500ms定时器触发             │                 │                 │               │
     │                 │───┐             │                 │                 │               │
     │                 │   │lockAndGet   │                 │                 │               │
     │                 │   │Result()     │                 │                 │               │
     │                 │◀──┘             │                 │                 │               │
     │                 │                 │                 │                 │               │
     │                 │   REQUEST_NEW_KEY(批量)           │                 │               │
     │                 │ ──────────────> │                 │                 │               │
     │                 │                 │ ──────────────> │                 │               │
     │                 │                 │                 │───┐             │               │
     │                 │                 │                 │   │SlidingWindow │               │
     │                 │                 │                 │   │累加计数      │               │
     │                 │                 │                 │◀──┘             │               │
     │                 │                 │                 │                 │               │
     │                 │                 │                 │  10ms定时器      │               │
     │                 │                 │                 │───┐             │               │
     │                 │                 │                 │   │判定热key    │               │
     │                 │                 │                 │◀──┘             │               │
     │                 │                 │                 │                 │               │
     │                 │   RESPONSE_NEW_KEY               │                 │               │
     │                 │ <────────────── │ <────────────── │                 │               │
     │                 │                 │                 │                 │               │
     │  EventBus通知   │                 │                 │                 │               │
     │◀────────────────│                 │                 │                 │               │
     │ 更新Caffeine    │                 │                 │                 │               │
     │                 │                 │                 │                 │               │
     │                 │                 │                 │   10s上报统计   │               │
     │                 │                 │                 │ ─────────────────────────────> │
```

---

## 五、设计亮点总结

### 亮点 1：双缓冲无锁读写分离

**问题**：高并发下，业务线程不断写入待测 key，定时线程需要周期性读取并清空，如何避免锁竞争？

**方案**：`TurnKeyCollector` / `TurnCountCollector` 使用两个 ConcurrentHashMap + AtomicLong 奇偶翻转：

```
atomicLong = 0 (偶数)  →  写 map0，读 map1
atomicLong = 1 (奇数)  →  写 map1，读 map0
```

- 写入线程和读取线程**永远操作不同的 map**，天然无锁
- 翻转瞬间仅影响新进入的请求，最多损失半个周期的数据
- `putIfAbsent + add()` 实现 key 去重合并，减少传输量

**对比**：如果用 `ConcurrentHashMap + synchronized` 或 `BlockingQueue`，高并发下性能会显著下降。

### 亮点 2：滑动窗口的 2 倍数组无锁设计

**问题**：滑动窗口需要频繁写入计数、读取窗口总和、清零过期时间片，三者并发如何无锁？

**方案**：2 倍时间长度的 `AtomicLong[]` 数组，奇偶周期交替使用：

```
周期 N（偶数）：写入 [0..N-1]，读取 [N..2N-1] 中的过期数据
周期 N+1（奇数）：写入 [N..2N-1]，读取 [0..N-1] 中的过期数据
```

- 写入操作是 `AtomicLong.addAndGet()`，天然原子性
- 读取时对侧数组本周期不会被写入，无需加锁
- 清零操作与写入操作不会发生在同一区域

### 亮点 3：一致性哈希路由

**问题**：多个 Worker 节点如何分工，保证同一 key 始终由同一 Worker 处理？

**方案**：`key.hashCode() % workerCount` 路由，Worker 列表按地址排序：

```java
int index = Math.abs(key.hashCode() % WORKER_HOLDER.size());
Channel channel = WORKER_HOLDER.get(index).channel;
```

- 同一 key 始终路由到同一 Worker，滑动窗口统计准确
- Worker 列表排序保证各 Client 路由一致性
- Worker 变更时通过 etcd 通知所有 Client 重新连接和排序

### 亮点 4：自适应并行转换

**问题**：`TurnCountCollector` 每 10 秒需要将 Map 数据转换为 List 上报，数据量小时并行开销大，数据量大时串行太慢。

**方案**：阈值判断，动态切换：

```java
if (map.size() > 5000) {
    return parallelConvert(map);    // parallelStream 并行
} else {
    return syncConvert(map);        // 普通 for 循环
}
```

- 小数据量避免线程池调度开销
- 大数据量利用多核并行提速
- 阈值 5000 是经验值，可根据实际场景调整

### 亮点 5：LongAdder 替代 AtomicLong 做高频计数

**问题**：多线程高频累加计数时，`AtomicLong` 的 CAS 操作在竞争激烈时自旋严重。

**方案**：`LongAdder` 内部分 Cell 数组，各线程对不同 Cell 累加，最终求和：

```
Thread-1 → Cell[0].add()
Thread-2 → Cell[1].add()
Thread-3 → Cell[2].add()
...
sum = Cell[0] + Cell[1] + Cell[2] + ...
```

- 写入时无 CAS 竞争，吞吐量远高于 AtomicLong
- 适用于"写多读少"的计数场景（统计上报是低频读取）
- `HotKeyModel.count`、`HitCount.totalHitCount` / `hotHitCount` 均使用 LongAdder

### 亮点 6：热 Key 即将过期时提前续期

**问题**：热 key 在本地 Caffeine 中过期后，到 Worker 重新判定推送之间有短暂空窗期，期间所有请求都会穿透到数据库。

**方案**：

```java
if (isHot && isNearExpire(valueModel)) {
    HotKeyPusher.push(key, null);  // 距过期 < 2s，提前上报
}
```

- 在 key 过期前 2 秒主动上报，Worker 判定后推送新值
- 新值到达后刷新 Caffeine 缓存，实现无缝衔接
- 避免了"过期 → 穿透 → 重新判定 → 推送"的延迟空窗

### 亮点 7：批量聚合 + 分频调度

**问题**：每个业务请求都发一条消息到 Worker，网络 IO 压力巨大。

**方案**：三层批量聚合：

| 层级 | 机制 | 频率 |
|------|------|------|
| 第一层：去重 | `putIfAbsent` 合并相同 key | 实时 |
| 第二层：时间窗口 | 双缓冲收集 500ms 内的 key | 500ms |
| 第三层：批量发送 | 一次 Netty writeAndFlush 发一批 | 500ms |

加上命中计数每 10 秒才上报一次，大幅减少了网络 IO：
- 待测 key：从每请求 1 次 → 每 500ms 1 批
- 命中计数：从每请求 1 次 → 每 10 秒 1 批
- 热 key 推送：10ms 1 批（保证低延迟感知）

### 亮点 8：事件驱动架构

**问题**：Client 端接收热 key 推送、规则变更、Worker 变更需要解耦处理。

**方案**：Guava EventBus 事件总线：

```
热key推送 → ReceiveNewKeyEvent → ReceiveNewKeySubscribe → DefaultNewKeyListener
规则变更 → KeyRuleInfoChangeEvent → KeyRuleHolder.ruleChange()
Worker变更 → WorkerInfoChangeEvent → WorkerChangeSubscriber
连接断开 → ChannelInactiveEvent → WorkerRetryConnector
```

- 消息接收、事件分发、业务处理完全解耦
- 新增监听器无需修改现有代码，符合开闭原则
- 同步事件总线，保证事件处理顺序

---

## 六、核心数据结构一览

| 组件 | 数据结构 | 用途 |
|------|----------|------|
| `TurnKeyCollector` | `ConcurrentHashMap × 2` + `AtomicLong` | 双缓冲收集待测 key |
| `TurnCountCollector` | `ConcurrentHashMap<String, HitCount> × 2` | 双缓冲收集命中计数 |
| `HitCount` | `LongAdder × 2` | 总访问 / 热key访问 计数 |
| `HotKeyModel` | `BaseModel` + `appName/keyType/remove` | 待测 key 传输模型 |
| `KeyCountModel` | `ruleKey/totalHitCount/hotHitCount` | 命中计数传输模型 |
| `HotKeyMsg` | `messageType + List<HotKeyModel/KeyCountModel>` | Netty 消息信封 |
| `WorkerInfoHolder` | `CopyOnWriteArrayList<Server>` | Worker 连接管理 |
| `SlidingWindow` | `AtomicLong[] × 2N` | 滑动窗口时间片计数 |
| `KeyRuleHolder` | `ConcurrentHashMap<Integer, LocalCache>` + `List<KeyRule>` | 规则匹配与缓存映射 |

---

## 七、关键时序参数

| 参数 | 值 | 说明 |
|------|-----|------|
| 待测 key 上报间隔 | 500ms | Client → Worker |
| 命中计数上报间隔 | 10s | Client → Worker |
| 热 key 推送间隔 | 10ms | Worker → Client |
| 心跳间隔 | Netty IdleState | 双向 PING/PONG |
| 热 key 过期续期阈值 | 2s | 提前上报避免空窗 |
| 并行转换阈值 | 5000 条 | 计数统计自适应切换 |
| 滑动窗口大小 | 可配置 | 决定热 key 判定的时间维度 |