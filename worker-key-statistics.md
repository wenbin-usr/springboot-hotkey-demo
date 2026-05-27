# Worker 端快速统计待测 Key 的机制

## 整体数据流

```
Client                     Worker                        Dashboard
  |                          |                              |
  |-- 待测key批次(0.5s) ---->|                              |
  |-- 命中计数统计(10s) ---->|                              |
  |                          |-- 滑动窗口统计 -------------->|
  |                          |-- 热key判定结果 ------------>|
  |<-- 热key推送(10ms) ------|                              |
```

## 1. 接收端：批量接收，不逐条处理

Client 每 **500ms** 批量发送待测 key 到 Worker（通过 `TurnKeyCollector` 双缓冲收集后由 `NettyKeyPusher` 发送），Worker 收到的是一批 `HotKeyModel`，而非单个 key，减少了网络开销和处理频次。

## 2. 统计核心：SlidingWindow（滑动窗口）

Worker 为每个 key 维护一个 `SlidingWindow`，其核心设计：

- 使用 **2 倍时间长度的 AtomicLong 数组**（`timeSlices`），奇偶交替读写，避免加锁
- 每个时间切片记录该时间窗口内的 key 访问次数
- 窗口滑动时自动过期旧数据
- 当窗口内总访问次数超过阈值，该 key 被标记为热 key

## 3. 双缓冲收集：TurnKeyCollector / TurnCountCollector

核心思路：

```
map0 (写入中) ←── collect()写入
map1 (读取中) ←── lockAndGetResult()读取并清空

下一个周期翻转：
map0 (读取中) ←── lockAndGetResult()读取并清空
map1 (写入中) ←── collect()写入
```

关键点：

- 用 `AtomicLong` 计数器奇偶翻转，实现**无锁读写分离**
- 写入时不阻塞，读取时对侧 map 停止写入，读完后清空
- 相同 key 通过 `putIfAbsent` + `add()` 合并计数，避免重复发送

## 4. 计数统计：TurnCountCollector

用 `LongAdder` 统计两类计数：

- `totalHitCount`：key 总访问次数
- `hotHitCount`：已经是热 key 的访问次数

存储格式为 `ruleKey + 时间戳`（如 `pin_2020-06-24 09:10:24`），每 **10 秒** 上报一次。当数据量 > 5000 条时自动切换为 `parallelStream` 并行转换，提升性能。

## 5. 热 Key 推送：10ms 批次

Worker 判定出热 key 后，每 **10ms** 批量推送回 Client，Client 通过 EventBus 通知各监听器更新本地 Caffeine 缓存。

## 关键设计总结

| 设计点 | 手段 | 效果 |
|--------|------|------|
| 读写无锁 | 双缓冲 map + AtomicLong 奇偶翻转 | 写入不阻塞，读取无竞争 |
| 批量处理 | Client 500ms 聚合、Worker 批量接收 | 减少网络 IO 和处理频次 |
| 滑动窗口 | 2x AtomicLong 数组奇偶交替 | 时间片统计无锁高并发 |
| 计数高效 | LongAdder 替代 AtomicLong | 多线程累加无 CAS 竞争 |
| 自适应并行 | 数据量 > 5000 切换 parallelStream | 大数据量下转换更快 |
| Key 去重 | putIfAbsent + add 合并计数 | 相同 key 不重复发送 |

## 核心思想

**双缓冲无锁交替 + 批量聚合 + 滑动窗口原子计数**，让高并发下的 key 统计既快又准。