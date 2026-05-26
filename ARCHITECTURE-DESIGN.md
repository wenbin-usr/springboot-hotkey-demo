# JD HotKey 值得关注的设计细节

## 1. 滑动窗口的「双倍缓冲」技巧

`SlidingWindow` 的 `timeSliceSize = windowSize * 2`，这个设计很巧妙：

- 环形数组大小是窗口的 **2 倍**，当前时间片写入时，只需把**未来**的格子清零（`clearFromIndex`），而不是清过去
- 避免了「清旧数据 → 读旧数据」的竞态问题，因为写入方向和清理方向天然错开
- 超过整个窗口没有数据时，直接 `reset()` 整体归零，避免脏数据累积

核心代码（`worker/tool/SlidingWindow`）：

```java
// 数组大小 = 窗口大小 * 2
this.timeSliceSize = windowSize * 2;
timeSlices = new AtomicLong[timeSliceSize];

// 定位当前时间片
private int locationIndex() {
    long now = SystemClock.now();
    if (now - lastAddTimestamp > timeMillisPerSlice * windowSize) {
        reset();  // 超过整个窗口无数据，整体归零
    }
    int index = (int) (((now - beginTimestamp) / timeMillisPerSlice) % timeSliceSize);
    return index < 0 ? 0 : index;
}

// 清的是"未来"的格子，不是过去
private void clearFromIndex(int index) {
    for (int i = 1; i <= windowSize; i++) {
        int j = index + i;
        if (j >= windowSize * 2) {
            j -= windowSize * 2;
        }
        timeSlices[j].set(0);
    }
}
```

---

## 2. 热键即将过期的「提前续期」机制

`JdHotKeyStore.isNearExpire()` 的设计值得注意：

```java
private static boolean isNearExpire(ValueModel valueModel) {
    if (valueModel == null) {
        return true;
    }
    return valueModel.getCreateTime() + valueModel.getDuration()
           - System.currentTimeMillis() <= 2000;
}
```

当缓存值**还有 2 秒就要过期**时，就主动再发一次探测请求，而不是等到过期后才发现。这是一个典型的**缓存预热**思路——避免热键过期瞬间的"惊群"效应（所有请求同时穿透到下游）。

调用位置：

```java
public static boolean isHotKey(String key) {
    boolean isHot = isHot(key);
    if (!isHot) {
        HotKeyPusher.push(key, null);
    } else {
        ValueModel valueModel = getValueSimple(key);
        if (isNearExpire(valueModel)) {   // 即将过期，提前续探
            HotKeyPusher.push(key, null);
        }
    }
}
```

---

## 3. Filter Chain 责任链模式

Worker 端用 `@Order` + Spring 自动收集实现了一个简洁的责任链：

```java
// NodesServerHandler.java
@Autowired
private List<INettyMsgFilter> messageFilters;

@Override
public void channelRead0(ChannelHandlerContext ctx, HotKeyMsg msg) {
    for (INettyMsgFilter messageFilter : messageFilters) {
        boolean doNext = messageFilter.chain(msg, ctx);
        if (!doNext) return;  // 某个过滤器处理完毕，终止链
    }
}
```

过滤器实现：

```java
@Order(1)
public class AppNameFilter implements INettyMsgFilter { ... }

@Order(2)
public class HeartBeatFilter implements INettyMsgFilter { ... }

@Order(3)
public class HotKeyFilter implements INettyMsgFilter { ... }

@Order(4)
public class KeyCounterFilter implements INettyMsgFilter { ... }
```

好处是**加过滤器不改旧代码**，只需要新增一个实现了 `INettyMsgFilter` 的 Spring Bean 并标注 `@Order` 即可自动注入。这比 Netty 原生的 `ChannelPipeline` 更轻量，也更适合业务消息分发。

---

## 4. EventBus 解耦客户端生命周期事件

`EventBusCenter` 封装了 Guava EventBus，客户端内部的事件流转完全解耦：

| 事件 | 发布者 | 订阅者 |
|------|--------|--------|
| `WorkerInfoChangeEvent` | EtcdStarter (Watch) | WorkerChangeSubscriber → 建连/断连 |
| `KeyRuleInfoChangeEvent` | EtcdStarter (Watch) | KeyRuleHolder → 刷新规则 |
| `ReceiveNewKeyEvent` | Netty 消息处理 | ReceiveNewKeySubscribe → 写本地缓存 |
| `ChannelInactiveEvent` | Netty 断线回调 | WorkerChangeSubscriber → 标记不可用 |

这意味着**Etcd Watch、Netty 通信、本地缓存**三个关注点互不依赖，新功能只需要新增 Subscriber 即可。

EventBusCenter 实现：

```java
public class EventBusCenter {
    private static final EventBus EVENT_BUS = new EventBus();

    public static void register(Object object) {
        EVENT_BUS.register(object);
    }
    public static void unregister(Object object) {
        EVENT_BUS.unregister(object);
    }
    public static void post(Object event) {
        EVENT_BUS.post(event);
    }
}
```

---

## 5. 批量聚合 + 定时推送的吞吐优化

`PushSchedulerStarter` 的设计体现了"攒一批再发"的思路：

```java
// 热键探测请求：每 500ms 聚合一批发送
scheduledExecutorService.scheduleAtFixedRate(() -> {
    IKeyCollector<HotKeyModel, HotKeyModel> collectHK = KeyHandlerFactory.getCollector();
    List<HotKeyModel> hotKeyModels = collectHK.lockAndGetResult();
    if (CollectionUtil.isNotEmpty(hotKeyModels)) {
        KeyHandlerFactory.getPusher().send(Context.APP_NAME, hotKeyModels);
        collectHK.finishOnce();
    }
}, 0, period, TimeUnit.MILLISECONDS);

// 命中统计：每 10s 聚合一批上报
scheduledExecutorService.scheduleAtFixedRate(() -> {
    IKeyCollector<KeyHotModel, KeyCountModel> collectHK = KeyHandlerFactory.getCounter();
    List<KeyCountModel> keyCountModels = collectHK.lockAndGetResult();
    if (CollectionUtil.isNotEmpty(keyCountModels)) {
        KeyHandlerFactory.getPusher().sendCount(Context.APP_NAME, keyCountModels);
        collectHK.finishOnce();
    }
}, 0, period, TimeUnit.SECONDS);
```

单次 key 探测从"一次网络请求"变成"攒一批复用一次连接"，在高并发场景下网络开销降低几个数量级。Worker 端的推送也是类似：

| 推送器 | 批量周期 | 用途 |
|--------|---------|------|
| AppServerPusher | 10ms | 高频推送热键到所有客户端 |
| DashboardPusher | 1s | 低频推送热键记录到控制台 |

频率按业务重要性分级，关键路径（客户端热键推送）频率远高于非关键路径（控制台记录）。

---

## 6. 生产级防御性编码

### 6.1 过期消息丢弃

`KeyProducer.push()` 中，如果消息的创建时间距当前超过 `timeOut`，直接丢弃不计入统计，防止网络延迟导致的脏计数：

```java
public void push(HotKeyModel model, long now) {
    if (model == null || model.getKey() == null) {
        return;
    }
    if (now - model.getCreateTime() > InitConstant.timeOut) {
        expireTotalCount.increment();
        return;
    }
    try {
        QUEUE.put(model);
        totalOfferCount.increment();
    } catch (InterruptedException e) {
        e.printStackTrace();
    }
}
```

### 6.2 异常降级

`JdHotKeyStore.isHotKey()` 的 catch 块直接 `return false`——探测框架本身不能成为业务故障源：

```java
public static boolean isHotKey(String key) {
    try {
        // ... 探测逻辑
        return isHot;
    } catch (Exception e) {
        return false;  // 探测框架异常不影响业务
    }
}
```

### 6.3 魔数占位防缓存穿透

`get()` 方法中对 `MAGIC_NUMBER` 的判断，用特殊整数值区分"缓存值为 null"和"key 不存在"，避免缓存穿透：

```java
public static Object get(String key) {
    ValueModel value = getValueSimple(key);
    if (value == null) {
        return null;
    }
    Object object = value.getValue();
    if (object instanceof Integer && Constant.MAGIC_NUMBER == (int) object) {
        return null;  // 魔数占位，表示值为空但 key 存在
    }
    return object;
}
```

### 6.4 网络健康检测

Worker 端 EtcdStarter 会检测网络状态，异常时**暂停注册**而不是反复重试导致日志爆炸。

---

## 7. 客户端启动的 Builder 模式

```java
ClientStarter starter = new ClientStarter.Builder()
    .setAppName("my-app")
    .setEtcdServer("http://etcd:2379")
    .setPushPeriod(500L)
    .setCaffeineSize(200000)
    .build();
```

Builder 内部还有校验逻辑（如 `caffeineSize < 128` 抛异常），把配置合法性和对象创建绑定在一起，避免运行时才暴露配置错误。

---

## 8. 集群容错——优雅降级策略

| 故障 | 降级行为 |
|------|---------|
| Etcd 宕机 | 已有规则和 Worker 列表缓存在本地，短期可继续运行 |
| 部分 Worker 宕机 | 剩余 Worker 自动承接（key 重新哈希），30s 重连器持续尝试 |
| 所有 Worker 宕机 | `isHotKey` 返回 false，业务走原有逻辑，不阻塞 |
| 网络分区 | 心跳超时断开，自动重连 |

核心原则：**热键探测是增强手段，不是关键路径**。整个框架的设计理念是不让探测框架本身成为系统故障单点。

---

## 9. 线程命名的可观测性意识

项目广泛使用 `NamedThreadFactory` 给线程池中的线程取有意义的名字：

```java
Executors.newSingleThreadScheduledExecutor(
    new NamedThreadFactory("hotkey-pusher-service-executor", true)
);
Executors.newSingleThreadScheduledExecutor(
    new NamedThreadFactory("worker-retry-connector-service-executor", true)
);
```

线上用 `jstack` 排查时能一眼看出是哪个组件的线程，这是生产系统的好习惯。

---

## 10. 自动重连的「定期巡检」模式

`WorkerRetryConnector` 不依赖连接断开时的回调触发重连，而是**每 30 秒主动巡检**一次：

```java
public static void retryConnectWorkers() {
    ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("worker-retry-connector-service-executor", true)
        );
    scheduledExecutorService.scheduleAtFixedRate(
        WorkerRetryConnector::reConnectWorkers, 30, 30, TimeUnit.SECONDS
    );
}

private static void reConnectWorkers() {
    List<String> nonList = WorkerInfoHolder.getNonConnectedWorkers();
    if (nonList.size() == 0) {
        return;
    }
    JdLogger.info(WorkerRetryConnector.class,
        "trying to reConnect to these workers :" + nonList);
    NettyClient.getInstance().connect(nonList);
}
```

好处是即使回调丢失（极端场景），也能兜底恢复；且无连接失败时不做任何操作，开销极低。

---

## 总结

jd-hotkey 最值得学习的不是某个单一技术点，而是**"增强而非依赖"的系统定位**——从 API 层的异常降级、到缓存即将过期的提前续期、到探测框架故障时的优雅退化，所有设计都在贯彻一个原则：

> **这个框架挂了，业务不能挂。**
