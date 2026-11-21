# 以 Scalability 为核心目的的设计模式

**来源**：*Cloud Design Patterns: Prescriptive Architecture Guidance for Cloud Applications*，1st edition，Microsoft patterns & practices，2014。
**选取口径**：仅取 `index.md`「四、专题索引」中标记为 **A（核心目的）** 的 6 个模式。B/C 档不在本文范围。
**页码**：均为原书印刷页码，随原文标记。
**引文与代码**：引用块与代码行均按原文逐字录入（代码为节选，省略不改动保留行；`...` 均为原文所有）。

## 总览

| 模式 | 印刷页 | 它把什么技术问题转成了什么资源问题 | 转成资源后的新瓶颈 |
| --- | --- | --- | --- |
| [Competing Consumers](#1-competing-consumers) | 28–33 | 消费能力不足 → 消费者实例数 | 消息队列本身成为瓶颈；消息乱序与重复 |
| [CQRS](#2-cqrs) | 42–49 | 读写负载互相拖累 → 读模型与写模型各自独立的资源池 | 读库与写库之间的一致性时延 |
| [Pipes and Filters](#3-pipes-and-filters) | 100–108 | 单段处理能力不足 → 每个 filter 独立部署与伸缩 | 最慢的那一段决定整体；跨段的数据可靠性 |
| [Queue-Based Load Leveling](#4-queue-based-load-leveling) | 116–119 | 峰值压垮服务 → 队列缓冲 + 服务实例数按均值配置 | 队列积压带来的延迟；共享资源争用 |
| [Sharding](#5-sharding) | 140–149 | 单节点容量/吞吐到顶 → 分片数（存储节点数） | 跨片查询与再平衡成本永久进入应用层 |
| [Throttling](#6-throttling) | 155–159 | 需求超过容量 → 主动削减请求，用「少做事」换取「不崩溃」 | 被限流的请求需重试；限流阈值本身要调 |

**共同结构**：六个模式都不改变单次请求的处理逻辑，而是改变「谁能被复制」「复制多少」「复制的代价转到哪里」。这正是本仓库主张的「技术问题 → 资源问题」的可操作形态。

---

## 1. Competing Consumers

**原文页**：p. 28–33

### 详细介绍

允许多个并发消费者从同一个消息通道上取消息并处理。应用把请求以消息形式投递到队列，消费者服务的一组实例从队列取消息并处理。因为队列保证每条消息只投递给一个消费者，所以实例之间不需要复杂协调。

原文定义（p. 28）：

> Enable multiple concurrent consumers to process messages received on the same messaging channel. This pattern enables a system to process multiple messages concurrently to optimize throughput, to improve scalability and availability, and to balance the workload.

### 问题与解法

**问题**（p. 28）：请求量随时间和租户剧烈波动，峰值时可能每秒数百个，低谷时极少；单体消费者要么被灌满，要么消息系统被涌入的消息压垮。想跑多个消费者实例，就必须解决两件事——每条消息只能被一个消费者处理，以及负载要在消费者之间均衡，不能有实例成为瓶颈。

**解法**：在应用与消费者之间放一个消息队列。队列承担三件事：缓冲（吸收波动）、分发（一条消息只给一个消费者）、解耦（生产者不关心谁在处理）。于是「处理能力」被转化成了「消费者实例数」这一可增减的资源。

原文列出的收益（p. 29）中有两条直接对应扩容：

> It is scalable. The system can dynamically increase or decrease the number of instances of the consumer service as the volume of messages fluctuates.

> It does not require complex coordination between the consumers, or between the producer and the consumer instances.

### 新增的约束（p. 30）

- **消息顺序不保证**：消费顺序不等于产生顺序，因此处理必须幂等。「顺序」这项约束没有被消除，而是被推给了业务逻辑。
- **毒消息**：格式错误的消息会让实例反复失败，必须移出队列单独存放。
- **消息系统本身会成为瓶颈**：原文明确指出单队列在大规模下可能被压垮，应考虑按生产者分区或分流到多个队列。这就是「扩容后的新瓶颈」。
- **结果回传**：消费者与生产者完全解耦，若需要返回值，必须另建回执通道并做关联。

### 原文示例与代码

示例使用 Azure Storage Queues / Service Bus Queues。可靠性通过 **PeekLock** 模式实现：取消息时不删除，只对其他消费者隐藏；处理完成后才删除；若消费者失败，锁超时会令消息重新可见（p. 31）。

创建队列并设置最大投递次数（p. 31）：

```csharp
public async Task Start()
{
  // Check if the queue already exists.
  var manager = NamespaceManager.CreateFromConnectionString(this.connectionString);
  if (!manager.QueueExists(this.queueName))
  {
    var queueDescription = new QueueDescription(this.queueName);
    // Set the maximum delivery count for messages in the queue. A message
    // is automatically dead-lettered after this number of deliveries.
    queueDescription.MaxDeliveryCount = 3;
    await manager.CreateQueueAsync(queueDescription);
  }
  ...
  this.client = QueueClient.CreateFromConnectionString(
    this.connectionString, this.queueName);
}
```

消费者以事件驱动方式并发取消息，并发上限由 `MaxConcurrentCalls` 控制（p. 32）：

```csharp
public void ReceiveMessages(Func<BrokeredMessage, Task> processMessageTask)
{
  var options = new OnMessageOptions();
  options.AutoComplete = false;
  options.MaxConcurrentCalls = 10;
  options.ExceptionReceived += this.OptionsOnExceptionReceived;
  this.client.OnMessageAsync(
    async (msg) =>
    {
      this.pauseProcessingEvent.WaitOne();
      await processMessageTask(msg);
    },
    options);
}
```

原文补充（p. 33）：可用 autoscaling 按队列长度启停实例；且 role 实例与 worker 进程不必一一对应，一个实例可承载多个 worker 进程（见 Compute Resource Consolidation）。

---

## 2. CQRS

**原文页**：p. 42–49（全称 Command and Query Responsibility Segregation）

### 详细介绍

把读操作（Queries）与写操作（Commands）用不同的接口隔离，并使用不同的数据模型。隔离是逻辑上的要求；物理上读写是否分库是可选的，但原文指出常见做法是分成不同的物理存储，以获得最大的性能、可扩展性与安全性。

原文定义（p. 42）：

> Segregate operations that read data from operations that update data by using separate interfaces. This pattern can maximize performance, scalability, and security; … and prevent update commands from causing merge conflicts at the domain level.

### 问题与解法

**问题**（p. 42–43）：传统 CRUD 中，读写共用同一套实体与同一个数据仓库，导致三类问题——

1. 读写所需的表示不一致，却被迫共用一个 DTO；
2. 协作型领域里并发更新会冲突（加锁则争用，乐观锁则合并冲突），且随吞吐上升而加剧；
3. 每个实体同时暴露读写权限，安全边界变模糊。

**解法**：为读和写分别建模。写模型承载完整的命令处理栈，包括业务逻辑与校验，保证聚合内一致；读模型没有业务逻辑，只返回为视图准备的 DTO。

于是扩容的关键变成：**读写可以各自按自己的负载配置资源**。原文（p. 44）：

> Separation of the read and write stores also allows each to be scaled appropriately to match the load. For example, read stores typically encounter a much higher load that write stores.

并且在 When to Use（p. 45）中给出了具体的资源配比建议：

> consider scaling out the read model, but running the write model on only one or a few instances. A small number of write model instances also helps to minimize the occurrence of merge conflicts.

这一点很关键：**读侧横向扩展是廉价的，写侧横向扩展昂贵**，因为写侧扩大会引入跨节点协调。CQRS 的价值就是把可分与不可分的部分先切开。

### 新增的约束（p. 44）

- 读写分库后是最终一致。必须处理「用户基于过期读数据发起写操作」的情况。
- 复杂度上升：需要额外代码生成并同步读模型。
- 原文建议只在最有价值的部分应用 CQRS，而不是全系统。

与 Event Sourcing 合用（p. 45–46）：写侧变成 append-only 的事件流，读侧是由这些事件重放得到物化视图。原文指出这样能「avoid update conflicts on a single aggregate and maximizes performance and scalability」；代价是重放需要大量处理与资源，可用周期性快照缓解。

### 原文示例与代码

读模型接口与专用 DTO（p. 46–47）：

```csharp
namespace ReadModel
{
  public interface ProductsDao
  {
    ProductDisplay FindById(int productId);
    IEnumerable<ProductDisplay> FindByName(string name);
    IEnumerable<ProductInventory> FindOutOfStockProducts();
    IEnumerable<ProductDisplay> FindRelatedProducts(int productId);
  }
  public class ProductDisplay
  {
    public int ID { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public decimal UnitPrice { get; set; }
    public bool IsOutOfStock { get; set; }
    public double UserRating { get; set; }
  }
```

写侧以命令为单位，命令粒度刻意设计得更细，以减少冲突（p. 47）：

```csharp
public class RateProduct : Icommand
{
  public RateProduct()
  {
    this.Id = Guid.NewGuid();
  }
  public Guid Id { get; set; }
  public int ProductId { get; set; }
  public int rating { get; set; }
  public int UserId {get; set; }
}
```

命令处理器（p. 47–48）：

```csharp
public class ProductsCommandHandler :
    ICommandHandler<AddNewProduct>,
    ICommandHandler<RateProduct>,
    ICommandHandler<AddToInventory>,
    ICommandHandler<ConfirmItemShipped>,
    ICommandHandler<UpdateStockFromInventoryRecount>
{
  private readonly IRepository<Product> repository;

  void Handle (RateProduct command)
  {
    var product = repository.Find(command.ProductId);
    if (product != null)
    {
      product.RateProuct(command.UserId, command.rating);
      repository.Save(product);
    }
  }
}
```

写模型接口使用领域语义（`AddToInventory`、`ConfirmItemsShipped`），而不是 CRUD 的 `Save`/`Update`（p. 48）。

---

## 3. Pipes and Filters

**原文页**：p. 100–108

### 详细介绍

把复杂处理拆成一系列可复用的离散步骤（filters），步骤之间通过标准化的数据格式连接（pipes）。每个 filter 只做一件事，可以独立部署、独立伸缩、独立替换。

原文定义（p. 100）：

> Decompose a task that performs complex processing into a series of discrete elements that can be reused. This pattern can improve performance, scalability, and reusability by allowing task elements that perform the processing to be deployed and scaled independently.

### 问题与解法

**问题**（p. 100–101）：把处理写成单体模块时，代码紧耦合、难以复用；更重要的是各步骤的资源需求差异很大——有的计算密集、需要强机器，有的很轻、普通机器即可；未来还要能插入新步骤或调整顺序。单体模块只能整体伸缩，等于为最重的一步付全部机器的钱。

**解法**：拆成 filter，各自独立部署。原文（p. 101）：

> The filters that comprise a pipeline can run on different machines, enabling them to be scaled independently... A filter that is computationally intensive can run on high performance hardware, while other less demanding filters can be hosted on commodity (cheaper) hardware.

于是「处理能力」被分解成「每一段的实例数」，可以按段配置不同规格的资源，而不是整体升级。

### 关键判断与新增约束

- **整体速度由最慢的 filter 决定**（p. 101）。这是最实用的一句：扩容前先找最慢那一段，否则给别段加资源没有意义。原文的应对是对慢 filter 跑并行实例。
- **流式处理可并行**（p. 102）：若输入输出都是流，下游 filter 不必等上游完成即可开始工作。
- **幂等性必需**（p. 102–103）：filter 失败后任务可能被重派给另一个实例，同一份更新可能执行两次。
- **重复消息**：失败重发会造成同一条消息出现两次，管道需去重。
- **上下文与状态**：filter 之间相互隔离，不能假设自己是被谁调用的，必须携带足够上下文；若上下文过大，这个方案就不划算（p. 103）。

### 原文示例与代码

用一串消息队列实现管道：每个 filter 监听上游队列，处理后投递到下游队列（p. 103）。`ServiceBusPipeFilter` 的骨架（p. 104–105）：

```csharp
public class ServiceBusPipeFilter
{
  private readonly string inQueuePath;
  private readonly string outQueuePath;
  private QueueClient inQueue;
  private QueueClient outQueue;

  public void OnPipeFilterMessageAsync(
    Func<BrokeredMessage, Task<BrokeredMessage>> asyncFilterTask, ...)
  {
    this.inQueue.OnMessageAsync(
      async (msg) =>
    {
      // Process the filter and send the output to the
      // next queue in the pipeline.
      var outMessage = await asyncFilterTask(msg);

      if (outQueue != null)
      {
        await outQueue.SendAsync(outMessage);
      }

      // Note: There is a chance that the same message could be sent twice
      // or that a message may be processed by an upstream or downstream
      // filter at the same time.
      // Idempotent message processing and concurrency should be considered
      // in a real-world implementation.
    },
    options);
  }
}
```

原文说明示例中每个 filter 实现为独立的 worker role，可各自按处理复杂度与资源需求伸缩，且同一 role 可并行多实例提升吞吐（p. 105）。

---

## 4. Queue-Based Load Leveling

**原文页**：p. 116–119

### 详细介绍

在任务与被调用的服务之间插入一个队列作为缓冲，把间歇性的高负载抹平，避免服务被打垮或任务超时。

原文定义（p. 116）：

> Use a queue that acts as a buffer between a task and a service that it invokes in order to smooth intermittent heavy loads that may otherwise cause the service to fail or the task to time out.

### 问题与解法

**问题**（p. 116）：服务可能被多方并发调用，请求量难以预测；峰值会让服务过载，大量并发还会因争用直接令服务失效。

**解法**：把同步直连改成「任务 → 队列 → 服务」。队列吸收峰值，服务按自己的节奏消费。原文的收益表述直接点明了成本模型（p. 117）：

> It can help to maximize scalability because both the number of queues and the number of services can be varied to meet demand.

> It can help to control costs because the number of service instances deployed needs only to be sufficient to meet average load rather than the peak load.

这是本仓库「技术问题转资源问题」的一个干净样本：**容量按峰值配置 → 容量按均值配置 + 队列作为弹性储备**。资源投入从「必须覆盖峰值」降为「覆盖均值」，代价是峰值期间的排队延迟。

### 新增的约束（p. 117–118）

- **必须自己控制消费速率**，不能把峰值原样传给下一段；对系统做压测来确认抹平效果，并据此调整队列数与服务实例数。
- **队列是单向的**，需要返回值时要另设回执机制。
- **对队列消费者做 autoscaling 要小心**：扩容会让这些实例争用共享资源，反而削弱抹平效果。这是「扩容引入新瓶颈」的典型。
- 若应用要求极低延迟的响应，此模式不适用（p. 118）。

### 原文示例

Azure web role 直接写独立存储服务，实例一多就把存储服务压垮（p. 118，Figure 2）。而存储服务只接受同步请求、无法改成从队列读，因此引入 worker role 作为代理：从队列取请求，按应用逻辑控制转发速率，再发给存储服务（p. 119，Figure 3）。

这个例子值得注意的地方在**适配成本**：被保护的服务本身不改，额外加一层「队列 + 代理」，用资源换掉了「改造服务」的工作量。

---

## 5. Sharding

**原文页**：p. 140–149

### 详细介绍

把一个数据存储水平拆成多个分片（shards）。每个分片 schema 相同，各自持有数据的一个互不重叠的子集，本身就是一个独立的数据存储，运行在充当存储节点的服务器上。

原文定义（p. 140）：

> Divide a data store into a set of horizontal partitions or shards. This pattern can improve scalability when storing and accessing large volumes of data.

### 问题与解法

**问题**（p. 140）：单服务器数据存储会依次撞上四类上限——存储空间、计算资源、网络带宽、地理位置。纵向扩容（加盘、加内存、升级 CPU）只能推迟，不能消除；原文直接说：

> Scaling vertically ... may postpone the effects of some of these limitations, but it is likely to be only a temporary solution.

**解法**：横向切分。原文列出的收益（p. 141）中第一条即扩容本身：

> You can scale the system out by adding further shards running on additional storage nodes.

以及「可用廉价硬件」「通过均衡负载降低争用」「把分片放在用户附近」。这里的关键是把「单节点容量」这一硬上限，转成了「节点数」这一可增减的资源。

分片键（shard key / partition key）必须**静态**，不能基于会变化的数据（p. 141）。分片逻辑可以放在应用的数据访问层，也可以由存储系统透明提供；抽象出物理位置的好处是数据可以在分片间迁移而不改业务逻辑，代价是每次访问多一层定位开销。

### 三种分片策略（p. 141–145）

| 策略 | 做法 | 优点 | 代价 |
| --- | --- | --- | --- |
| **Lookup** | 维护分片键到物理分片的映射表；可用虚拟分片映射到更少的物理分区 | 控制力最强；虚拟分片使再平衡不必改应用代码 | 每次访问要查映射 |
| **Range** | 按分片键顺序分组，键相邻的数据放在一起 | 范围查询效率高，便于按区域管理数据 | 分布可能不均；再平衡困难 |
| **Hash** | 对键做哈希决定分片 | 分布更均匀，路由不需维护映射表 | 计算开销；再平衡困难 |

原文用多租户例子点出 Range 的隐患（p. 144）：按注册顺序分配，若最新注册的租户最活跃，负载就集中在少数分片；Hash 会把连续租户打散。

再平衡（Scaling and Data Movement Operations，p. 145）：Lookup 可在用户级在线或离线完成迁移；Range 通常需要部分或全部离线做拆分合并；Hash 因键即哈希，迁移最复杂，但不需要维护状态。

### 新增的约束（p. 146–147）

这些正是「扩容后的新瓶颈」，也是本仓库要强调的部分：

- **分片键不能变**：键变化意味着数据要跨片迁移，写放大。
- **键必须唯一**：不能用各分片间不协调的自增字段作分片键。
- **无法为所有查询设计分片键**：只能优先满足最高频的查询，其余靠 Index Table 之类的二级索引。
- **跨片 join 昂贵**：单分片内查询远优于跨分片；常用做法是把常一起查的实体反范式化放进同一分片。
- **扇出查询**（fan-out）：并行查多个分片再聚合，逻辑复杂度上升。
- **跨片引用完整性难以维持**：原文建议尽量减少跨片写，并明确「evaluate whether complete data consistency is actually a requirement」，常用做法是最终一致。
- **运维成本随分片数增长**：监控、备份、一致性检查、审计都要在多分片上做。
- **倾向多而小的分片**：便于负载均衡与迁移，搬小分片比搬大分片快。

### 原文示例与代码

用多个 SQL Server 数据库作分片，应用自己做分片逻辑并发起扇出查询（p. 147–149）：

```csharp
private IEnumerable<ShardInformation> GetShards()
{
  // This retrieves the connection information from a shard store
  // (commonly a root database).
  return new[]
  {
    new ShardInformation
    {
      Id = 1,
      ConnectionString = ...
    },
    new ShardInformation
    {
      Id = 2,
      ConnectionString = ...
    }
  };
}
```

```csharp
var shards = GetShards();
var results = new ConcurrentBag<string>();
Parallel.ForEach(shards, shard =>
{
  // NOTE: Transient fault handling is not included,
  // but should be incorporated when used in a real world application.
  using (var con = new SqlConnection(shard.ConnectionString))
  {
    con.Open();
    var cmd = new SqlCommand("SELECT ... FROM ...", con);
    var reader = cmd.ExecuteReader();
    while (reader.Read())
    {
      results.Add(reader.GetString(0));
    }
  }
});
```

---

## 6. Throttling

**原文页**：p. 155–159

### 详细介绍

控制某个应用实例、某个租户、或整个服务的资源消耗速率。需求超过容量时，系统不是硬扛到崩溃，而是主动限制一部分请求，从而保住整体可用性与 SLA。

原文定义（p. 155）：

> Control the consumption of resources used by an instance of an application, an individual tenant, or an entire service. This pattern can allow the system to continue to function and meet service level agreements, even when an increase in demand places an extreme load on resources.

### 问题与解法

**问题**（p. 155）：负载随时间与活动类型波动，还有突发的、无法预期的尖峰。若处理需求超过可用资源，性能会恶化甚至失败，而 SLA 可能不允许失败。

原文特别指出了 autoscaling 的时间缺口（p. 155）：

> while autoscaling may trigger the provisioning of additional resources, this provisioning is not instantaneous. If demand grows quickly, there may be a window of time where there is a resource deficit.

**解法**：设一个软上限（soft limit），持续监控资源使用，接近上限时开始限制。原文给出四类策略（p. 155–156）：

1. 拒绝单个用户在给定时间窗内超过 *n* 次的请求（需要按租户/用户计量）。
2. 关闭或降级非核心功能，把资源让给核心功能（例：降低视频分辨率）。
3. 用负载均衡化（即 Queue-Based Load Leveling）抹平；多租户场景下可用 Priority Queue 让高价值租户优先。
4. 推迟低优先级应用或租户的操作，并返回「系统繁忙、请稍后重试」。

注意第 3、4 条与本仓库的「成本」视角一致：**限流不是扩容的替代品，而是把「拒绝」从无差别崩溃变成有选择的降级。**

### 与 autoscaling 的配合（p. 156–157）

这是本节最有价值的部分。原文给出明确的时间线：T1 达到软上限 → 开始扩容，但新资源来不及到位 → 临时限流 → 扩容完成 → 放开限流。

> However, if the new resources do not become available sufficiently quickly then the existing resources may be exhausted and the system could fail. To prevent this from occurring, the system is temporarily throttled, as described earlier.

也就是说，**扩容有延迟，限流是这段延迟里的兜底手段**。另外原文指出：若突发很短，直接限流可能比扩容更划算，因为扩容会显著增加运行成本（p. 158）。

### 新增的约束（p. 157–158）

- 限流是架构级决策，**事后再加很难**，应在设计早期考虑。
- 限流必须**快**：能迅速检测到活动上升并响应，负载回落后也要迅速恢复，因此要求持续采集与监控性能数据。
- 拒绝请求时必须返回**明确的错误码**，让客户端知道是限流而非故障，可以稍后重试。
- 若需求增长过快，即使限流也可能撑不住；此时需要预留更多容量或采用更激进的扩容策略。

### 原文示例

多租户问卷系统：监控各租户提交请求的速率，限制单租户每秒请求数，超出即拒绝，防止一个租户影响其他租户的响应性与可用性（p. 158–159，Figure 3）。

---

## 附：与 Guidance 的衔接

原文把三项配套 guidance 单列（不计入上表），它们是把上述模式落到具体容量规划时的必读材料：

- **Autoscaling**（p. 174）——决定「什么时候加、加多少」。
- **Data Partitioning**（p. 197）——分片的容量规划方法。
- **Compute Partitioning**（p. 185）——计算侧的分区方法。

三项在本书中各自成章，本文未收录；需要时按上述页码查阅原书。
