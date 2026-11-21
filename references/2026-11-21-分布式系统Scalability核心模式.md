# 以 Scalability 为核心目的的分布式模式

**来源**：Unmesh Joshi, *Patterns of Distributed Systems*，1st Edition，Addison-Wesley Professional，2023。ISBN 978-0-13-822224-6（电子书）/ 978-0-13-822198-0（印刷版）。
**选取口径**：仅取原书索引「五、专题索引」中标记为 **A（核心目的）** 的 6 个模式。B（主要收益）与 C（间接相关）不在本文范围。
**判定基准**（原文口径）：满足其一即入选——让吞吐/容量随节点数增长、直接降低单请求成本、或明确指出规模上限。

| 模式 | 印刷页 | 扩展机制归类 |
| --- | --- | --- |
| [Singular Update Queue](#1-singular-update-queue) | 159–166 | 降低单请求成本：用单线程替代大锁 |
| [Follower Reads](#2-follower-reads) | 183–192 | 横向扩展读侧：读吞吐随副本数增长 |
| [Fixed Partitions](#3-fixed-partitions) | 219–242 | 横向扩展写侧：分区数固定，扩容只搬部分分区 |
| [Key-Range Partitions](#4-key-range-partitions) | 243–256 | 横向扩展写侧：按 size/负载自动分裂 |
| [Request Batch](#5-request-batch) | 399–404 | 降低单请求成本：摊薄网络与序列化开销 |
| [Request Pipeline](#6-request-pipeline) | 405–408 | 提高单连接利用率：喂满服务端队列 |

**页码说明**：`p. N` 为印刷版页锚点，取自出版社官方目录，标注到章与 Problem/Solution/Examples 三级。章内 `###` 级小节无独立页码，本文只标注其所属章的页码范围。
**代码说明**：以下代码均为原文节选，逐行照录，未改动任何保留行；省略处用 `…` 标出（原文自带的 `...` 保留原样）。

---

## 1. Singular Update Queue

**印刷页**：159–166（Problem p. 159、Solution p. 159、Examples p. 166）

### 详细介绍

用单个线程异步处理请求，从而在不阻塞调用方的前提下保持顺序。

原文定义（p. 159）：

> Use a single thread to process requests asynchronously to maintain order without blocking the caller.

结构是两段式：一个线程安全的队列 + 一个专职消费线程。多个并发客户端各自在自己的线程上把请求放进队列，立即拿到一个 `CompletableFuture` 并返回；专职线程按到达顺序逐个取出、执行、完成对应的 future。

原文指出这在 Go、Kotlin 这类支持 goroutine/channel 的语言里是天然写法，在 Java 里则需要显式用 `Thread` 或单线程 `ExecutorService`。

### 它解决的问题与解法

**问题**（p. 159）：多个并发客户端要更新同一份状态，必须一次只改一个。常规做法是加锁，但如果每个任务耗时较长（例如写文件），**阻塞其他调用线程等待锁**会严重损害整体吞吐与延迟。要在「保证串行」和「不浪费算力」之间同时满足。

**解法**：把「互斥」换成「排队」。客户端不争锁，只把请求投递到队列就返回，因此调用方永远不会被长时间阻塞；真正的状态变更由唯一线程串行执行。

这个模式精准对应本仓库的那条主线：**把「锁竞争」这个技术约束，换成「队列容量 + 单线程处理速率」这个资源约束。** 单节点天花板由此变成「消费者线程的处理速度」，而不再受锁等待影响。

原文给的实现是继承 `Thread`，自建队列与处理函数：

```
public class SingularUpdateQueue<Req, Res> extends Thread implements Logging {
  private ArrayBlockingQueue<RequestWrapper<Req, Res>> workQueue
       = new ArrayBlockingQueue<RequestWrapper<Req, Res>>(100);
  private Function<Req, Res> handler;
  private volatile boolean isRunning = false;
```

提交请求时用 wrapper 把请求和 future 绑在一起，future 返回给客户端（p. 159–160）：

```
public CompletableFuture<Res> submit(Req request) {
  try {
     var requestWrapper = new RequestWrapper<Req, Res>(request);
     workQueue.put(requestWrapper);
     return requestWrapper.getFuture();
  }
  catch (InterruptedException e) {
     throw new RuntimeException(e);
  }
 }
```

专职线程循环取任务、执行、完成 future：

```
@Override
public void run() {
  isRunning = true;
  while(isRunning) {
    Optional<RequestWrapper<Req, Res>> item = take();
    item.ifPresent(requestWrapper -> {
       try {
          Res response = handler.apply(requestWrapper.getRequest());
          requestWrapper.complete(response);

       } catch (Exception e) {
         requestWrapper.completeExceptionally(e);
       }
     });
   }
}
```

原文强调用带超时的 `poll` 而不是永久阻塞的 `take`，这样才能干净地关闭线程：

```
private Optional<RequestWrapper<Req, Res>> take() {
  try {
    return Optional.ofNullable(workQueue.poll(2, TimeUnit.MILLISECONDS));

  } catch (InterruptedException e) {
    return Optional.empty();
  }
}

public void shutdown() {
  this.isRunning = false;
}
```

### 新增的约束

原文在章内讨论了四个容易踩的坑：

- **队列类型的选择**（p. 159–166 内）：`ArrayBlockingQueue` 提供阻塞式背压（Kafka 请求队列用）；`ConcurrentLinkedQueue` + ForkJoinPool 适合由协调者调度消费者（Akka Actor mailbox）；`LinkedBlockingDeque`（ZooKeeper 与 Kafka 响应队列）；对延迟极敏感时用 `RingBuffer`（LMAX Disruptor）。
- **背压**：生产快、消费慢时无界队列会吃光内存。原文明确 `ArrayBlockingQueue` 的 `put` 会阻塞生产者、`add` 会抛异常但不阻塞——**要背压就必须用 `put`**。
- **任务串联**：一个阶段的结果要交给下一阶段时，可在 future 回调里提交到另一个 SingularUpdateQueue。
- **禁止在唯一线程上做阻塞网络调用**；调用要异步，且**不要在异步回调里访问该队列的状态**——回调在别的线程执行，会破坏「所有状态变更都走单线程」的前提。结果应作为新事件重新入队。

### 原文示例（p. 159–166）

以 Write-Ahead Log 消费者为例：把请求放进队列，写入 WAL 后再返回响应，保证写入与响应的相对顺序。

```
public class WalRequestConsumer implements Consumer<Message<RequestOrResponse>> {

  private final SingularUpdateQueue<Message<RequestOrResponse>, Message<RequestOrResponse>> walWriterQueue;
  private final WriteAheadLog wal;

  public WalRequestConsumer(Config config) {
    this.wal = WriteAheadLog.openWAL(config);
  walWriterQueue = new SingularUpdateQueue<>((message) -> {
    wal.writeEntry(serialize(message));
    return responseMessage(message);
  });
  startHandling();
}
```

`accept` 在调用者线程上运行，因此多个调用者可同时提交：

```
@Override
public void accept(Message message) {
  CompletableFuture<Message<RequestOrResponse>> future = walWriterQueue.submit(message);
  future.whenComplete((responseMessage, error) -> {
    sendResponse(responseMessage);
  });
}
```

原文给出的 Go 等价实现（channel + 单 goroutine）：`s.requestChannel <- request` 投递，`singularUpdateQueue()` 中 `for { select { case e := <-s.requestChannel: ... } }` 串行处理。

**Examples（p. 166）**：ZooKeeper（ZAB）、etcd（Raft）等共识实现都用这种结构处理严格有序的请求；Kafka Controller 用单线程处理来自 ZooKeeper 的并发事件；Cassandra 的 SEDA 单线程 stage 更新 Gossip 状态；LMAX Disruptor 遵循 Single Writer Principle 避免互斥。

**为什么它算 Scalability**：它不增加节点，而是**抬高单节点天花板**——同样的 CPU，不再被锁等待空转。原文的判定口径把「直接降低单请求成本」也计入扩展手段，正是此意。

---

## 2. Follower Reads

**印刷页**：183–192（Problem p. 183、Solution p. 183、Examples p. 191）

### 详细介绍

把只读请求发给 follower 副本，以获得更高吞吐和更低延迟。

原文定义（p. 183）：

> Serve read requests from followers to achieve better throughput and lower latency

写请求仍必须发给 leader 以保持一致性；只读请求可以发给最近的 follower。这在「客户端以读为主」时尤其有效。

### 它解决的问题与解法

**问题**（p. 183）：采用 Leader and Followers 模式时，所有请求都发给 leader，leader 可能过载；多数据中心部署时，远端客户端访问 leader 还要额外承担跨机房延迟。

**解法**：把读流量从 leader 卸载到副本。于是**读吞吐随副本数线性增长**，并且可以把读请求路由到地理上最近的副本，从而降低延迟。

原文同时给出一条不能忽略的前提（p. 183）：从 follower 读可能拿到旧值。**即使实现了 Raft 这类共识算法，leader 与 follower 之间也始终存在复制延迟**——因为 leader 知道哪些值已提交之后，还需要再一条消息才能告知 follower。因此 follower 读只适用于能容忍轻微陈旧数据的场景。

### 三个必须处理的附加问题

**（1）找到最近的副本**（Solution 内，p. 183–191）

节点维护自身的 region 元数据，客户端优先选同 region 的 follower：

```
public List<String> get(String key) {
  List<ReplicaDescriptor> allReplicas = allFollowerReplicas(key);
  ReplicaDescriptor nearestFollower = findNearestFollowerBasedOnLocality(allReplicas, clientRegion);
  GetValueResponse getValueResponse = sendGetRequest(nearestFollower.getAddress(), new GetValueRequest(key));
  return getValueResponse.getValue();
}
```

若没有同 region 副本，则退回全部副本：

```
ReplicaDescriptor findNearestFollowerBasedOnLocality(List<ReplicaDescriptor> followers, String clientRegion) {
  List<ReplicaDescriptor> sameRegionFollowers = matchLocality(followers, clientRegion);
  List<ReplicaDescriptor> finalList = sameRegionFollowers.isEmpty()?followers:sameRegionFollowers;
  return finalList.get(0);
}
```

更精细的做法是**测量延迟**。原文指出 MongoDB、CockroachDB 用移动平均，且因为节点间本来就有 Single Socket Channel + HeartBeat，采集延迟几乎零成本：

```
public class WeightedAverage {
  long averageLatencyMs = 0;
  public void update(long heartbeatRequestLatency) {
    //Example implementation of weighted average as used in Mongodb
    //The running, weighted average round trip time for heartbeat messages to the target node.
    // Weighted 80% to the old round trip time, and 20% to the new round trip time.
    averageLatencyMs = averageLatencyMs == 0
         ? heartbeatRequestLatency
         : (averageLatencyMs * 4 + heartbeatRequestLatency) / 5;
 }

 public long getAverageLatency() {
   return averageLatencyMs;
 }
```

**（2）断连或过慢的 follower**（p. 183–191 内）

follower 可能断连或磁盘变慢而落后。做法是：follower 发现自己一段时间没收到 leader 的消息就**停止服务读请求**。MongoDB 允许配置最大允许滞后时间（max staleness），超过则不选它；Kafka 中若消费者请求的 offset 太超前，follower 返回 `OFFSET_OUT_OF_RANGE`，由消费者改向 leader 请求。

**（3）Read Your Own Writes**（p. 183–191 内）

这是 follower 读最典型的可用性陷阱：客户端写入后立刻读，读落到还没同步的 follower 上，看不到自己刚写的值。

解法是给每次写附带一个单调递增的版本戳（High-Water Mark 或 Hybrid Clock），写响应把版本戳返回给客户端；读请求带上该版本戳，follower 若版本不足则**等待**到追上再返回：

```
public CompletableFuture<Optional<String>> get(String key, int atVersion) {
  if(this.replicatedLog.getRole() == ServerRole.FOLLOWING) {
     //check if we have the version with us;
     if (!isVersionUptoDate(atVersion)) {
       //wait till we get the latest version.
       CompletableFuture<Optional<String>> future = new CompletableFuture<>();
       //Timeout if version does not progress to required version
       //before followerWaitTimeout ms.
       future.orTimeout(config.getFollowerWaitTimeoutMs(), TimeUnit.MILLISECONDS);
       waitingRequests.put(atVersion, future);
       return future;
    }
  }
  return CompletableFuture.completedFuture(mvccStore.get(key, atVersion));
}

private boolean isVersionUptoDate(int atVersion) {
  return version >= atVersion;
}
```

原文明确要求**等待必须有超时**，否则 follower 长期落后会把请求挂死；超时后返回错误，由客户端改读其他副本。版本追平时完成挂起的请求：

```
private void completeWaitingFuturesIfFollower(int version, String value) {
  CompletableFuture completableFuture = waitingRequests.remove(version);
  if (completableFuture != null) {
     logger.info("Completing pending requests for version " + version + " with " + value);
     completableFuture.complete(Optional.of(value));
  }
}
```

**（4）线性一致读**：需要读到最新数据、不能容忍复制延迟时，读请求必须转给 leader。

### 原文示例（p. 191）

- **Neo4j**：causal cluster，每次写返回 bookmark，读副本时带上 bookmark，保证读到该写之后的状态。
- **MongoDB**：副本集维护 causal consistency，写返回 `operationTime`，后续读带上它。
- **CockroachDB**：leader 发布 **closed timestamp**，follower 只服务不晚于该时间戳的读。
- **Kafka**：follower 知道 leader 的 High-Water Mark；若消费者请求超出，返回错误让消费者重试。

---

## 3. Fixed Partitions

**印刷页**：219–242（Problem p. 219、Solution p. 220、Examples p. 241）

### 详细介绍

固定分区数量，使得集群规模变化时「数据 → 分区」的映射保持不变。

原文定义（p. 219）：

> Keep the number of partitions fixed to keep the mapping of data to the partition unchanged when size of a cluster changes.

机制是两跳映射：**键 → 逻辑分区**（哈希取模分区数），**逻辑分区 → 集群节点**（由协调者分配）。节点增减只影响第二跳，第一跳的映射完全不变。

### 它解决的问题与解法

**问题**（p. 219）：把数据映射到节点有两个硬要求——分布均匀，且**不查所有节点就能知道某条数据在哪个节点**。用 `hash(key) % N` 可以同时满足。但集群规模一变就出事：3 节点变 5 节点时，几乎所有键的归属都会改变，**全部数据都要搬迁**。

**解法**：引入固定的逻辑分区层。集群启动时预先配置分区数（例如 1024），此后不再改变；键哈希映射到分区，分区再映射到节点。加节点时只需把**部分整个分区**搬到新节点，而不是重算全部键的归属。

原文给出的关键约束（p. 220）：

> So the number of partitions selected should be significantly higher than the number of cluster nodes.

并给出业界默认值：Akka 建议分片数是节点数的 10 倍，Apache Ignite 默认 1024，Hazelcast 在集群规模小于 100 时默认 271。

**另一个常被忽略的理由**（p. 220 侧栏）：像 Kafka 这样的消息系统需要**分区内有序**。固定分区保证「数据到分区」的映射不变，因此分区在节点间搬迁不会破坏分区内的顺序。

### 哈希函数的选择（p. 220）

必须与平台和运行时无关。Java 的 `Object.hashCode()` 依赖 JVM 运行时，同一个键在不同 JVM 上可能得到不同哈希；因此实际使用 MD5 或 Murmur 这类确定性算法：

```
public static BigInteger hash(String key)
{
  try
  {
    MessageDigest messageDigest = MessageDigest.getInstance("MD5");
    return new BigInteger(messageDigest.digest(key.getBytes()));
  }
  catch (Exception e)
  {
    throw new RuntimeException(e);
  }
}
```

### 分区到节点的映射（p. 220）

映射需要存下来并让客户端可访问。原文说常见做法是用一个专用的 **Consistent Core** 当协调者：跟踪所有节点、保存分区到节点的映射，并用 Replicated Log 保证映射本身容错。YugabyteDB 的 master cluster、Kafka 的 controller 都是这个形态；Akka、Hazelcast 这类 P2P 系统用 Emergent Leader 充当协调者；Kubernetes 用 etcd 这类通用 Consistent Core。

### 搬迁分区的做法（p. 219–241 内）

原文给的策略是算平均值后按差额搬：30 个分区、3 个节点时每节点 10 个；加第 4 个节点后均值约 7，于是从每个现有节点搬 3 个分区给新节点。

```
public List<Migration> repartition(PartitionTable partitionTable) {
  int averagePartitionsPerNode = getAveragePartitionsPerNode();
  List<Member> liveMembers = membership.getLiveMembers();
  var overloadedNodes = partitionTable.getOverloadedNodes(averagePartitionsPerNode, liveMembers);
  var underloadedNodes = partitionTable.getUnderloadedNodes(averagePartitionsPerNode, liveMembers);

  var migrations = tryMovingPartitionsToUnderLoadedMembers(averagePartitionsPerNode, overloadedNodes, underloadedNodes);
  return migrations;
}
```

搬迁指令先写进 Replicated Log，再执行，保证协调者故障后能恢复：

```
private void applyMigratePartitionCommand(MigratePartitionsCommand command) {
  logger.info("Handling partition migrations " + command.migrations);
```

原文还提醒：搬迁涉及大量数据移动，实际系统通常由管理员触发而非全自动。

### 新增的约束

- **分区数上限即节点数上限**：分区数一经配置不再改变，所以必须按未来增长留足空间；但分区太多也有管理开销。
- **分区应显著多于节点**，否则增量节点只能拿到很少分区，甚至只能靠拆分现有分区。
- 哈希必须确定性；不能依赖语言运行时默认哈希。

### 原文示例（p. 241）

Kafka 每个 topic 创建时即固定分区数；Akka Cluster Sharding 固定 shard 数并建议为集群规模的 10 倍；Apache Ignite 与 Hazelcast 的缓存分区数固定。

### 对照：按节点数比例分区（p. 219–241 内）

原文用一整节介绍 Cassandra 式替代方案（即常说的 consistent hashing）：每个节点分配随机 token，数据按键哈希落在「第一个大于它的 token」所属节点。加节点只搬相邻区间的一小部分数据。

但原文明确指出其缺陷：

> This basic technique of assigning a single token to each node has shown to create data imbalance.

原因是新节点加入时搬迁负担压在**某一个**现有节点上。Cassandra 因此改为每节点多个 token（默认 256），使数据从多个节点各搬一点。原文的结论是：**多数数据系统仍使用固定分区**。

---

## 4. Key-Range Partitions

**印刷页**：243–256（Problem p. 243、Solution p. 244、Examples p. 255）

### 详细介绍

按有序的键区间分区，从而高效支持范围查询。

原文定义（p. 243）：

> Partition data in sorted key ranges to efficiently handle range queries.

分区边界由**分割点（split key）**定义，每个分区是一段 `[startKey, endKey)`。

### 它解决的问题与解法

**问题**（p. 243）：用哈希映射（即 Fixed Partitions 的做法）时，要查「a 到 f 开头的所有名字」这类范围查询，**无法知道该去哪些分区取数据**，只能查询所有分区再合并。对单个请求查全部分区远非最优。

**解法**：让分区边界对应**键的顺序**而不是哈希值。这样范围查询只需定位起止键所在的分区，只查这几个分区。

代价是分布可能不均（相邻键若集中在少数区间会造成热点），这一点由下面的自动分裂与负载分裂机制缓解。

### 预定义区间（p. 244）

若已知键空间与分布，可预先指定分割点。原文用空串表示最小键与最大键：

```
List<Range> createRangesFromSplitPoints(List<String> splits) {
  List<Range> ranges = new ArrayList<>();
  String startKey = Range.MIN_KEY;
  for (String split : splits) {
    String endKey = split;
    ranges.add(new Range(startKey, endKey));
    startKey = split;
  }
  ranges.add(new Range(startKey, Range.MAX_KEY));
  return ranges;
}
PartitionTable arrangePartitions(List<Range> ranges, List<Member> liveMembers) {
  PartitionTable partitionTable = new PartitionTable();
  for (int i = 0; i < ranges.size(); i++) {
    //simple round-robin assignment.
    Member member = liveMembers.get(i % liveMembers.size());
    int partitionId = newPartitionId();
    Range range = ranges.get(i);
    PartitionInfo partitionInfo = new PartitionInfo(partitionId, member.getAddress(), PartitionStatus.ASSIGNED, range);
    partitionTable.addPartition(partitionId, partitionInfo);
  }
  return partitionTable;
}
```

映射同样由协调者用 Replicated Log 容错保存。

**示例场景**（p. 243–255 内）：三个服务器 athens、byzantium、cyrene，分割点为 `"b"` 和 `"d"`，生成三段区间并分配到三个节点。客户端要查 a–c 开头的名字时，只向覆盖这些键的分区发请求。

### 自动分裂（p. 243–255 内）

多数情况下无法预先知道合适的分割点。此时协调者先只建**一个覆盖全部键空间的分区**：

```
public PartitionTable createPartitionTableWithOneRange() {
  PartitionTable partitionTable = new PartitionTable();
  List<Member> liveMembers = membership.getLiveMembers();
  Member member = liveMembers.get(0);
  Range firstRange = new Range(Range.MIN_KEY, Range.MAX_KEY);
  int partitionId = newPartitionId();
  partitionTable.addPartition(partitionId, new PartitionInfo(partitionId, member.getAddress(), PartitionStatus.ASSIGNED, firstRange));
  return partitionTable;
}
```

每个分区配置最大尺寸，节点上的后台任务定期检查，超限就把分区一分为二：

```
public void scheduleSplitCheck() {
  scheduler.scheduleAtFixedRate(()->{
    splitCheck();
  }, 1000, 1000, TimeUnit.MILLISECONDS);
}

int MAX_PARTITION_SIZE = 1000;
public void splitCheck(Partition partition) {
  String middleKey = partition.getMiddleKeyIfSizeCrossed(MAX_PARTITION_SIZE);
  if (!middleKey.isEmpty()) {
     logger.info("Partition " + partition.getId() + " reached size " + partition.size() + ". Triggering split");
     network.send(coordLeader, new SplitTriggerMessage(partition.getId(), middleKey, requestNumber++, listenAddress));
  }
}
```

**算尺寸与找中点**：全文扫描分区成本很高。原文提到 TiKV 把分区尺寸与对应键一起存进数据存储，从而无需全扫即可求出中点；YugabyteDB、HBase 这类「每分区一个 store」的实现则通过扫描 store 文件元数据估算中点。

**一致性保护**：客户端若缓存了旧的分区元数据，可能把请求发错分区。做法是给分区加 **Generation Clock**，每次分裂递增；携带旧代号的请求被拒绝，客户端重新加载分区表后重试。YugabyteDB 则选择另建两个新分区并标记原分区为已删除。

### 新增的约束：冷启动单点

这是本章最值得注意的一条（p. 243–255 内）。自动分裂从**单个区间**开始，意味着在第一次分裂发生之前，**所有客户端请求都压在一个节点上**，即使集群里还有其他空闲节点。原文因此指出需要按负载触发分裂：

> This is why sometimes splitting on parameters such as total nunmber of requests, or CPU, and memory usage are also used to trigger a partition split.

CockroachDB 与 YugabyteDB 都支持基于负载的分裂。

这也正是本仓库反复强调的模式：**扩容的前提是分区已经存在**；分区尚未形成时，加节点没有收益。

### 原文示例（p. 255）

HBase、CockroachDB、YugabyteDB、TiKV 均支持范围分区。

---

## 5. Request Batch

**印刷页**：399–404（Problem p. 399、Solution p. 399、Examples p. 404）

### 详细介绍

把多个请求合并成一批，以充分利用网络。

原文定义（p. 399）：

> Combine multiple requests to optimally utilise the network.

### 它解决的问题与解法

**问题**（p. 399）：当大量小请求发往集群节点时，网络延迟与服务端处理时间（含序列化/反序列化）会成为显著开销。原文给了量化例子：网络容量 1 Gbps、延迟与处理时间约 100 微秒时，客户端同时发几百个「只有几字节」的请求，每个都要 100 微秒完成，整体吞吐会被严重限制。

**解法**：客户端不立即发送，而是把请求先放进队列；积累到批量上限、或最老请求等待超过设定时间窗，就把整批打包发出。服务端拆包，逐个按单请求处理，再整批回包。

这是典型的**把「每请求固定成本」摊薄到多请求上**——单请求成本下降，单节点天花板抬高。

原文的客户端实现：提交时不发网络请求，只入队并登记 future：

```
LinkedBlockingQueue<RequestEntry> requests = new LinkedBlockingQueue<>();

public CompletableFuture send(SetValueRequest setValueRequest) {
  int requestId = enqueueRequest(setValueRequest);
  CompletableFuture responseFuture = trackPendingRequest(requestId);
  return responseFuture;
}

private int enqueueRequest(SetValueRequest setValueRequest) {
  int requestId = nextRequestId();
  byte[] requestBytes = serialize(setValueRequest, requestId);
  requests.add(new RequestEntry(requestBytes, clock.nanoTime()));
  return requestId;
}
```

入队时间被记录，用于判断是否已等待够久：

```
class RequestEntry {
  byte[] serializedRequest;
  long createdTime;
  
  public RequestEntry(byte[] serializedRequest, long createdTime) {
    this.serializedRequest = serializedRequest;
    this.createdTime = createdTime;
}
```

用 correlationId 把响应映射回挂起的请求：

```
Map<Integer, CompletableFuture> pendingRequests = new ConcurrentHashMap<>();

private CompletableFuture trackPendingRequest(Integer correlationId) {
  CompletableFuture responseFuture = new CompletableFuture();
  pendingRequests.put(correlationId, responseFuture);
  return responseFuture;
}
```

独立线程按两个条件之一决定是否发包：

```
@Override
public void run() {
  while (isRunning) {
    boolean maxWaitTimeElapsed = requestsWaitedFor(config.getMaxBatchWaitTime());
    boolean maxBatchSizeReached = maxBatchSizeReached(requests);
    if (maxWaitTimeElapsed || maxBatchSizeReached) {
       RequestBatch batch = createBatch(requests);
       try {
          BatchResponse batchResponse = sendBatchRequest(batch, address);
          handleResponse(batchResponse);
  
       } catch (IOException e) {
         batch.getPackedRequests().stream().forEach(r -> {
         pendingRequests.get(r.getCorrelationId()).completeExceptionally(e);
       });
     }
   }
 }
}
```

装填逻辑：只要批里还有空间就继续取：

```
private RequestBatch createBatch(LinkedBlockingQueue<RequestEntry> requests) {
  RequestBatch batch = new RequestBatch(MAX_BATCH_SIZE_BYTES);
  RequestEntry entry = requests.peek();
  while (entry != null && batch.hasSpaceFor(entry.getRequest())) {
    batch.add(entry.getRequest());
    requests.remove(entry);
    entry = requests.peek();
  }
  return batch;
}
```

两个触发条件各自的判定：

```
private boolean maxBatchSizeReached(Queue<RequestEntry> requests) {
  return accumulatedRequestSize(requests) > MAX_BATCH_SIZE_BYTES;
}

private boolean requestsWaitedFor(long batchingWindowInMs) {
  RequestEntry oldestPendingRequest = requests.peek();
  if (oldestPendingRequest == null) {
     return false;
  }
  long oldestEntryWaitTime = clock.nanoTime() - oldestPendingRequest.createdTime;
  return oldestEntryWaitTime > batchingWindowInMs;
}
```

服务端拆包逐个处理：

```
private void handleBatchRequest(RequestOrResponse batchRequest, ClientConnection clientConnection) {
  RequestBatch batch = JsonSerDes.deserialize(batchRequest.getMessageBodyJson(), RequestBatch.class);
  List<RequestOrResponse> requests = batch.getPackedRequests();
  List<RequestOrResponse> responses = new ArrayList<>();
  for (RequestOrResponse request : requests) {
    RequestOrResponse response = handleSetValueRequest(request);
    responses.add(response);
  }
  sendResponse(batchRequest, clientConnection, new BatchResponse(responses));
}
```

### 新增的约束（p. 399–404 内）

- **批量大小要调，不是越大越好。** 原文：过大的批量（MB 级）会带来额外处理开销，收益递减。给出两个参照：Kafka 默认批量 16 KB，`linger.ms` 默认 0；消息更大时应用更大批量。批量参数通常通过压测确定。
- **通常与 Request Pipeline 搭配**才能同时改善吞吐与延迟。
- **重试要幂等**：带退避策略重试时，整批会重发，而服务端可能已处理了其中一部分；必须配合 **Idempotent Receiver** 才能安全重试。

### 原文示例（p. 404）

Kafka 支持生产者批量；BookKeeper 用类似方式批量刷盘；TCP 的 Nagle 算法就是把多个小包合并发送。

---

## 6. Request Pipeline

**印刷页**：405–408（Problem p. 405、Solution p. 405、Examples p. 408）

### 详细介绍

在连接上连续发送多个请求，不等上一个请求的响应返回。

原文定义（p. 405）：

> Improve latency by sending multiple requests on the connection without waiting for the response of the previous requests.

### 它解决的问题与解法

**问题**（p. 405）：用 Single Socket Channel 通信时，若每个请求都要等前一个响应，服务端的请求队列填不满，容量被浪费。原文举 Singular Update Queue 为例：服务端在**处理中**仍然可以接受新请求直到队列填满；如果一次只发一个请求，大部分服务端容量被无谓浪费。

**解法**：把「发送」与「接收」拆成两个线程。发送线程只负责把请求写进 socket，不等待；接收线程持续读响应并处理（可直接处理，也可提交给 Singular Update Queue）。

发送端不等待响应：

```
public void sendOneWay(RequestOrResponse request) throws IOException {
  var dataStream = new DataOutputStream(socketOutputStream);
  byte[] messageBytes = serialize(request);
  dataStream.writeInt(messageBytes.length);
  dataStream.write(messageBytes);
}
```

独立的响应线程：

```
class ResponseThread extends Thread implements Logging {
  private volatile boolean isRunning = false;
  private SingleSocketChannel socketChannel;

  public ResponseThread(SingleSocketChannel socketChannel) {
    this.socketChannel = socketChannel;
  }

  @Override
  public void run() {
    try {
      isRunning = true;
      logger.info("Starting responder thread = " + isRunning);
      while (isRunning) {
        doWork();
     }

  } catch (IOException e) {
    e.printStackTrace();
    getLogger().error(e); //thread exits if stopped or there is IO error
  }
}
```

### 新增的两个约束

原文明确指出 pipeline 会引入两个必须处理的问题（p. 405–408 内）：

**（1）必须限制在途请求数。** 无节制地发会把接收方压垮。做法是用一个有界队列记录每个地址的在途请求，队列满即阻塞发送方；收到响应就腾出位置。原文示例把每个 socket 连接的在途上限设为 5：

```
private final Map<InetAddressAndPort, ArrayBlockingQueue<RequestOrResponse>> inflightRequests = new ConcurrentHashMap<>();
private int maxInflightRequests = 5;
public void send(InetAddressAndPort to, RequestOrResponse request) throws InterruptedException {
  ArrayBlockingQueue<RequestOrResponse> requestsForAddress = inflightRequests.get(to);
  if (requestsForAddress == null) {
     requestsForAddress = new ArrayBlockingQueue<>(maxInflightRequests);
     inflightRequests.put(to, requestsForAddress);
  }
  requestsForAddress.put(request);
```

收到响应后移除对应的在途记录，并校验顺序：

```
private void consume(SocketRequestOrResponse response) {
  Integer correlationId = response.getRequest().getCorrelationId();
  Queue<RequestOrResponse> requestsForAddress = inflightRequests.get(response.getAddress());
  RequestOrResponse first = requestsForAddress.peek();
  if (correlationId != first.getCorrelationId()) {
     throw new RuntimeException("First response should be for the first request");
  }
  requestsForAddress.remove(first);
  responseConsumer.accept(response.getRequest());
}
```

**（2）失败与重试会打乱顺序。** 原文举例：两个请求在途，第一个失败重试，服务端可能先处理了第二个；服务器必须能拒绝乱序请求，否则重试会导致消息重排。

原文给出的两个真实做法：Raft 每条日志项都带上「期望的前一条日志索引」，不匹配即拒绝；Kafka 允许 `max.in.flight.requests.per.connection` 大于 1，但需要幂等生产者——给每批消息分配唯一标识，broker 检查序列号并拒绝乱序请求。

### 原文示例（p. 408）

所有共识算法（Zab、Raft）都支持请求管道；Kafka 鼓励客户端使用 pipeline 提升吞吐。

---

## 六个模式合起来看

这六个模式覆盖了扩展的两条不同路径，正好对应本仓库的两条判定线：

**抬高单节点天花板（不增加节点）**

- Singular Update Queue：把锁等待换成排队，消除空转。
- Request Batch：把每请求固定成本摊到多个请求上。
- Request Pipeline：把连接的往返等待填满，提高利用率。

这三者的共同点：**都不改变集群拓扑，只降低单位请求的资源消耗**。它们的天花板是单机资源上限，收益是一次性的，但成本最低、最先应该做。

**横向扩展（增加节点才有效）**

- Fixed Partitions：固定分区数，扩容只搬部分分区，映射不变。
- Key-Range Partitions：按范围切分，自动分裂，但冷启动期只有一个分区压单节点。
- Follower Reads：读吞吐随副本数增长，代价是复制延迟与 read-your-writes 问题。

这三者的共同点：**必须先有「可分割的单元」，扩容才成立**。Fixed Partitions 的单元是逻辑分区，Key-Range 的单元是键区间，Follower Reads 的单元是副本——没有这些单元，加节点不会带来任何收益。

原文的一句提示把这两条路径接了起来：Request Batch 通常与 Request Pipeline 搭配使用，而 Partition 相关模式又需要 Consistent Core 与 Replicated Log 支撑。也就是说，**单机优化与横向扩展在真实系统里是叠加使用，而不是二选一**。
