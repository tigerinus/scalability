# Scalability

本仓库用于编写一份关于 Scalability（可扩展性）的技术文档。

## 文档目标

1. **系统化归纳 Scalability 的方法论**：把扩容这件事从"堆机器"整理成一套可复用的分析流程——定义负载、设定目标、定位瓶颈、选择手段、验证结果。
2. **按系统类型阐述扩展设计**：针对不同负载特征的系统（读多写少、写密集、事件流、实时推送、搜索分析等），说明各自的瓶颈来源、可用的扩展手段以及代价。
3. **提供可运行的 demo**：用一个贯穿案例逐步演进，用压测和监控数据证明每项手段解决了什么问题、又引入了什么新问题，而不是只给架构图。

## 文档定位

- **目标读者**：高级软件开发工程师。
- **Demo 技术栈**：Golang。

## 核心主张

### Headline

工程师实现 Scalability 的目标，是把技术问题转换成资源问题。即当系统无法满足需求的时候，通过投入更多的资源即可解决，而不是改代码打补丁把问题当成 Bug 去修。

### 展开

Scalability 的工作不是把技术问题当作一次性的功能 Bug 修掉，而是建立一套可度量、可决策的扩展机制：明确负载如何增长、设定可接受的性能与可靠性目标、识别限制系统能力的瓶颈，再判断该瓶颈能否通过增加或调整资源来缓解。

在狭义层面，资源可以是算力、内存、存储、网络带宽和 I/O 等容量。承载这些资源的单元可能是进程、容器、虚拟机或物理机；横向扩展时，则需要把一份资源与一份可独立处理工作的能力组合成可复制的扩容单元。

因此，扩容不是盲目堆机器。横向扩展必须先建立可分割的工作单元，例如分区、副本或独立的处理阶段；分割之后，系统才能把负载分摊到多个实例上。每一次资源化都要回答：增加的资源能否改善目标指标、新瓶颈在哪里、协调成本是否抵消了收益，以及总成本是否可接受。

性能曲线和压测用于验证这条资源化是否成立。它们展示资源饱和度与吞吐、延迟、错误率等指标的关系，并帮助识别线性区、趋缓区、峰值、退化区和失效区。可运营点由服务目标决定，不一定是最大吞吐量。

并不是所有技术约束都能资源化。串行依赖、共享状态、跨节点一致性和协调开销会在加资源后成为新的瓶颈；当资源扩展有延迟或代价过高时，限流、降级、调度、缓存、代码优化或架构重构也可能是更合理的选择。最终目标不是证明系统“能加机器”，而是证明在给定负载下，投入资源能够以可接受的成本维持目标。



## 范围

- 包含：方法论、负载模型、常见瓶颈、扩展模式与取舍分析、demo 与压测结果。
- 暂不包含：具体厂商产品的操作手册、与扩展性无关的通用架构选型讨论。

## 大纲

### 1. 为什么需要重新定义 Scalability

- 数据系统从可靠性、可扩展性、可维护性三个维度看待，不能用“能否加机器”作单一标签。
- Scalability 是负载增长时仍能满足可接受的延迟、可靠性和成本，核心目标是把技术问题转化为资源问题。
- 边界：功能性 bug 不属于扩展问题；串行、共享状态、跨节点一致性和协调开销不能靠堆资源自动消除。
- **文献引用**：DDIA Ch.1《Reliable, Scalable, and Maintainable Applications》；定义三维目标、扩展不是一维标签，并建立负载参数与扩展方向的基本框架。[DDIA 核心章节](references/2026-11-21-DDIA核心章节.md)

### 2. 建立负载模型和服务目标

- 用负载参数描述系统：请求率、读写比、并发数、数据量、事件率、工作集和突发形态。
- 用吞吐、响应时间分位数、错误率和可用性约束设定服务目标。
- 区分平均延迟与尾部延迟；用百分位和直方图，而不是只看平均值。
- 区分两类核心问题：固定资源下负载增加会怎样，负载固定时增加多少资源能维持目标。
- **文献引用**：DDIA Ch.1 的负载参数、响应时间分布、尾部延迟、SLO/SLA 与两种扩展提问方式；Twitter fan-out 示例用于说明负载不能只用吞吐量概括。[DDIA 核心章节](references/2026-11-21-DDIA核心章节.md)

### 3. 性能曲线：定位瓶颈和可运营区间

- 横轴为资源饱和度或负载，纵轴为一个明确的性能指标。
- 逐段解释 A→B、B→C、C→D、D→E、E→F：空转开销、线性区、饱和趋缓、过载退化、失效。
- 区分纯排队过载与自损型过载；区分吞吐、延迟和错误率各自的变化。
- 可运营点由服务目标决定，通常不是峰值；峰值和容量上限只属于某个指标。
- 用压测曲线定位瓶颈切换和资源组合，而不把所有资源压成一条总轴。
- **文献引用**：DDIA Ch.1 的响应时间分布、队列延迟、head-of-line blocking、尾部延迟放大和固定到达率压测；Cloud Throttling 与 Cloud Queue-Based Load Leveling 的资源上限、队列缓冲和退化场景；模式层 Request Pipeline 与 Request Batch 的在途队列与吞吐利用。[DDIA 核心章节](references/2026-11-21-DDIA核心章节.md)；[Cloud Scalability 模式](references/2026-11-21-Scalability核心模式.md)；[分布式 Scalability 模式](references/2026-11-21-分布式系统Scalability核心模式.md)

### 4. 缩小瓶颈：实例、资源和扩容单元

- 定义狭义资源：算力、内存、存储、I/O、网络带宽和连接数。
- 定义实例：进程、容器、虚拟机或物理机等承载资源并执行工作的个体。
- 定义扩容单元：可以整体复制或提升规格、用于承载工作负载的最小增量。
- 给出问题转换模板：
  - 缺口：哪个指标、在什么负载下不达标？
  - 瓶颈：是哪一种资源或协调约束？
  - 扩容单元：横向复制什么，纵向提升哪类资源？
  - 新瓶颈：加完资源后瓶颈会移到哪里？
  - 成本：单位吞吐、延迟改善和每单位资源的代价是什么？
- **文献引用**：DDIA Ch.1 的 scale up / scale out 与 shared-nothing；模式层的 Competing Consumers、Follower Reads、Pipes and Filters、Fixed/Key-Range Partitions 和 Sharding 用实例、副本、filter、分区证明可复制单元的具体形态。[DDIA 核心章节](references/2026-11-21-DDIA核心章节.md)；[分布式 Scalability 模式](references/2026-11-21-分布式系统Scalability核心模式.md)；[Cloud Scalability 模式](references/2026-11-21-Scalability核心模式.md)
- **术语依据**：`instance / instance type` 用于承载资源并具有不同规格的个体，`scale unit / 扩容单元` 用于描述一起伸缩的组件和扩容增量，`stamp / cell / service unit` 用于成组副本。`node` 更适合基础设施中的节点语境，不适合作为所有形态（进程、容器、虚拟机）的统一术语。[资源单元术语来源](references/2026-11-21-资源单元术语来源.md)

### 5. 四种扩展路径

- 降低单请求成本：减少锁等待、序列化、网络往返和无效工作；抬高单实例天花板。
- 横向复制：复制实例、分区、副本或过滤器阶段，让负载分摊到多个单元。
- 负载整形：批量、管道、队列、削峰、限流和背压，在资源到位前保护系统。
- 改变数据或执行模型：读写分离、物化视图、分区、分片、异步化和优先级。
- 每种手段都必须说明解决了什么瓶颈、引入了什么新瓶颈，以及适用边界。
- **文献引用**：
  - 降低单请求成本：Singular Update Queue、Request Batch、Request Pipeline。
  - 横向复制：Competing Consumers、Follower Reads、Fixed/Key-Range Partitions、Pipes and Filters、Sharding。
  - 负载整形：Queue-Based Load Leveling、Throttling、Competing Consumers。
  - 改变数据和执行模型：CQRS、Pipes and Filters、Sharding。[Cloud Scalability 模式](references/2026-11-21-Scalability核心模式.md)；[分布式 Scalability 模式](references/2026-11-21-分布式系统Scalability核心模式.md)

### 6. 并行扩展的上限

- 区分单机瓶颈、串行比例、竞争、协调和一致性。
- 介绍 Amdahl 类串行上限和通用可扩展性定律中的竞争、一致性成本。
- 从 N=1、2、4 的实测数据判断资源扩展是否接近线性，而不是只观察结论。
- 重点：降低协调范围、减少共享状态和跨节点一致性，通常比继续加机器划算。
- **文献引用**：DDIA Ch.6 的单分区瓶颈、热点和跨分区协调；Cloud CQRS、Sharding、Throttling 与 Competing Consumers 的一致性时延、再平衡、共享资源争用和退化代价；分布式模式中的 Fixed/Key-Range Partitions、Follower Reads、Request Pipeline 与 Singular Update Queue 展示串行、复制和协调如何限制并行收益。[DDIA 核心章节](references/2026-11-21-DDIA核心章节.md)；[Cloud Scalability 模式](references/2026-11-21-Scalability核心模式.md)；[分布式 Scalability 模式](references/2026-11-21-分布式系统Scalability核心模式.md)

### 7. 数据和状态如何规模化

- 复制：读侧横向扩展、复制延迟、跨机房延迟和一致性边界。
- 分区：键范围、哈希、热点、二级索引、再平衡和请求路由。
- 存储与索引：工作集、I/O、内存、写放大、缓存失效和索引维护。
- 跨分区写入、全局索引和分布式事务如何把并行收益转成协调成本。
- 通过固定分区、键范围分区和按负载分裂的模式建立可分割单元。
- **文献引用**：DDIA Ch.6 全章；Cloud CQRS、Competing Consumers、Pipes and Filters、Sharding；分布式模式 Follower Reads、Fixed Partitions、Key-Range Partitions。[DDIA 核心章节](references/2026-11-21-DDIA核心章节.md)；[Cloud Scalability 模式](references/2026-11-21-Scalability核心模式.md)；[分布式 Scalability 模式](references/2026-11-21-分布式系统Scalability核心模式.md)

### 8. 按系统类型分析

- 无状态微服务：实例复制、下游依赖、连接池、缓存和限流。
- 数据库：查询、索引、连接数、锁、复制、分片和热点。
- 队列与消费者：分区数、消费者并发、积压、乱序、幂等和毒消息。
- 管道和流处理：阶段并行、最慢阶段、背压、分区和批量。
- 有开始和结束的 worker job：完成时间、任务粒度、队头阻塞、幂等和重试。
- 实时推送和搜索分析：分别以连接数、内存、扇出、索引和数据局部性为核心瓶颈。
- **文献映射**：
  - 无状态微服务：Follower Reads、CQRS、Competing Consumers；
  - 数据库：DDIA Ch.6、Sharding、CQRS；
  - 队列与消费者：Competing Consumers、Queue-Based Load Leveling、Throttling；
  - 管道和流处理：Pipes and Filters、Request Batch、Request Pipeline；
  - worker job：Singular Update Queue、Pipes and Filters、Queue-Based Load Leveling；
  - 实时推送与搜索：Follower Reads、Fixed/Key-Range Partitions 为主要参考。[DDIA 核心章节](references/2026-11-21-DDIA核心章节.md)；[Cloud Scalability 模式](references/2026-11-21-Scalability核心模式.md)；[分布式 Scalability 模式](references/2026-11-21-分布式系统Scalability核心模式.md)

### 9. 压测与观测

- 压测负载必须保持固定到达率，避免关闭式客户端导致 coordinated omission。
- 每级负载记录吞吐、延迟分位数、错误率和资源曲线；每次运行记录加载速率、保持时间和回归过程。
- 区分容量、峰值吞吐、可运营点和可持续负载。
- 对瓶颈资源和成本指标建立基线，使用相同硬件、数据、负载和服务目标作前后对比。
- 在各章中复用同一个 Golang 服务作为贯穿 Demo，每章只演示一个新变量或一种新瓶颈。
- Demo 从 1 个实例起步，随后逐步加入实例、队列、下游存储、分区或任务并行；每步只引入一个变量。
- 每一步都记录资源化模板：缺口、瓶颈、扩容单元、新瓶颈、成本和验证结果。
- **文献引用**：DDIA Ch.1 的关闭式压测偏斜、tail latency、百分位聚合和线性区到饱和区的观测方法；Throttling 的资源监测与软上限；Request Batch/Request Pipeline 的批量窗口、在途上限和压测权衡。[DDIA 核心章节](references/2026-11-21-DDIA核心章节.md)；[Cloud Scalability 模式](references/2026-11-21-Scalability核心模式.md)；[分布式 Scalability 模式](references/2026-11-21-分布式系统Scalability核心模式.md)

### 10. 做决策：加资源、改代码还是改架构

- 判断资源是否能够改善瓶颈，以及改善能持续到哪个规模。
- 优先选择可逆、可测量、成本低的手段；只有瓶颈证明需要时再引入分片、跨节点一致性或架构重构。
- 当资源扩展有延迟、突发短暂或协调成本过高时，限流和降级作为兜底。
- 最终决策以单位吞吐、单位成本、尾部延迟、可靠性和复杂度共同决定，而不是只看峰值。
- **文献引用**：DDIA Ch.1 的 scale up / scale out 与架构高度依赖负载假设；Throttling 的 autoscaling 缓冲窗口与短突发成本取舍；Cloud 6 模式和分布式 6 模式都用于比较可逆性、新瓶颈与收益边界。[DDIA 核心章节](references/2026-11-21-DDIA核心章节.md)；[Cloud Scalability 模式](references/2026-11-21-Scalability核心模式.md)；[分布式 Scalability 模式](references/2026-11-21-分布式系统Scalability核心模式.md)

### 11. 总结：一套可复用的 Scalability 流程

1. 描述负载和增长形态。
2. 定义可验证的服务目标。
3. 建立性能曲线并定位瓶颈。
4. 判断瓶颈能否资源化。
5. 选择实例、资源或可复制扩容单元。
6. 评估串行、竞争、一致性和协调成本。
7. 压测验证，确认旧瓶颈消失、新瓶颈出现。
8. 以成本和可逆性作最终决策。



## 产出物

- `README.md`：文档目标与范围。
- `docs/`：正文与各章节内容（结构待定）。
- demo 代码与压测脚本：待大纲确定后补充。

## 状态

- [x] 明确文档目标
- [x] 确定目标读者与 demo 技术栈
- [x] 收敛大纲
- [ ] 正文与 demo
