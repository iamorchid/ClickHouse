# ClickHouse 代码结构详解

## 规模概览

| 指标 | 数值 |
|------|------|
| `src/` C++ 源文件数 | ~7,900 |
| `src/` 子目录数 | 31 |
| `contrib/` 第三方库数 | 263 |
| C++ 标准 | C++23 |
| 构建系统 | CMake ≥ 3.25 |
| 编译器要求 | Clang ≥ 21 |

---

## 1. 顶层目录结构

| 目录 | 说明 |
|------|------|
| **`src/`** | 核心源码，包含所有数据库逻辑，约 7,900 个 C++ 文件，分为 31 个子目录 |
| **`programs/`** | 可执行入口点（server、client、local、keeper、benchmark 等）。每个程序是一个薄 `main()` 封装，最终编译为单个 `clickhouse` 多调用二进制（multi-call binary） |
| **`base/`** | 底层基础库：`glibc-compatibility`（glibc 兼容层）、`pcg-random`（随机数）、`poco`（Poco 框架 fork）、`harmful`（有害函数检测）、`widechar_width`、`readpassphrase` |
| **`contrib/`** | 263 个 vendored 第三方库源码 + CMake 胶水文件。每个库有对应的 `*-cmake/` 目录提供 CMakeLists.txt 集成 |
| **`tests/`** | 所有测试：SQL 无状态/有状态测试、集成测试、性能测试、模糊测试、sqllogic、压力测试、Jepsen 测试 |
| **`cmake/`** | CMake 模块：架构检测（`arch.cmake`）、编译器标志（`cxx.cmake`）、sanitizer 配置（`sanitize.cmake`）、CPU 特性（`cpu_features.cmake`）、ccache、clang-tidy、版本生成 |
| **`rust/`** | Rust 组件，如 `delta-kernel-rs`（Delta Lake 内核），通过 corrosion 桥接与 CMake 集成 |
| **`docs/`** | 用户文档（Markdown/HTML） |
| **`docker/`** | 构建和运行 ClickHouse 的 Dockerfile |
| **`ci/`** | CI 脚本和工具链 |
| **`utils/`** | 杂项工具（如 `c++expr` 编译测试工具） |
| **`packages/`** | 打包脚本（deb、rpm 等） |
| **`benchmark/`** | 基准测试框架 |
| **`format_sources/`** | 代码格式化配置 |

---

## 2. `src/` 分层架构

ClickHouse 的源码遵循清晰的分层架构，从底层基础设施到上层查询执行逐层构建：

```
SQL 文本
   │
   ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 0: 基础设施 (Common / Core / IO)                           │
│   线程池、内存分配、Context 全局状态、网络、Block/Column/Field     │
│   ReadBuffer/WriteBuffer 体系、压缩编解码                        │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│ Layer 1: 数据模型 (Columns / DataTypes / AggregateFunctions)     │
│   列实现（Vector、String、Array、Nullable、JSON…）               │
│   类型系统（Int、String、DateTime64、Decimal…）                  │
│   聚合函数（count、sum、uniq、quantile、topK…）                  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│ Layer 2: SQL 编译管线 (Parsers → Analyzer → Planner)             │
│   SQL 词法/语法分析 → AST                                       │
│   AST → QueryTree（语义分析、类型推断、常量折叠）                 │
│   QueryTree → QueryPlan（物理计划、Join 排序、并行副本）          │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│ Layer 3: 执行引擎 (QueryPipeline / Processors)                   │
│   QueryPlan → Processor DAG（push/pull 执行模型）                │
│   PipelineExecutor 多线程执行图                                  │
│   物理步骤：ReadFromMergeTree、AggregatingStep、JoinStep…        │
│   行级变换：FilterTransform、ExpressionTransform、SortTransform  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│ Layer 4: 编排层 (Interpreters)                                   │
│   Context、Aggregator、Join、Set、ExpressionActions              │
│   串联 解析→分析→计划→执行 的完整查询生命周期                    │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│ Layer 5: 存储引擎 (Storages)                                     │
│   MergeTree（主力引擎）：Parts、索引、合并、变异、复制            │
│   Distributed：分布式表                                         │
│   ObjectStorage：S3/Azure/HDFS/数据湖                            │
│   System：100+ 张系统表                                         │
│   外部数据源：Kafka、MySQL、PostgreSQL、MongoDB…                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. `src/` 各目录详细说明

### 3.1 Layer 0：基础设施

#### `Common/`（~561 文件）

共享基础设施，是整个系统的地基。核心组件包括：

- **`Context`**：全局状态容器，管理 Settings、数据库、表、集群、ZooKeeper 连接等。是 ClickHouse 中最核心的单例对象
- **`Settings`**：用户可配置的所有参数定义
- **线程池**：`ThreadPool`、`BackgroundSchedulePool`、`BackgroundMergingMutatingPool` 等
- **内存分配**：`Arena`（内存池）、`Allocator`、`MemoryTracker`（内存用量追踪）
- **网络**：`Connection`、`ConnectionPool`、`TCPHandler` 底层通信
- **哈希/字符串工具**：`CityHash`、`SipHash`、字符串操作函数
- **ZooKeeper 客户端**：`ZooKeeper`、`ZooKeeperRetries`
- **配置文件**：`ConfigProcessor`、`ConfigurationManager`

#### `Core/`

核心数据结构定义：

- **`Block`**：数据块，由多列 `ColumnWithTypeAndName` 组成，是数据流转的基本单位
- **`ColumnWithTypeAndName`**：带类型和名称的列引用
- **`Field`**：单个值的通用容器（类似 `std::variant`）
- **`NamesAndTypes`**：列名和类型的列表
- **`SortDescription`**：排序描述
- **`Protocol`**：客户端-服务端协议定义（数据包类型、版本号）
- **`Settings`**：设置项枚举和类型

#### `IO/`

所有 I/O 原语：

- **`ReadBuffer` / `WriteBuffer`**：读写缓冲区的抽象基类层次结构，所有 I/O 操作的基础
- **压缩读取器**：`CompressedReadBuffer`，支持透明解压
- **`CompressionCodec`**：压缩编解码器（LZ4、ZSTD、Delta、Gorilla、FPC、ALP、T64、DoubleDelta、GCD）
- **异步读取**：`AsyncReadBuffer`、`CachedInMemoryReadBuffer`
- **AIO**：异步文件 I/O
- **对象存储 I/O**：Azure/S3 blob 存储读写
- **归档支持**：tar、zip 等归档格式读取

#### `Compression/`

压缩编解码器实现和 `CompressionFactory`（根据名称创建对应编解码器的工厂）。

#### `Loggers/`

日志子系统，封装 Poco 日志框架。

#### `Daemon/`

守护进程工具：PID 文件管理、信号处理、进程守护。

#### `BridgeHelper/`

ODBC/JDBC 桥接进程的辅助工具。

---

### 3.2 Layer 1：数据模型

#### `Columns/`

列式数据模型的各种列实现。列是 ClickHouse 数据处理的核心单元：

| 类 | 说明 |
|---|------|
| `ColumnVector<T>` | 固定大小数值列（Int8/16/32/64、Float32/64） |
| `ColumnString` | 变长字符串列 |
| `ColumnFixedString` | 固定长度字符串列 |
| `ColumnArray` | 数组列（嵌套列） |
| `ColumnTuple` | 元组列（多个异构子列） |
| `ColumnNullable` | 可为空的列（数据列 + null mask 列） |
| `ColumnDecimal<T>` | 高精度十进制列 |
| `ColumnLowCardinality` | 低基数列（字典编码） |
| `ColumnSparse` | 稀疏列（大量默认值优化） |
| `ColumnDynamic` | 动态类型列 |
| `ColumnObject` | JSON 对象列 |
| `ColumnVariant` | 变体类型列（类似 union） |
| `ColumnAggregateFunction` | 聚合函数状态列 |
| `ColumnCompressed` | 压缩列 |
| `ColumnConst` | 常量列（所有行值相同） |

所有列实现 `IColumn` 接口，提供统一的 `insert`、`get`、`size`、`compareAt`、`permute` 等操作。

#### `DataTypes/`

类型系统，定义 SQL 类型到内部表示的映射：

| 类 | 说明 |
|---|------|
| `DataTypeInt8/16/32/64/128/256` | 整数类型 |
| `DataTypeUInt8/16/32/64/128/256` | 无符号整数 |
| `DataTypeFloat32/64` | 浮点数 |
| `DataTypeString` | 字符串 |
| `DataTypeFixedString` | 定长字符串 |
| `DataTypeDateTime / DateTime64` | 日期时间 |
| `DataTypeDate / Date32` | 日期 |
| `DataTypeDecimal` | 高精度小数 |
| `DataTypeUUID` | UUID |
| `DataTypeArray` | 数组 |
| `DataTypeTuple` | 元组 |
| `DataTypeMap` | 键值映射 |
| `DataTypeNullable` | 可为空 |
| `DataTypeEnum` | 枚举 |
| `DataTypeJSON` | JSON 对象 |
| `DataTypeDynamic` | 动态类型 |
| `DataTypeAggregateFunction` | 聚合函数状态 |
| `DataTypeGeo` | 地理类型（Point、Ring、Polygon、MultiPolygon） |

每个 DataType 负责：序列化/反序列化、文本解析、创建对应的 Column、类型推断。

#### `AggregateFunctions/`

所有聚合函数实现：

- **基础聚合**：`count`、`sum`、`min`、`max`、`avg`、`any`
- **唯一计数**：`uniq`、`uniqExact`、`uniqHLL12`、`uniqCombined`、`uniqCombined64`
- **分位数**：`quantile`、`quantiles`、`quantileExact`、`quantileTDigest`
- **Top-K**：`topK`、`topKWeighted`
- **统计**：`corr`、`covar`、`varSamp`、`stddevSamp`、`entropy`、`skewSamp`、`kurtSamp`
- **数组聚合**：`groupArray`、`groupUniqArray`、`groupArrayInsertAt`
- **条件聚合**：`argMin`、`argMax`
- **组合器**：`-If`（条件）、`-Array`（数组展开）、`-State`（返回中间状态）、`-Merge`（合并状态）、`-Distinct`（去重）

聚合函数框架通过 `IAggregateFunction` 基类 + 组合器模式实现灵活的函数组合。

---

### 3.3 Layer 2：SQL 编译管线

#### `Parsers/`（~382 文件）

SQL 词法分析和语法分析，将 SQL 文本转换为 AST：

- **AST 节点层次**：`IAST` 基类 → `ASTSelectQuery`、`ASTCreateQuery`、`ASTInsertQuery`、`ASTExpressionList`、`ASTFunction`、`ASTIdentifier`、`ASTLiteral` 等
- **解析器**：`ParserSelectQuery`、`ParserQuery`、`ExpressionParser`
- **Token 层**：`Token`、`TokenIterator`、`TokenType`
- **特性**：支持 ClickHouse 全部 SQL 语法扩展（ENGINE、SETTINGS、FORMAT、SAMPLE、ARRAY JOIN、GLOBAL JOIN 等）

#### `Analyzer/`

语义分析，将 AST 转换为类型化的 QueryTree：

- **QueryTree 节点**：`IQueryTreeNode` → `ColumnNode`、`FunctionNode`、`ConstantNode`、`IdentifierNode`、`QueryNode`、`JoinNode`、`ArrayJoinNode`、`TableNode`
- **分析过程**：
  1. 标识符解析（列名 → 表和列的绑定）
  2. 类型推断（表达式结果类型推导）
  3. 常量折叠（编译期计算常量表达式）
  4. 子查询内联
  5. 聚合函数验证
- **Pass 机制**：通过一系列 Pass 逐步变换 QueryTree

#### `Planner/`

逻辑/物理计划生成，将分析后的 QueryTree 转换为可执行的 QueryPlan：

- **QueryPlan**：`IQueryPlanStep` 的树结构，每个 Step 产生一部分 Processor
- **关键组件**：
  - `PlannerAggregation`：聚合策略选择（hash、two-level、external）
  - `PlannerJoin`：Join 顺序和算法选择（hash join、merge join）
  - `PlannerActionsVisitor`：遍历 QueryTree 生成 ActionsDAG
  - 并行副本计划（parallel replicas）
- **优化**：谓词下推、投影裁剪、Join 重排序

---

### 3.4 Layer 3：执行引擎

#### `QueryPipeline/`

流水线构建层：

- **`QueryPipeline`**：有向无环图（DAG）的 Processor 管线
- **`QueryPipelineBuilder`**：构建 Pipeline 的 Builder
- **`Pipe`**：管线中的一段（Source + Transforms）
- **`RemoteQueryExecutor`**：分布式查询远程执行器
- **`DistributedPlanExecutor`**：分布式计划执行
- 执行速度限制、大小限制、Profile 信息收集

#### `Processors/`

核心执行引擎，实现 push/pull 执行模型。这是 ClickHouse 执行层的基石：

##### `IProcessor`
所有 Processor 的基类。每个 Processor 有若干输入端口（`InputPort`）和输出端口（`OutputPort`），通过 `prepare()` + `work()` 接口驱动数据流动。

##### `Executors/`
- **`PipelineExecutor`**：多线程执行器，调度 Processor DAG
- **`ExecutingGraph`**：将 Pipeline 转换为执行图
- **`ExecutionThreadContext`**：线程本地执行上下文
- **`PullingPipelineExecutor`**：拉取模式执行（客户端逐块拉取）
- **`PushingPipelineExecutor`**：推送模式执行（INSERT 场景）
- **`CompletedPipelineExecutor`**：同步等待完成

##### `QueryPlan/`（~181 文件）
物理计划步骤，每个 Step 生成一组 Processor：

| Step | 说明 |
|------|------|
| `ReadFromMergeTree` | 从 MergeTree 读取数据（最核心的 Step） |
| `AggregatingStep` | 聚合（hash 聚合、two-level 聚合） |
| `FilterStep` | 过滤（WHERE/HAVING） |
| `JoinStep` | Join 操作 |
| `SortingStep` | 排序（ORDER BY） |
| `LimitStep` | 限制行数（LIMIT/OFFSET） |
| `ExpressionStep` | 表达式计算（SELECT 列、函数调用） |
| `CreatingSetsStep` | 构建 IN 子查询的 Set |
| `TotalsHavingStep` | TOTALS 和 HAVING 处理 |
| `WindowStep` | 窗口函数 |
| `DistinctStep` | 去重（DISTINCT） |
| `CubeStep` / `RollupStep` | CUBE / ROLLUP |
| `BroadcastExchangeStep` | 广播交换（分布式） |
| `BuildRuntimeFilterStep` | 运行时过滤器构建 |
| `CommonSubplanStep` | 公共子计划优化 |

##### `Transforms/`
行级数据变换 Processor：

- `AggregatingTransform`：聚合变换
- `FilterTransform`：过滤变换
- `ExpressionTransform`：表达式计算
- `SortTransform`：排序
- `ArrayJoinTransform`：ARRAY JOIN 展开
- `DistinctTransform`：去重
- `CheckConstraintsTransform`：约束检查
- `AddingDefaultsTransform`：填充默认值
- `BuildRuntimeFilterTransform`：运行时过滤器
- `ScatterByPartitionTransform`：按分区散列（分布式写入）

##### `Sources/` / `Sinks/`
- **Sources**：数据源 Processor（`SourceFromSingleChunk`、`MySQLSource`、`MongoDBSource`、`ArrowFlightSource`）
- **Sinks**：数据汇 Processor

##### `Formats/`
格式感知的 Processor（读写各种数据格式：CSV、JSON、Parquet、ORC、Arrow 等）。

##### `Merges/`
MergeTree 后台合并相关的 Processor。

##### `TTL/`
TTL（Time To Live）相关 Processor。

---

### 3.5 Layer 4：编排层

#### `Interpreters/`（~551 文件）

**ClickHouse 中最大、最复杂的模块**。编排整个查询生命周期，将解析→分析→计划→执行串联起来：

- **`Context`**：全局/会话状态管理，是系统的"大脑"。管理所有数据库、表、集群、设置、缓存等
- **`Aggregator`**：聚合引擎，实现 hash 聚合、two-level 聚合、外部聚合（溢出到磁盘）
- **`AggregationMethod`**：聚合使用的各种 hash 表策略
- **`Join`**：Join 实现（hash join、merge join、direct join）
- **`Set`**：IN 子查询的集合构建
- **`ExpressionActions`**：表达式动作执行器
- **`ActionsDAG`**：表达式动作的有向无环图
- **`AsynchronousInsertQueue`**：异步插入队列
- **DDL 解释器**：`CreateQuery`、`DropQuery`、`AlterQuery`、`RenameQuery` 等各种 DDL 语句的解释执行
- **`MergeTreeDataSelect`**：MergeTree 查询的入口
- **`Cluster`**：分布式集群拓扑
- **日志系统**：`QueryLog`、`PartLog`、`SystemLog` 等

---

### 3.6 Layer 5：存储引擎

#### `Storages/`

所有表引擎实现，是 ClickHouse 存储层的核心：

##### `MergeTree/`（~320 文件）

**主力存储引擎**，也是 ClickHouse 最复杂的子系统：

| 组件 | 说明 |
|------|------|
| `MergeTreeData` | MergeTree 数据管理：Part 集合、元数据、设置 |
| `MergeTreeDataPart` | 单个数据 Part 的表示 |
| `MergeTreeReader` | Part 数据读取（列式读取、索引过滤） |
| `MergeTreeWriter` | Part 数据写入 |
| `BackgroundJobsAssignee` | 后台任务调度（合并、变异、TTL 清理） |
| `DataPartsExchange` | 副本间数据同步（fetch/send parts） |
| `Compaction/` | 合并策略和实现 |
| `Merger` | Part 合并算法 |
| `StorageMergeTreeAnalyzeIndexes` | 索引分析 |
| Projections | 物化投影 |
| Mutations | 数据变异（ALTER UPDATE/DELETE） |
| `ReadFromMergeTree` | 生成读取 MergeTree 的 QueryPlan |

核心概念：
- **Part**：不可变的数据分片，包含若干列文件 + 索引文件 + 元数据
- **Mark**：稀疏索引标记，实现 O(log N) 数据定位
- **Primary Index**：主键索引（稀疏，存储在内存中）
- **Skip Index**：二级索引（minmax、set、bloom filter、ngrambf）
- **Merge**：后台合并小 Part 为大 Part，实现 LSM 树式的写入放大优化
- **Mutation**：异步数据变异（重写 Part）
- **Replication**：基于 ZooKeeper/Keeper 的副本复制

##### `Distributed/`
分布式表引擎：
- 异步 INSERT 批量发送
- 目录队列管理
- 分片/副本路由
- 分布式查询的本地和远程执行

##### `ObjectStorage/`
对象存储后端：
- `S3/`：Amazon S3 兼容存储
- `Azure/`：Azure Blob 存储
- `HDFS/`：Hadoop HDFS
- `DataLakes/`：数据湖格式（Iceberg、Delta、Hudi）
- `Local/`：本地文件系统

##### `System/`
`system.*` 系统表（100+ 张），提供数据库运行时的可观测性：
- `system.tables`、`system.columns`、`system.databases`
- `system.parts`、`system.parts_columns`
- `system.query_log`、`system.part_log`
- `system.metrics`、`system.asynchronous_metrics`
- `system.replicas`、`system.clusters`
- `system.settings`、`system.users`、`system.roles`

##### 其他引擎

| 引擎 | 说明 |
|------|------|
| `Kafka/` | Kafka 消费引擎 |
| `RabbitMQ/` | RabbitMQ 消费引擎 |
| `NATS/` | NATS 消费引擎 |
| `PostgreSQL/` | PostgreSQL 外部表 |
| `MySQL/` | MySQL 外部表 |
| `RocksDB/` | RocksDB 后端引擎 |
| `ArrowFlight/` | Arrow Flight 协议 |
| `Hive/` | Hive Metastore 集成 |
| `MaterializedView/` | 物化视图引擎 |
| `TimeSeries/` | 时序引擎 |
| `WindowView/` | 窗口视图 |
| `Streaming/` | 流式存储抽象 |
| 顶层文件 | `StorageFile`、`StorageURL`、`StorageMemory`、`StorageLog`、`StorageBuffer`、`StorageNull`、`StorageMerge`、`StorageSet`、`StorageJoin`、`StorageView`、`StorageDictionary`、`StorageGenerateRandom` 等 |

---

### 3.7 Layer 6：数据库 & 字典

#### `Databases/`

数据库引擎：

- **`DatabaseAtomic`**：默认引擎，支持原子 DDL
- **`DatabaseMemory`**：纯内存数据库
- **`DatabaseDictionary`**：基于字典的数据库
- **`DatabaseFilesystem`**：文件系统数据库
- **`DatabaseReplicated`**：复制数据库（分布式 DDL）
- **`DatabaseS3` / `DatabaseHDFS`**：远程存储数据库
- **`DatabaseOverlay`**：叠加数据库
- **`MySQL/` / `PostgreSQL/` / `SQLite/`**：外部数据库引擎
- **`DataLake/`**：数据湖 catalog 支持

#### `Dictionaries/`

字典子系统：
- 内存字典实现
- 外部字典数据源
- 布局实现：`hashed`、`complex_key`、`range`、`ip`、`cache` 等
- 字典生命周期管理和刷新

---

### 3.8 Layer 7：服务端 & 客户端

#### `Server/`

服务端实现：

- **`HTTPHandler` / `HTTPHandlerFactory`**：HTTP 接口处理
- **`TCPHandler`**：原生 TCP 协议处理
- **`GRPCServer`**：gRPC 接口
- **`InterserverIOHTTPHandler`**：节点间通信
- **`CloudPlacementInfo`**：云部署信息
- ACME/TLS 证书管理
- Arrow Flight 服务端

#### `Client/`

客户端实现：

- **`Connection`**：与服务端的连接管理
- **`ConnectionEstablisher`**：连接建立
- **`ConnectionPool`**：连接池
- **`ClientBase`**：客户端基类
- BuzzHouse（模糊测试客户端）
- AI 集成

#### `Access/`

RBAC 和认证：

- `AccessControl`：访问控制管理
- `AccessRights`：权限定义
- `User` / `Role`：用户和角色
- `RowPolicy`：行级安全策略
- `SettingsProfile`：设置配置文件
- `Quota`：配额管理

#### `Backups/`

备份/恢复子系统。

#### `Coordination/`

ClickHouse Keeper：ZooKeeper 兼容的嵌入式协调服务，基于 NuRaft 实现 Raft 共识。

---

### 3.9 Layer 8：函数 & 表函数

#### `Functions/`（~860 文件，最大目录）

所有标量 SQL 函数实现：

| 类别 | 函数示例 |
|------|---------|
| **算术** | `plus`、`minus`、`multiply`、`divide`、`modulo`、`intDiv`、`negate`、`abs` |
| **字符串** | `length`、`substring`、`concat`、`replace`、`lower`、`upper`、`trim`、`regexp`、`like`、`position` |
| **日期时间** | `toYear`、`toMonth`、`toDayOfWeek`、`dateDiff`、`dateAdd`、`now`、`today` |
| **数组** | `arrayJoin`、`arrayMap`、`arrayFilter`、`arrayReduce`、`arraySort`、`has`、`indexOf` |
| **元组/Map** | `tuple`、`tupleElement`、`mapKeys`、`mapValues`、`mapContains` |
| **JSON** | `JSONExtract`、`JSONHas`、`JSONLength`、`visitParam*` |
| **哈希** | `cityHash64`、`sipHash64`、`MD5`、`SHA1`、`SHA256`、`xxHash`、`murmurHash` |
| **编码** | `hex`、`unhex`、`base64Encode`、`base64Decode`、`URL*` |
| **IP** | `IPv4NumToString`、`IPv4StringToNum`、`isIPAddressInRange` |
| **地理** | `h3*`、`s2*`、`geohash*`、`greatCircleDistance` |
| **URL** | `domain`、`path`、`queryString`、`extractURLParameter` |
| **位图** | `bitmapBuild`、`bitmapContains`、`bitmapAnd`、`bitmapOr` |
| **ML/AI** | `aiGenerate`、`aiClassify`、`aiEmbed`、`aiExtract`、`aiTranslate` |
| **向量搜索** | `L2Distance`、`cosineDistance`、`L1Distance` |

函数框架通过 `IFunction` 基类 + `FunctionFactory` 注册表实现，支持向量化执行（一次处理一整列数据）。

#### `TableFunctions/`

表值函数（返回表的函数）：

- `file()`：读取本地文件
- `url()`：通过 URL 读取数据
- `s3()` / `azureBlobStorage()`：云存储
- `hdfs()`：HDFS
- `mysql()` / `postgresql()` / `mongodb()`：外部数据库
- `redis()`：Redis
- `kafka()`：Kafka
- `numbers()`：生成数字序列
- `merge()`：合并多个表
- `dictionary()`：字典作为表
- `executable()`：外部程序
- `arrowFlight()`：Arrow Flight 协议

#### `AggregateFunctions/`

聚合函数（详见 Layer 1 数据模型部分）。

---

## 4. `programs/` 入口点

每个子目录构建一个独立的可执行文件，但最终全部链接为单个 `clickhouse` 多调用二进制（类似 BusyBox）：

| 程序 | 说明 |
|------|------|
| **`server/`** | `clickhouse-server`：主数据库服务端进程 |
| **`client/`** | `clickhouse-client`：交互式 SQL CLI 客户端 |
| **`local/`** | `clickhouse-local`：无需服务端，直接查询本地文件 |
| **`keeper/`** | `clickhouse-keeper`：独立的 ZooKeeper 兼容协调服务 |
| **`benchmark/`** | `clickhouse-benchmark`：查询基准测试工具 |
| **`disks/`** | `clickhouse-disks`：磁盘管理工具 |
| **`compressor/`** | 独立压缩工具 |
| **`format/`** | SQL 格式化器 |
| **`keeper-client/`** | Keeper CLI 客户端 |
| **`keeper-bench/`** | Keeper 基准测试 |
| **`obfuscator/`** | 数据脱敏工具 |
| **`install/`** | 安装辅助工具 |
| **`git-import/`** | Git 日志导入工具 |
| **`main.cpp`** | 顶层 `main()`，根据 argv[0] 分发到对应子程序 |

---

## 5. 构建系统

### CMake 架构

```
CMakeLists.txt (根)
├── cmake/arch.cmake          # 架构检测 (x86_64, aarch64, etc.)
├── cmake/cxx.cmake           # C++ 编译器标志
├── cmake/tools.cmake         # 编译器/链接器/归档器检测
├── cmake/sanitize.cmake      # Sanitizer 配置 (ASan/TSan/UBSan/MSan)
├── cmake/cpu_features.cmake  # CPU 特性检测 (SSE4.2, AVX2, AVX-512)
├── cmake/ccache.cmake        # ccache 配置
├── cmake/clang_tidy.cmake    # clang-tidy 集成
├── cmake/git.cmake           # Git 版本信息
├── cmake/utils.cmake         # 工具函数
├── src/CMakeLists.txt        # 主源码构建
│   ├── src/Common/CMakeLists.txt
│   ├── src/Core/CMakeLists.txt
│   ├── src/IO/CMakeLists.txt
│   ├── ... (每个子目录追加 dbms_sources/dbms_headers)
│   └── src/Functions/CMakeLists.txt
├── programs/CMakeLists.txt   # 可执行文件构建
├── base/CMakeLists.txt       # 基础库构建
└── contrib/CMakeLists.txt    # 第三方库构建 (263 个 add_subdirectory)
```

### 构建特性

- **多目标编译**：AVX2/AVX-512 多版本分发（`multitarget`）
- **Sanitizer 支持**：ASan、TSan、UBSan、MSan
- **PGO**（Profile-Guided Optimization）
- **LTO**（Link-Time Optimization）
- **Split debug symbols**：调试符号分离
- **IWYU**（Include What You Use）
- **clang-tidy** 静态分析

---

## 6. 测试组织

### `tests/` 目录结构

| 目录 | 说明 |
|------|------|
| **`queries/0_stateless/`** | 主力功能测试套件。每个测试是一个 `.sql` 文件 + `.reference` 期望输出文件，通过 `clickhouse-test` 运行。数千个用例 |
| **`queries/0_stateful/`** | 有状态测试（需要预置数据集） |
| **`integration/`** | 集成测试，使用 Docker Compose + pytest。测试与外部系统（Kafka、S3、PostgreSQL、MySQL 等）的交互、集群复制等 |
| **`performance/`** | XML 定义的性能基准测试（`.xml` 文件描述查询和期望性能） |
| **`sqllogic/`** | SQL 逻辑测试（标准 SQL 一致性） |
| **`stress/`** | 压力测试（并发 DDL/DML、随机操作） |
| **`fuzz/`** | 模糊测试（查询模糊、AST 模糊） |
| **`jepsen.clickhouse/`** | Jepsen 分布式正确性测试（线性一致性） |
| **`benchmarks/`** | 基准测试套件 |
| **`config/`** | 测试配置文件 |
| **`clickhouse-test`** | 测试运行器脚本 |

### 单元测试

许多 `src/*/` 目录包含自己的 `tests/` 子目录，存放 C++ 单元测试（基于 Google Test，来自 `contrib/googletest`）。

---

## 7. `contrib/` 第三方库（263 个）

按类别分组的关键库：

| 类别 | 库 |
|------|---|
| **压缩** | lz4, zstd, brotli, bzip2, xz, snappy, zlib-ng, simdcomp |
| **序列化** | arrow, avro, capnproto, flatbuffers, google-protobuf, msgpack-c, nlohmann-json, rapidjson, simdjson, thrift, orc |
| **网络** | curl, grpc, aws-c-*（AWS SDK C 库族）, azure, c-ares, libssh |
| **云存储** | aws (S3), azure (Blob), google-cloud-cpp (GCS), libhdfs3 (HDFS) |
| **数据库** | mariadb-connector-c (MySQL), postgres (libpq), mongo-c-driver, mongo-cxx-driver, rocksdb, sqlite-amalgamation |
| **消息队列** | cppkafka, librdkafka (Kafka), AMQP-CPP (RabbitMQ), nats-io |
| **ML/AI** | ai-sdk-cpp, SimSIMD（向量相似度）, usearch（向量搜索）, FP16, fastops |
| **字符串/文本** | re2（正则）, vectorscan（Hyperscan fork）, StringZilla, libstemmer_c, cld2（语言检测）, wordnet-blast, icu |
| **地理** | h3（Uber H3）, s2geometry（Google S2） |
| **数学/统计** | boost（子集）, croaring（Roaring bitmaps）, datasketches-cpp, pocketfft, miniselect, pdqsort, double-conversion, fast_float |
| **内存** | jemalloc |
| **并发** | abseil-cpp, liburing (io_uring) |
| **编译器** | llvm-project（JIT 编译、LLVM IR）, corrosion（Rust 桥接） |
| **协调** | NuRaft（Raft 共识，用于 Keeper） |
| **测试** | googletest, google-benchmark, libfuzzer-cmake, libprotobuf-mutator |
| **哈希** | cityhash102, xxHash, murmurhash, crc32c, wyhash, SHA3IUF, farmhash |
| **认证/安全** | openssl, jwt-cpp, krb5, openldap, cyrus-sasl, libbcrypt, libcotp |
| **WASM** | wasmedge, wasmtime（WASM UDF） |
| **杂项** | fmtlib, spdlog, magic_enum, replxx（CLI readline）, antlr4, yaml-cpp, libxml2, libarchive, libcpuid, libuv, delta-kernel-rs |

---

## 8. 架构总结

ClickHouse 是一个**分层列式数据库**，架构特点：

1. **列式存储为核心**：`Columns/` + `DataTypes/` 定义列式数据模型，所有处理围绕列展开
2. **向量化执行**：`Functions/` 中每个函数一次处理一整列（`IColumn`），充分利用 SIMD
3. **Push/Pull DAG 执行模型**：`Processors/` 构成 DAG 图，`PipelineExecutor` 多线程调度
4. **LSM 树式存储**：MergeTree 引擎通过 Part 合并实现高效写入，稀疏索引实现快速查询
5. **编译管线分离**：`Parsers → Analyzer → Planner` 三阶段，职责清晰
6. **`Interpreters/` 编排全局**：最复杂的模块，串联所有组件
7. **插件化存储引擎**：`Storages/` 支持多种引擎，通过统一接口集成
8. **丰富的函数生态**：`Functions/` 860 文件，覆盖从基础算术到 AI/向量搜索
9. **深度第三方集成**：263 个 contrib 库，从压缩、序列化到云服务全覆盖
