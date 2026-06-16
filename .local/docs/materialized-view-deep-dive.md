# ClickHouse 物化视图实现原理深度解析

本文基于当前代码库分析普通 `MATERIALIZED VIEW` 的实现原理、工作机制、核心结构、查询加速方式，以及持续写入新数据时物化结果如何与新增数据合并。

需要先明确一个容易误解的点：ClickHouse 普通 `MATERIALIZED VIEW` 不是透明查询改写优化器。它更接近“插入触发器 + 目标表”。用户写入源表时，ClickHouse 把新写入的数据块同步推给依赖的物化视图，执行视图保存的 `SELECT`，再把结果写入目标表。查询加速来自用户直接查询已经预计算好的目标表或物化视图，而不是系统自动把对源表的查询改写成对视图的查询。

## 关键代码位置

| 模块 | 文件 | 作用 |
|---|---|---|
| 物化视图存储对象 | `src/Storages/StorageMaterializedView.h` | 定义 `StorageMaterializedView`，保存目标表、刷新任务、读写接口等 |
| 物化视图读写实现 | `src/Storages/StorageMaterializedView.cpp` | 解析目标表、委托读取目标表、写入目标表、处理内部表、刷新型视图 |
| 插入依赖构建 | `src/Interpreters/InsertDependenciesBuilder.h` | 定义插入源表时如何构造依赖视图 pipeline |
| 插入依赖执行 | `src/Interpreters/InsertDependenciesBuilder.cpp` | 收集依赖视图，执行视图 `SELECT`，写入目标表，处理级联视图 |
| 插入入口 | `src/Interpreters/InterpreterInsertQuery.cpp` | 构造 `INSERT` pipeline，并把物化视图依赖接入写入流程 |
| 依赖图 | `src/Interpreters/DatabaseCatalog.cpp` | 维护源表到依赖视图的映射，例如 `getDependentViews` |
| 创建语义 | `src/Interpreters/InterpreterCreateQuery.cpp` | 处理 `CREATE MATERIALIZED VIEW`、`POPULATE`、回填等 |
| 官方说明 | `docs/en/sql-reference/statements/create/view.md` | 描述物化视图类似插入触发器、`POPULATE` 限制、按 block 转换等 |

## 总体模型

普通物化视图可以拆成三类对象：

1. 源表：用户持续写入的明细表，例如 `events`。
2. 视图对象：`StorageMaterializedView`，保存视图定义、目标表标识、`SELECT` 查询。
3. 目标表：保存物化结果的真实表，可以是显式 `TO` 表，也可以是自动创建的内部表。

结构图：

```text
┌────────────────────┐
│ source table        │
│ events              │
└──────────┬─────────┘
           │ 用户 INSERT 新 block
           ▼
┌─────────────────────────────┐
│ InsertDependenciesBuilder    │
│ 1. 找依赖 events 的视图       │
│ 2. 构造视图 SELECT pipeline   │
│ 3. 构造目标表 sink            │
└──────────┬──────────────────┘
           │ 对新 block 执行视图 SELECT
           ▼
┌─────────────────────────────┐
│ StorageMaterializedView      │
│ mv                           │
│ 保存 SELECT 和 target table   │
└──────────┬──────────────────┘
           │ 写入转换结果
           ▼
┌────────────────────┐
│ target table        │
│ mv_target 或 .inner │
└────────────────────┘
```

如果用户执行：

```sql
CREATE MATERIALIZED VIEW mv TO mv_target AS
SELECT ... FROM events ...;
```

则 `mv` 是视图对象，`mv_target` 是真实落盘目标表。

如果用户执行：

```sql
CREATE MATERIALIZED VIEW mv
ENGINE = SummingMergeTree
ORDER BY key
AS SELECT ... FROM events ...;
```

则 ClickHouse 会为 `mv` 创建内部目标表。代码中 `StorageMaterializedView::generateInnerTableName` 会生成类似 `.inner_id.<uuid>` 或 `.inner.<view_name>` 的内部表名。

## 创建阶段

创建普通 `MATERIALIZED VIEW` 时，核心工作由 `InterpreterCreateQuery` 和 `StorageMaterializedView` 完成。

### 目标表选择

语法上有两种写法：

```sql
CREATE TABLE target
(
    key UInt64,
    value UInt64
)
ENGINE = SummingMergeTree
ORDER BY key;

CREATE MATERIALIZED VIEW mv TO target AS
SELECT key, sum(value) AS value
FROM source
GROUP BY key;
```

这种写法使用显式目标表 `target`。

另一种写法：

```sql
CREATE MATERIALIZED VIEW mv
ENGINE = SummingMergeTree
ORDER BY key
AS
SELECT key, sum(value) AS value
FROM source
GROUP BY key;
```

这种写法没有 `TO`，所以 `StorageMaterializedView` 会把 `has_inner_table` 设为 `true`，并要求创建语句必须包含 `ENGINE`。否则无法确定物化结果保存在哪里。

代码逻辑可以简化为：

```text
CREATE MATERIALIZED VIEW
        │
        ├─ 有 TO target
        │    └─ 目标表 = 用户指定的 target
        │
        └─ 无 TO target
             ├─ 必须有 ENGINE
             └─ 创建内部表 .inner.*
```

### 保存 `SELECT`

`StorageMaterializedView` 构造时会要求创建语句必须有 `SELECT`。之后它通过 `SelectQueryDescription::getSelectQueryFromASTForMatView` 解析并保存查询描述。

简化结构：

```text
StorageMaterializedView
├─ metadata.columns
├─ metadata.select.inner_query
├─ metadata.select.select_table_id
├─ target_table_id
├─ has_inner_table
└─ refresher
```

其中：

- `metadata.select.inner_query` 是物化视图保存的内部 `SELECT`。
- `metadata.select.select_table_id` 是源表标识。
- `target_table_id` 是目标表标识。
- `has_inner_table` 表示是否使用自动内部表。
- `refresher` 只用于 `REFRESH` 型物化视图。

### 建立依赖

物化视图依赖关系由 `DatabaseCatalog` 维护。创建视图后，源表到视图之间会形成依赖边：

```text
events ──depends-to──> sales_daily_mv
```

代码中查询依赖视图时会调用 `DatabaseCatalog::getDependentViews(source_table_id)`。写入 `events` 时，插入 pipeline 就能通过这张依赖图找到所有需要同步推送的物化视图。

### `POPULATE`

如果创建时指定 `POPULATE`：

```sql
CREATE MATERIALIZED VIEW mv
ENGINE = SummingMergeTree
ORDER BY day
POPULATE AS
SELECT toDate(ts) AS day, sum(amount) AS amount
FROM events
GROUP BY day;
```

`InterpreterCreateQuery::fillTableIfNeeded` 会构造一个内部 `INSERT SELECT`，把已有数据回填到物化视图目标表中。

流程：

```text
CREATE MATERIALIZED VIEW ... POPULATE
        │
        ▼
创建视图和目标表
        │
        ▼
构造 INSERT INTO mv SELECT ... FROM events
        │
        ▼
执行回填
```

但 `POPULATE` 不是严格在线一致的回填机制。创建和回填期间并发写入源表的数据可能不会进入物化视图。生产上更稳妥的模式通常是：

```sql
CREATE MATERIALIZED VIEW mv TO target AS
SELECT ... FROM events ...;

INSERT INTO target
SELECT ... FROM events ...;
```

## 写入阶段

假设有如下表和物化视图：

```sql
CREATE TABLE events
(
    ts DateTime,
    user_id UInt64,
    amount UInt64
)
ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE sales_daily
(
    day Date,
    amount UInt64
)
ENGINE = SummingMergeTree
ORDER BY day;

CREATE MATERIALIZED VIEW sales_daily_mv TO sales_daily AS
SELECT
    toDate(ts) AS day,
    sum(amount) AS amount
FROM events
GROUP BY day;
```

用户写入：

```sql
INSERT INTO events VALUES
('2026-06-15 10:00:00', 1, 100),
('2026-06-15 10:01:00', 2, 50),
('2026-06-16 09:00:00', 1, 30);
```

内部流程：

```text
输入 block:
┌─────────────────────┬─────────┬────────┐
│ ts                  │ user_id │ amount │
├─────────────────────┼─────────┼────────┤
│ 2026-06-15 10:00:00 │ 1       │ 100    │
│ 2026-06-15 10:01:00 │ 2       │ 50     │
│ 2026-06-16 09:00:00 │ 1       │ 30     │
└─────────────────────┴─────────┴────────┘
        │
        ├──────────────────────────────┐
        │                              │
        ▼                              ▼
写入源表 events                 推给 sales_daily_mv
                                       │
                                       ▼
                              对本次 block 执行:
                              SELECT toDate(ts), sum(amount)
                              GROUP BY day
                                       │
                                       ▼
                              结果 block:
                              2026-06-15 150
                              2026-06-16 30
                                       │
                                       ▼
                              写入 sales_daily
```

注意：物化视图的 `SELECT` 处理的是“本次插入的数据块”，不是重新扫描整个 `events`。

### `InsertDependenciesBuilder`

写入流程的核心是 `InsertDependenciesBuilder`。构造时它会：

1. 保存初始写入表 `init_table_id`。
2. 读取写入 header。
3. 根据设置判断是否启用去重、是否忽略视图错误、是否 squash 小 block。
4. 调用 `collectAllDependencies` 收集依赖视图。
5. 根据依赖关系构造完整 pipeline。

简化后的 pipeline：

```text
INSERT input
    │
    ▼
createPreSink(root)
    │  补默认值、类型转换、校验目标表结构
    ▼
createSink(root)
    │  写入源表
    ▼
createPostSink(root)
    │
    ├─ CopyTransform 复制 block 给多个依赖视图
    │
    ├─ view A:
    │    ├─ createSelect(view A)
    │    ├─ createSink(view A target)
    │    └─ createPostSink(view A)
    │
    └─ view B:
         ├─ createSelect(view B)
         ├─ createSink(view B target)
         └─ createPostSink(view B)
```

其中：

- `createSelect` 用视图保存的 `SELECT` 对输入 block 做转换。
- `createSink` 把转换结果写入目标表。
- `createPostSink` 继续处理级联物化视图。

### 同步触发而非异步补偿

普通物化视图是在 `INSERT` pipeline 中同步触发的。默认情况下，如果写入某个物化视图失败，整个 `INSERT` 也会失败。设置 `materialized_views_ignore_errors` 后，可以让视图侧错误只记录 warning，但这会带来视图目标表部分写入的风险。

文档中也强调：物化视图在错误场景下不提供确定性行为，已经写入目标表的 block 会保留，错误之后的 block 不会继续写入该视图。

## 读取阶段

查询物化视图时，`StorageMaterializedView::readImpl` 实际会读取目标表。

流程：

```text
SELECT ... FROM sales_daily_mv
        │
        ▼
StorageMaterializedView::readImpl
        │
        ├─ 获取 target table: sales_daily
        ├─ 加共享锁
        ├─ 检查权限
        ├─ 构造 target storage snapshot
        └─ 调用 target->read
```

所以查询：

```sql
SELECT day, sum(amount)
FROM sales_daily_mv
GROUP BY day;
```

本质上是读 `sales_daily`。

`StorageMaterializedView::write` 也是类似逻辑：直接把写入委托给目标表。代码注释里也体现了一个原则：数据不会写进 `StorageMaterializedView` 自身，而是写进它的目标表。

## 查询加速机制

物化视图加速查询的本质是预计算。

没有物化视图时：

```sql
SELECT
    toDate(ts) AS day,
    sum(amount) AS amount
FROM events
GROUP BY day
ORDER BY day;
```

这类查询需要扫描明细表 `events`。如果 `events` 有几十亿行，查询要做大量读取、表达式计算和聚合。

有物化视图后：

```sql
SELECT
    day,
    sum(amount) AS amount
FROM sales_daily
GROUP BY day
ORDER BY day;
```

查询只扫描日级预聚合表 `sales_daily`。

对比：

```text
直接查源表:
events 明细行
    │  扫描大量行
    │  执行 toDate
    │  执行 GROUP BY
    ▼
结果

查物化结果:
sales_daily 预聚合行
    │  扫描少量行
    │  必要时再做轻量 GROUP BY
    ▼
结果
```

加速来源包括：

1. 行数减少：明细行变成按天、按小时、按用户等维度聚合后的行。
2. 计算前移：表达式、过滤、聚合在写入时完成。
3. 排序键更合适：目标表可以针对查询模式设计 `ORDER BY`。
4. 引擎合并：`SummingMergeTree`、`AggregatingMergeTree` 在后台 merge 中进一步压缩结果。
5. IO 降低：查询读取的数据量更小。

需要注意：ClickHouse 普通 `MATERIALIZED VIEW` 不会自动把：

```sql
SELECT toDate(ts), sum(amount)
FROM events
GROUP BY toDate(ts);
```

透明改写成：

```sql
SELECT day, sum(amount)
FROM sales_daily
GROUP BY day;
```

用户或应用需要显式查询 `sales_daily` 或 `sales_daily_mv`。如果目标是自动查询改写，需要分析 `PROJECTION` 等其他机制。

## 新数据和已有物化结果如何合并

这是理解 ClickHouse 物化视图最重要的部分。

普通物化视图只负责把“新写入 block 的转换结果”追加到目标表。真正的合并语义由目标表引擎和查询语句共同完成。

### 使用普通 `MergeTree`

如果目标表是：

```sql
CREATE TABLE sales_daily
(
    day Date,
    amount UInt64
)
ENGINE = MergeTree
ORDER BY day;
```

两次写入源表：

```text
第一次 INSERT 触发 MV:
2026-06-15 150

第二次 INSERT 触发 MV:
2026-06-15 70
```

目标表会保存两行：

```text
sales_daily:
┌────────────┬────────┐
│ day        │ amount │
├────────────┼────────┤
│ 2026-06-15 │ 150    │
│ 2026-06-15 │ 70     │
└────────────┴────────┘
```

查询时必须再聚合：

```sql
SELECT day, sum(amount) AS amount
FROM sales_daily
GROUP BY day;
```

结果：

```text
2026-06-15 220
```

在这种情况下，目标表只是保存增量结果，不会自动把相同 `day` 的行合并成一行。

### 使用 `SummingMergeTree`

如果目标表是：

```sql
CREATE TABLE sales_daily
(
    day Date,
    amount UInt64
)
ENGINE = SummingMergeTree
ORDER BY day;
```

新数据仍然是追加写入：

```text
part_1:
2026-06-15 150

part_2:
2026-06-15 70
```

后台 merge 时，`SummingMergeTree` 会把相同排序键的数值列求和：

```text
merged part:
2026-06-15 220
```

完整过程：

```text
已有目标表 parts
        ▲
        │
新 INSERT 到 events
        │
        ▼
MV 对新 block 做 GROUP BY
        │
        ▼
生成增量聚合行
        │
        ▼
写入 sales_daily 新 part
        │
        ▼
后台 merge 将相同 ORDER BY key 的数值列求和
```

但后台 merge 是异步的，并且不同 part 之间可能还没有完全合并。所以即使使用 `SummingMergeTree`，查询仍建议保留：

```sql
SELECT day, sum(amount) AS amount
FROM sales_daily
GROUP BY day;
```

这样无论后台 merge 是否完成，查询结果都正确。

### 使用 `AggregatingMergeTree`

`AggregatingMergeTree` 更适合保存聚合状态，比如 `uniq`、`avg`、`quantile` 等。

示例：

```sql
CREATE TABLE user_daily
(
    day Date,
    users AggregateFunction(uniq, UInt64),
    amount AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY day;

CREATE MATERIALIZED VIEW user_daily_mv TO user_daily AS
SELECT
    toDate(ts) AS day,
    uniqState(user_id) AS users,
    sumState(amount) AS amount
FROM events
GROUP BY day;
```

写入第一批数据：

```text
2026-06-15 user_id = 1 amount = 100
2026-06-15 user_id = 2 amount = 50
```

物化视图写入目标表的是状态：

```text
day = 2026-06-15
users = uniqState({1, 2})
amount = sumState(150)
```

写入第二批数据：

```text
2026-06-15 user_id = 2 amount = 20
2026-06-15 user_id = 3 amount = 50
```

目标表新增状态：

```text
day = 2026-06-15
users = uniqState({2, 3})
amount = sumState(70)
```

查询时使用 `-Merge` 后缀：

```sql
SELECT
    day,
    uniqMerge(users) AS users,
    sumMerge(amount) AS amount
FROM user_daily
GROUP BY day;
```

结果：

```text
┌────────────┬───────┬────────┐
│ day        │ users │ amount │
├────────────┼───────┼────────┤
│ 2026-06-15 │ 3     │ 220    │
└────────────┴───────┴────────┘
```

状态合并图：

```text
batch_1 state:
uniqState({1,2}) + sumState(150)
        │
        ├─────────────┐
        │             ▼
        │       AggregatingMergeTree
        │             ▲
        └─────────────┤
batch_2 state:        │
uniqState({2,3}) + sumState(70)
        │
        ▼
query:
uniqMerge -> {1,2,3}
sumMerge  -> 220
```

`AggregatingMergeTree` 在后台合并 part 时也会合并相同排序键的聚合状态。但和 `SummingMergeTree` 一样，查询仍应该用 `GROUP BY` 和 `uniqMerge`、`sumMerge` 这类函数保证结果正确。

## 按 block 聚合的影响

物化视图里的聚合只针对当前插入 block 生效。

例如：

```sql
CREATE MATERIALIZED VIEW mv TO target AS
SELECT key, sum(value) AS value
FROM source
GROUP BY key;
```

如果一次 `INSERT` 输入：

```text
key=1 value=10
key=1 value=20
```

视图输出：

```text
key=1 value=30
```

如果两次 `INSERT`：

```text
第一次: key=1 value=10
第二次: key=1 value=20
```

视图会向目标表写入两次：

```text
key=1 value=10
key=1 value=20
```

最终是否变成一行 `key=1 value=30`，取决于目标表引擎和查询：

- `MergeTree`：不会自动语义合并，查询必须 `sum`。
- `SummingMergeTree`：后台 merge 可求和，但查询仍建议 `sum`。
- `AggregatingMergeTree`：保存状态，查询用 `-Merge` 聚合函数。

## 级联物化视图

ClickHouse 支持级联物化视图。

示例：

```sql
CREATE TABLE events
(
    ts DateTime,
    amount UInt64
)
ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE sales_minute
(
    minute DateTime,
    amount UInt64
)
ENGINE = SummingMergeTree
ORDER BY minute;

CREATE MATERIALIZED VIEW sales_minute_mv TO sales_minute AS
SELECT
    toStartOfMinute(ts) AS minute,
    sum(amount) AS amount
FROM events
GROUP BY minute;

CREATE TABLE sales_hour
(
    hour DateTime,
    amount UInt64
)
ENGINE = SummingMergeTree
ORDER BY hour;

CREATE MATERIALIZED VIEW sales_hour_mv TO sales_hour AS
SELECT
    toStartOfHour(minute) AS hour,
    sum(amount) AS amount
FROM sales_minute
GROUP BY hour;
```

写入 `events` 后：

```text
events
  │ INSERT 新 block
  ▼
sales_minute_mv
  │ 输出分钟级增量
  ▼
sales_minute
  │ sales_minute 本身也有依赖视图
  ▼
sales_hour_mv
  │ 输出小时级增量
  ▼
sales_hour
```

代码中 `InsertDependenciesBuilder::createPostSink` 会在写入一个目标表后继续查找依赖它的下游视图，并为每个下游视图构造 `createSelect`、`createSink`、`createPostSink`。如果依赖路径形成环，`DependencyPath::pushBack` 会检测到已经访问过的表并抛出异常。

## 多个物化视图依赖同一源表

一个源表可以有多个物化视图：

```text
events
  ├─ sales_daily_mv  -> sales_daily
  ├─ users_daily_mv  -> users_daily
  └─ errors_hour_mv  -> errors_hour
```

写入 `events` 时，`CopyTransform` 会把同一个输入 block 复制给多个视图分支：

```text
INSERT block
    │
    ▼
write events
    │
    ▼
CopyTransform
    ├─ branch 1: sales_daily_mv SELECT -> sales_daily
    ├─ branch 2: users_daily_mv SELECT -> users_daily
    └─ branch 3: errors_hour_mv SELECT -> errors_hour
```

设置 `parallel_view_processing` 和目标表是否支持并行写入，会影响这些分支是否可以并行处理。

## 去重和重试

物化视图位于 `INSERT` pipeline 中，失败场景需要谨慎理解。

默认行为：

- 如果某个物化视图写入失败，`INSERT` 会失败。
- 源表是否已经写入成功，取决于 pipeline 执行时序。
- 某些 block 可能已经写入目标表，后续 block 未写入。

相关设置：

- `insert_deduplicate`
- `deduplicate_blocks_in_dependent_materialized_views`
- `materialized_views_ignore_errors`

`InsertDependenciesBuilder` 会根据设置决定是否对子物化视图启用 block 去重。为了接近 exactly-once 语义，失败后重试通常需要开启源表和依赖物化视图的插入去重。

如果设置：

```sql
SET materialized_views_ignore_errors = true;
```

则视图错误会被记录为 warning，外层 `INSERT` 可以成功返回。但失败视图的目标表可能只收到部分数据，下游级联视图也只会看到已经成功写入的部分。

## `REFRESH` 型物化视图

当前代码库还支持 `REFRESH` 型物化视图，它和普通插入触发型物化视图不同。

普通物化视图：

```text
源表 INSERT
    │
    ▼
同步触发视图 SELECT
    │
    ▼
追加写入目标表
```

`REFRESH` 型物化视图：

```text
后台调度
    │
    ▼
周期性执行完整 SELECT
    │
    ├─ APPEND: 追加写入目标表
    │
    └─ 非 APPEND:
         1. 创建临时目标表
         2. INSERT SELECT 写入临时表
         3. exchangeTargetTable 原子替换目标表
```

相关实现位于 `StorageMaterializedView::prepareRefresh` 和 `StorageMaterializedView::exchangeTargetTable`。

所以，当讨论“新数据不断写入后如何合并”时，需要区分：

- 普通 `MATERIALIZED VIEW`：每次源表 `INSERT` 产生增量，追加到目标表，由目标表引擎和查询合并。
- `REFRESH ... APPEND`：每次刷新结果追加到目标表。
- `REFRESH` 非 `APPEND`：每次刷新重算目标表，再原子交换替换。

## 完整示例：从明细到日级聚合

### 建表

```sql
CREATE TABLE events
(
    ts DateTime,
    user_id UInt64,
    amount UInt64
)
ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE sales_daily
(
    day Date,
    amount UInt64
)
ENGINE = SummingMergeTree
ORDER BY day;

CREATE MATERIALIZED VIEW sales_daily_mv TO sales_daily AS
SELECT
    toDate(ts) AS day,
    sum(amount) AS amount
FROM events
GROUP BY day;
```

### 第一次写入

```sql
INSERT INTO events VALUES
('2026-06-15 10:00:00', 1, 100),
('2026-06-15 10:01:00', 2, 50);
```

视图处理新 block：

```text
2026-06-15 150
```

目标表可能为：

```text
sales_daily:
2026-06-15 150
```

### 第二次写入

```sql
INSERT INTO events VALUES
('2026-06-15 11:00:00', 3, 70);
```

视图处理新 block：

```text
2026-06-15 70
```

后台 merge 前目标表可能为：

```text
sales_daily:
2026-06-15 150
2026-06-15 70
```

后台 merge 后某个 part 内可能为：

```text
sales_daily:
2026-06-15 220
```

### 正确查询方式

```sql
SELECT
    day,
    sum(amount) AS amount
FROM sales_daily
GROUP BY day
ORDER BY day;
```

结果稳定为：

```text
2026-06-15 220
```

也可以查询视图：

```sql
SELECT
    day,
    sum(amount) AS amount
FROM sales_daily_mv
GROUP BY day
ORDER BY day;
```

`StorageMaterializedView::readImpl` 会委托读取 `sales_daily`。

## 最佳实践

1. 用显式 `TO` 表保存物化结果，便于单独管理目标表结构、分区、排序键和权限。
2. 目标表排序键应匹配常用查询维度，例如按天查询就把 `day` 放入 `ORDER BY`。
3. 对可加指标使用 `SummingMergeTree`，但查询仍保留 `sum` 和 `GROUP BY`。
4. 对复杂聚合使用 `AggregatingMergeTree`，写入 `sumState`、`uniqState`，查询 `sumMerge`、`uniqMerge`。
5. 不要假设物化视图会自动修正源表历史数据的 `UPDATE`、`DELETE`、`DROP PARTITION`。普通物化视图只响应源表新 `INSERT`。
6. 大表回填优先使用单独的 `INSERT SELECT`，谨慎使用 `POPULATE`。
7. 对 exactly-once 要求高的场景，关注 `insert_deduplicate` 和 `deduplicate_blocks_in_dependent_materialized_views`。
8. 物化视图里的 `SELECT` 应显式给目标列起别名，因为 ClickHouse 按列名而不是列顺序插入目标表。

## 总结

ClickHouse 普通 `MATERIALIZED VIEW` 的实现可以概括为：

```text
源表 INSERT 新 block
        │
        ▼
依赖图找到相关物化视图
        │
        ▼
每个视图对新 block 执行保存的 SELECT
        │
        ▼
结果写入目标表
        │
        ▼
查询目标表或视图获得预计算结果
```

它的查询加速来自预计算和更适合查询的目标表布局。新写入数据不会和历史明细表重新全量计算，而是生成增量结果追加到目标表。后续的语义合并由目标表引擎和查询共同完成：`MergeTree` 需要查询时聚合，`SummingMergeTree` 可以后台求和但查询仍建议聚合，`AggregatingMergeTree` 保存聚合状态并通过 `-Merge` 聚合函数得到最终结果。
