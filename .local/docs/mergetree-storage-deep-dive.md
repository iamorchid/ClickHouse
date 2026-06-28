# MergeTree 存储引擎深度解析

## 0. 存储心智模型

`MergeTree` 的存储不是“一个表一个大文件”，而是一个不断追加、后台合并的有序 `Part` 集合。一次 `INSERT` 通常先生成一个或多个小 `Part`，后台任务再把相邻且同分区的 `Part` 合成更大的 `Part`。查询时，执行器不会先把这些 `Part` 物化成一个整体，而是逐层剪枝：分区、`Part`、mark range、列子流、压缩块。

```
Table
  │
  ├── Partition 202401
  │     │
  │     ├── Part 202401_1_1_0       ← 一次插入产生的 0 级 Part
  │     ├── Part 202401_2_5_2       ← 合并 2..5 后产生的 2 级 Part
  │     └── Part 202401_6_9_2
  │
  └── Partition 202402
        └── Part 202402_10_10_0
```

单个 `Part` 是查询和合并的基本物理单元。它内部又分成三类内容：

```
Part
  │
  ├── 元数据
  │     ├── columns.txt
  │     ├── checksums.txt
  │     ├── serialization.json
  │     └── count.txt
  │
  ├── 数据流
  │     ├── Wide:    <column-or-substream>.bin + <column-or-substream>.mrk2
  │     └── Compact: data.bin + data.mrk3/data.mrk4
  │
  └── 索引
        ├── primary.idx
        ├── minmax_*.idx
        └── skp_idx_<name>.idx2 + skp_idx_<name>.mrk2
```

最关键的边界有三个：

- `Part` 边界：合并、复制、校验、移动、删除都以 `Part` 为单位。
- granule 边界：稀疏主键索引和 marks 的基本行粒度。默认可理解为约 `index_granularity` 行，但自适应粒度会根据未压缩字节数缩小 granule。
- 压缩块边界：`.bin` 文件里的实际 I/O 解压单位。mark 指向压缩块和块内偏移，granule 不要求和压缩块一一对应。

## 1. Data Part 结构

### 概述

MergeTree 表的数据以 **Data Part（数据分片）** 的形式存储，每个 Part 是一个独立的目录。`IMergeTreeDataPart` 是核心抽象类，有两个具体子类：

- **`MergeTreeDataPartWide`** — 每列一个或多个文件（大 Part 使用）
- **`MergeTreeDataPartCompact`** — 所有列交错存储在单个 `data.bin` 文件中（小 Part 使用）

选择 Wide 还是 Compact 由 `min_bytes_for_wide_part` 和 `min_rows_for_wide_part` 设置控制。小 Part 先以 Compact 格式创建，超过阈值后在合并时转为 Wide。

`Part` 名称本身也编码了生命周期信息。以 `202401_12_18_3_25` 为例，`202401` 是分区 ID，`12..18` 是覆盖的 block number 范围，`3` 是合并层级，`25` 是可选 mutation 版本。读取时只需要 `Active` 的 `Part`；合并完成后，新 `Part` 进入 `Active`，被覆盖的旧 `Part` 变成 `Outdated`，等没有查询引用后再删除。

```
INSERT block 12 ─┐
INSERT block 13 ─┼── merge ──> 202401_12_18_3
...              │              │
INSERT block 18 ─┘              ├── 行按 sorting key 有序
                                ├── primary.idx 覆盖所有 mark
                                └── checksums.txt 固化文件集合
```

### Part 目录中的文件

一个典型的 Wide 格式 Part 目录（如 `all_1_5_2/`）包含以下文件：

| 文件 | 说明 |
|------|------|
| `checksums.txt` | 所有文件的校验和（CityHash128），包括压缩和未压缩的大小/哈希。是 Part 身份验证的权威文件 |
| `columns.txt` | 列名和类型定义 |
| `columns_substreams.txt` | 每列的序列化子流顺序 |
| `count.txt` | 行数（二进制格式） |
| `primary.idx` | **稀疏主键索引** — 每个 mark 一行 PK 值（可选压缩为 `.cidx`） |
| `minmax_*.idx` | 分区列的 MinMax 索引，用于分区裁剪 |
| `<column_name>.bin` | 压缩后的列数据 |
| `<column_name>.mrk2` | **Marks** — 偏移量对，将 mark 编号映射到 `.bin` 文件中的位置 |
| `skp_idx_<name>.idx2` | 二级索引（skip index）数据 |
| `skp_idx_<name>.mrk2` | 二级索引的 marks |
| `serialization.json` | 序列化方式信息（如稀疏序列化、LowCardinality 统计） |
| `default_compression_codec.txt` | 此 Part 使用的默认压缩编解码器 |
| `metadata_version.txt` | Schema 版本号 |
| `uuid.txt` | Part 唯一标识符 |
| `ttl.txt` | TTL 信息（每列的 min/max TTL、表 TTL、行 WHERE TTL 等） |
| `version.txt` | 事务元数据（MVCC 支持） |
| 统计文件 | 每列统计信息（min、max、ndv、直方图） |

**Compact 格式**：没有每列独立的 `.bin` 和 `.mrk2` 文件，而是一个 `data.bin` 和一个 `data.mrk3`。

### Wide 与 Compact 的物理差异

Wide 格式把每个列子流放到独立文件中。它的优点是列裁剪非常直接，读取 `SELECT a, b` 时一般不需要打开 `c.bin`。复杂类型会拆成多个子流，子流也各自有 marks。

```
Wide Part
  ├── a.bin          ├── a.mrk2
  ├── b.bin          ├── b.mrk2
  ├── arr.size0.bin  ├── arr.size0.mrk2   ← Array 的 offsets 子流
  ├── arr.bin        ├── arr.mrk2         ← Array 的元素子流
  └── primary.idx

mark 17
  ├── a.mrk2   -> a.bin   的压缩块偏移 + 解压块内偏移
  ├── b.mrk2   -> b.bin   的压缩块偏移 + 解压块内偏移
  └── arr.mrk2 -> arr.bin 的压缩块偏移 + 解压块内偏移
```

Compact 格式把所有列的 granule 顺序写入一个 `data.bin`。它减少小 `Part` 的文件数量和打开文件成本，但读取少数列时仍要在同一个文件里跨过其他列的数据。`data.mrk3` 或 `data.mrk4` 中的一个 mark 保存该 granule 内每个列或子流的位置。

```
Compact Part
  ├── data.bin
  ├── data.mrk3
  └── primary.idx

data.bin
  ┌──────────── Granule 0 ────────────┬──────────── Granule 1 ────────────┐
  │ a rows │ b rows │ arr offsets/data │ a rows │ b rows │ arr offsets/data │
  └───────────────────────────────────┴───────────────────────────────────┘

data.mrk3
  mark 0: [a position][b position][arr position][rows in granule]
  mark 1: [a position][b position][arr position][rows in granule]
```

因此，`MergeTreeDataPartWide` 更适合较大的、查询列裁剪收益明显的 `Part`；`MergeTreeDataPartCompact` 更适合刚插入的小 `Part`，避免大量小文件把元数据和文件系统开销放大。

### 校验和结构

`MergeTreeDataPartChecksums` 存储 `std::map<String, Checksum>`，每个 `Checksum` 包含：
- `file_size` + `file_hash`：压缩文件的 CityHash128
- `uncompressed_size` + `uncompressed_hash`：未压缩数据的大小和哈希

ZooKeeper 中使用轻量级 `MinimalisticDataPartChecksums` 以节省内存。

校验和覆盖的是 `Part` 目录内的文件集合，而不是 SQL 层的行语义。写入、fetch、attach、合并收尾时，`checksums.txt` 是判断 `Part` 文件是否完整和一致的权威依据。复制场景下，下载方可以边接收文件边计算校验，最后用源副本发送的校验和确认内容没有损坏。

---

## 2. 列存储格式

### Wide 格式

每列存储在独立的文件中。简单类型使用单个 `<column>.bin` 文件。复杂类型（Array、Tuple、Nested）可能产生多个子流文件：

- `Array(UInt64)` 产生 `arr.size0.bin`（offsets）和 `arr.bin`（元素数据）
- 每个子流有独立的 mark 文件
- 长列名会被 SipHash 短名替换

常见复杂类型的子流大致如下：

| 类型 | 典型子流 | 作用 |
|------|----------|------|
| `Array(T)` | `size0` + nested data | `size0` 存 offsets，nested data 存连续元素 |
| `Nullable(T)` | `null` + nested data | `null` 存空值位图，nested data 存非空载荷 |
| `LowCardinality(T)` | dictionary + indexes | dictionary 存字典值，indexes 存每行字典编号 |
| `Map(K, V)` | offsets + keys + values | 本质类似 `Array(Tuple(K, V))` |
| `Tuple(...)` | 每个元素一个子流树 | 元素类型继续递归序列化 |

可以把“列”理解为 SQL 语义上的字段，把“子流”理解为物理读写单元。`columns.txt` 描述 SQL 列，`columns_substreams.txt` 描述列到子流的展开顺序，`serialization.json` 描述使用哪种序列化策略。

```
SQL column: arr Array(Nullable(UInt64))

物理子流树
  arr.size0          ← 每行数组结束 offset
  arr.null           ← 所有数组元素的 null 位图
  arr                ← 所有非空/默认载荷的 UInt64 值

Wide 文件
  arr.size0.bin + arr.size0.mrk2
  arr.null.bin  + arr.null.mrk2
  arr.bin       + arr.mrk2
```

每列的写入管线：

```
数据 → CompressedWriteBuffer（压缩）→ HashingWriteBuffer → WriteBufferFromFileBase（磁盘）
```

`MergeTreeDataPartWriterWide` 管理：
- `ColumnStreams` — 每子流一个 `MergeTreeWriterStream`
- `SerializationStates` — 每列的批量序列化状态
- `last_non_written_marks` — 跨块边界的未完成 mark
- `rows_written_in_last_mark` — 处理输入块小于粒度的情况

### Compact 格式

所有列交错存储在单个 `data.bin` 中。数据按 **granule（颗粒）** 组织：每个 granule 内，各列按顺序序列化（列 0、列 1、...、列 N）。

`MergeTreeDataPartWriterCompact` 使用：
- `ColumnsBuffer` — 累积行直到一个完整 granule
- `streams_by_codec` — 按压缩编解码器分组的多个压缩流，共享一个输出文件
- 单个 `data.mrk3` 文件，每个 mark 条目包含所有列的偏移量 + 行数

### 序列化

列使用 `ISerialization::serializeBinaryBulkWithMultipleStreams` 写入，对应的反序列化方法读取。每种数据类型提供自己的序列化方式（如 `UInt64` 是 8 字节小端，`String` 是长度前缀等）。`serialization.json` 记录选择的序列化类型。

序列化层只负责把列的内存表示拆成一组顺序字节流；它并不知道查询谓词，也不决定哪些 mark 被读取。mark 写入发生在 `MergeTreeDataPartWriter` 层：每写到一个 granule 边界，就记录当前每个子流在压缩文件中的位置。读取时，`MergeTreeReader` 按同一套子流路径恢复列。

```
Block
  │
  ├── Column a ── SerializationUInt64 ──> a.bin
  ├── Column s ── SerializationString ──> s.bin
  └── Column arr ─┬─ offsets substream ─> arr.size0.bin
                  └─ data substream ───> arr.bin
```

---

## 3. 索引系统

### 3.1 主键索引（稀疏索引）

**构建**：写入时，`MergeTreeDataPartWriterOnDisk::calculateAndSerializePrimaryIndex` 每 `index_granularity` 行写入一行主键值。

**磁盘格式**：`primary.idx` 是二进制序列化的元组序列。对于 `marks_count` 个 mark 和 `key_size` 个 PK 列：

```
[row0_col0][row0_col1]...[row0_colN]    ← mark 0
[row1_col0][row1_col1]...[row1_colN]    ← mark 1
...
[rowM_col0][rowM_col1]...[rowM_colN]    ← mark M
```

每行使用列类型的 `serializeBinary` 序列化。可选择压缩（`.cidx` 扩展名）。

**内存中**：主键索引加载为 `IndexPtr = shared_ptr<const Columns>` — 每 PK 列一个列向量，每个包含 `marks_count` 个值。索引很小：通常每 8192 行一个值。通过 `PrimaryIndexCache` 支持懒加载。

**查询使用**（`KeyCondition`）：将 WHERE 子句转换为逆波兰表达式（RPN），对每个 mark 范围调用 `checkInRange`，使用 `checkInHyperrectangle` 计算 `BoolMask`（can_be_true, can_be_false），确定哪些 mark 范围可能包含匹配数据。通过二分查找或顺序扫描找到最小匹配范围集合。

主键索引是“每个 granule 的第一行主键值”，不是 B-tree，也不指向单行。它只能把候选范围缩到 mark range；真正的行级过滤仍由表达式执行器在读出数据后完成。

```
Part 内行顺序按 sorting key 排列

rows:      0 ........ 8191 | 8192 ...... 16383 | 16384 ..... 24575
granule:  G0               | G1                | G2
mark:     M0               | M1                | M2
PK index: PK(row 0)        | PK(row 8192)      | PK(row 16384)

WHERE key BETWEEN 100 AND 200
  │
  ├── `KeyCondition` 在 `primary.idx` 上判断哪些 granule 可能相交
  └── 结果是 `[from_mark, to_mark)`，例如 `[17, 23)`
```

由于索引是稀疏的，`ORDER BY (tenant_id, ts)` 对 `tenant_id = 42 AND ts >= ...` 很有效，但对不在排序键前缀上的条件帮助有限。二级索引可以继续剪枝，但也只回答“这个 granule 是否可能匹配”，不能保证命中。

### 3.2 二级索引（Skip Index）

二级索引更准确地说是 **data skipping index**：它不保存“值到行号”的精确映射，而是给一段连续数据保存一个摘要。查询时，如果摘要能证明这一段数据不可能满足条件，就整段跳过；如果不能证明，就继续读取真实列数据再做行级过滤。

因此它的核心语义是：

```
索引回答的问题：
  这个 index granule 是否可能包含满足条件的行？

可能的回答：
  false  → 一定不匹配，可以跳过对应 mark range
  true   → 可能匹配，必须读取数据验证
```

`true` 不表示一定命中，尤其是 `bloom_filter` 可能有 false positive，`set` 超过容量后也会退化成“无法判断”。但 `false` 必须可靠，不能跳过真实匹配的数据。

#### 存储层次

二级索引存储在 `skp_idx_<name>.idx2` 文件中，带有 `skp_idx_<name>.mrk2` marks。每个 skip index 有自己的 `GRANULARITY`，含义是“一个索引 granule 覆盖多少个 MergeTree mark”。

如果表的 `index_granularity = 8192`，某个 skip index 定义为 `GRANULARITY 4`，则一个 skip index granule 通常覆盖大约 `8192 * 4 = 32768` 行。

```
主数据 marks:
  M0      M1      M2      M3      M4      M5      M6      M7
  |-------|-------|-------|-------|-------|-------|-------|

skip index GRANULARITY 4:
  I0 covers marks [M0, M4)          I1 covers marks [M4, M8)
  |--------------------------------| |--------------------------------|

磁盘文件:
  skp_idx_user_set.idx2
    I0 的摘要
    I1 的摘要
    ...

  skp_idx_user_set.mrk2
    I0 在 .idx2 中的位置
    I1 在 .idx2 中的位置
    ...
```

`.mrk2` 只定位索引摘要在 `.idx2` 里的位置；真正被跳过的是对应的数据 marks。也就是说，二级索引本身不会定位到行，它把候选 mark range 从“大范围”缩成“小范围”。

#### 写入时如何构建

写入 `Part` 时，`MergeTreeDataPartWriterOnDisk::calculateAndSerializeSkipIndices` 随数据 granule 一起聚合索引。每个索引都有一个 `IMergeTreeIndexAggregator`：

```
数据 mark M0  ┐
数据 mark M1  ├── index GRANULARITY 4 ──> 聚合成索引 granule I0 ──> 写入 .idx2
数据 mark M2  │                                      │
数据 mark M3  ┘                                      └── 写入 .mrk2 定位

数据 mark M4  ┐
数据 mark M5  ├── index GRANULARITY 4 ──> 聚合成索引 granule I1
数据 mark M6  │
数据 mark M7  ┘
```

聚合器具体保存什么，取决于索引类型：

- `minmax`：保存这一段内每个索引表达式的最小值和最大值。
- `set`：保存这一段内出现过的不同值，数量超过 `max_rows` 后该 granule 变成不可用于剪枝的状态。
- `bloom_filter`：保存这一段内值的 Bloom filter 位数组。
- `ngrambf_v1` / `tokenbf_v1`：先把文本拆成 n-gram 或 token，再把 token 放入 Bloom filter。

#### 查询时如何加速

读取路径不是“一上来就查二级索引”，而是先经过更便宜的剪枝，再用二级索引进一步缩小范围：

```
SELECT
  │
  ├── 1. 分区裁剪 / MinMax Part 级裁剪
  │
  ├── 2. 主键索引得到候选 mark ranges
  │       例如: [0, 128)
  │
  ├── 3. 对候选范围内的 skip index granule 做判断
  │       I0: mayBeTrueOnGranule = false → 删除 marks [0, 4)
  │       I1: mayBeTrueOnGranule = true  → 保留 marks [4, 8)
  │       I2: mayBeTrueOnGranule = false → 删除 marks [8, 12)
  │
  └── 4. 只读取保留下来的数据 marks，再执行 WHERE 行级过滤
```

这就是“加速”的来源：减少需要读取的 `.bin` / `data.bin` 压缩块数量，减少反序列化和表达式过滤的行数。它不会改变结果正确性，因为任何无法证明“不匹配”的 granule 都会保留。

如果 SQL 查询里没有主键条件，第 2 步不会把候选范围缩小成空，也不会阻止二级索引使用。此时主键阶段只是保留当前 part 的候选 mark ranges，通常是分区裁剪 / MinMax Part 级裁剪之后剩下的全量范围，例如 `[0, total_marks_count)`；随后第 3 步仍然可以用二级索引在这些候选范围上做 granule 级排除。

例如表按 `ts` 排序，但在 `status` 上建了 `set` skip index：

```sql
CREATE TABLE logs
(
    ts DateTime,
    status UInt16,
    message String,
    INDEX status_set status TYPE set(16) GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY ts;
```

查询：

```sql
SELECT count()
FROM logs
WHERE status = 500;
```

这个查询没有 `ts` 条件，所以主键索引无法利用 `ORDER BY ts` 缩小范围。但 `status_set` 能检查每个 skip-index granule 里是否可能出现 `500`：不可能出现的 granule 对应的数据 marks 会被跳过，可能出现的 granule 才会读取数据并继续执行行级 `WHERE`。因此 ClickHouse 的二级索引更准确地说是“data skipping index”：它不是先从二级索引查出主键再回表，而是在候选 marks 上证明某些 granule 不可能匹配，从而少读数据。

当前代码实现也对应这个流程：

- `RangesInDataPart` 默认把 part 的候选范围初始化为 `[0, total_marks_count)`。
- `MergeTreeDataSelectExecutor::markRangesFromPKRange` 在主键条件、`_part_offset` 条件都不可用时，直接返回传入的 `part_with_ranges.ranges`。
- `MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes` 随后仍会遍历可用的 skip indexes，并调用 `filterMarksUsingIndex`。
- `filterMarksUsingIndex` 会把候选 data mark ranges 映射成 skip-index granule ranges，读取索引 granule，调用 `mayBeTrueOnGranule`；返回 `false` 的 granule 被丢弃，返回 `true` 的 granule 转回 data `MarkRange` 并保留。

所以，没有主键信息时二级索引仍能加速的前提是：`WHERE` 中存在能被某个 skip index 表达式识别的谓词。如果查询既没有可用主键条件，也没有可用 skip-index 条件，就只能读取候选 ranges 后做行级过滤。

基础接口 `IMergeTreeIndex` 包含四个组件：

- `IMergeTreeIndexGranule` — 单个索引条目的内存表示
- `IMergeTreeIndexAggregator` — 从数据块构建 granule
- `IMergeTreeIndexCondition` — 对 granule 评估查询谓词
- `IMergeTreeIndex` — 上述组件的工厂

#### minmax

存储每个 granule 中被索引表达式的 min/max 值，作为 `Range` 对象的超矩形。条件使用 `KeyCondition::checkInHyperrectangle` 判断 granule 是否可能包含匹配行。

**磁盘上的 Granule**：序列化为每列的二进制 min/max 字段值序列。

适合场景：被索引列在物理顺序上有局部性。例如日志按时间写入，`response_time`、`event_date`、递增 ID 或按业务维度聚集的数据。若列值在每个 granule 里都覆盖全范围，`minmax` 很难跳过数据。

示例：

```sql
CREATE TABLE events
(
    ts DateTime,
    region String,
    price UInt32,
    INDEX price_mm price TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY ts;
```

假设 `price_mm` 的每个索引 granule 覆盖 4 个 marks：

| 索引 granule | 覆盖 marks | `price` min/max | `WHERE price > 900` |
|--------------|------------|-----------------|---------------------|
| `I0` | `[0, 4)` | `[10, 120]` | 不可能，跳过 |
| `I1` | `[4, 8)` | `[80, 980]` | 可能，保留 |
| `I2` | `[8, 12)` | `[30, 400]` | 不可能，跳过 |
| `I3` | `[12, 16)` | `[901, 1200]` | 可能，保留 |

最终只读取 marks `[4, 8)` 和 `[12, 16)`。保留的 granule 里仍可能有 `price <= 900` 的行，后续 `WHERE` 会继续过滤。

#### set

每个 granule 存储最多 `max_rows` 个不同值到一个内部 `Block` 中。使用 `ClearableSetVariants`（哈希表）去重。条件从查询重建 mini DAG，评估存储的值是否能满足查询。

**磁盘上的 Granule**：序列化内部 Block（列）+ 超矩形范围。

`set` 的关键限制是 `max_rows`：如果某个索引 granule 里的 distinct 值数量超过上限，磁盘上会写成不可用于剪枝的状态。读取时遇到这种 granule，`mayBeTrueOnGranule` 返回 `true`，也就是必须保留。

适合场景：每个局部数据段中的 distinct 值很少，例如按时间写入的日志里 `status`、`region`、`event_type`、低基数字段。它不适合高基数且随机分布的列，例如 `user_id` 在每个 granule 里几乎都不同。

示例：

```sql
CREATE TABLE logs
(
    ts DateTime,
    service String,
    status UInt16,
    message String,
    INDEX status_set status TYPE set(16) GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY ts;
```

假设查询：

```sql
SELECT count()
FROM logs
WHERE status = 500;
```

索引内容可以理解为：

| 索引 granule | 覆盖 marks | `status_set` 保存的值 | 判断 |
|--------------|------------|-----------------------|------|
| `I0` | `[0, 2)` | `{200, 204, 301}` | 不含 `500`，跳过 |
| `I1` | `[2, 4)` | `{200, 404, 500}` | 含 `500`，保留 |
| `I2` | `[4, 6)` | `{200}` | 不含 `500`，跳过 |
| `I3` | `[6, 8)` | distinct 超过 `16` | 无法判断，保留 |

所以读取器不用读 `[0, 2)` 和 `[4, 6)` 对应的数据 marks。`I3` 虽然可能没有 `500`，但因为集合溢出，不能安全跳过。

#### bloom_filter

每列每 granule 创建一个 `BloomFilter`。参数：`bits_per_row`、`hash_functions`。聚合器将列值哈希到 `HashSet<UInt64>`，然后填充 Bloom 过滤器位数组。条件将 WHERE 子句转为 RPN，对每个原子检查对应的 Bloom 过滤器。

`bloom_filter` 的特点是空间小，可以支持较高基数值的成员判断，但允许 false positive：

- Bloom filter 判断“不存在”：一定不存在，可以跳过。
- Bloom filter 判断“可能存在”：不确定，必须读取。

适合场景：等值、`IN`、数组或 map 中的成员判断，且查询值相对稀疏。它不适合范围条件，例如 `x > 100`。

示例：

```sql
CREATE TABLE page_views
(
    ts DateTime,
    user_id UInt64,
    url String,
    INDEX user_bf user_id TYPE bloom_filter(0.01) GRANULARITY 8
)
ENGINE = MergeTree
ORDER BY ts;
```

查询：

```sql
SELECT *
FROM page_views
WHERE user_id = 424242;
```

执行时，对每个 `user_bf` granule 检查 Bloom filter：

| 索引 granule | Bloom filter 判断 | 操作 |
|--------------|-------------------|------|
| `I0` | `424242` 一定不存在 | 跳过对应 marks |
| `I1` | `424242` 可能存在 | 保留并读取 |
| `I2` | `424242` 一定不存在 | 跳过对应 marks |
| `I3` | `424242` 可能存在 | 保留并读取 |

如果 `I3` 是 false positive，读出数据后 `WHERE user_id = 424242` 会把所有行过滤掉。代价是多读了一段数据，但不会返回错误结果。

#### ngrambf_v1 / tokenbf_v1

用于文本搜索。聚合器使用 `Tokenizer` 将文本分割为 n-gram 或 token，哈希后插入 Bloom 过滤器。条件支持 `FUNCTION_MATCH`（like, hasToken）、`FUNCTION_MULTI_SEARCH`（multiSearchAny）等。

`ngrambf_v1` 更偏向子串匹配，`tokenbf_v1` 更偏向按分词后的 token 匹配。它们的加速原理和普通 Bloom filter 一样：如果某个文本 token 或 n-gram 在 granule 的 Bloom filter 中一定不存在，就跳过对应 marks。

#### 为什么有时二级索引不生效

常见原因：

- 查询条件不能转换成该索引支持的条件。例如 `bloom_filter` 不能有效处理范围比较。
- granule 太大，摘要太粗，几乎每个 granule 都“可能匹配”。
- `set` 的 `max_rows` 太小，很多 granule distinct 值溢出。
- 被索引列随机分布，每个 granule 都包含大量不同值。
- 主键已经把范围缩得很小，二级索引的额外收益不明显。

设计 skip index 时，通常先问两个问题：这个条件能否被索引类型可靠判断？每个局部 granule 的摘要是否足够“窄”，能经常返回 `false`？

### 3.3 Marks（标记）

**定义**：Mark 是压缩数据文件中的一个位置，允许不解压整个文件就能随机访问。每个 Mark 是一个 `MarkInCompressedFile`，包含两个值：

- `offset_in_compressed_file` — 压缩块起始的字节偏移
- `offset_in_decompressed_block` — 解压块内目标数据起始的字节偏移

```
  .bin 文件（压缩块序列）
  ┌──────────────────┬──────────────────┬──────────────────┐
  │  Compressed      │  Compressed      │  Compressed      │
  │  Block 0         │  Block 1         │  Block 2         │
  │  (解压后 64KB)   │  (解压后 64KB)   │  (解压后 64KB)   │
  └──────────────────┴──────────────────┴──────────────────┘
       ↑                    ↑
       Mark 0               Mark 1
       offset_in_compressed  offset_in_compressed
       = 0                   = block0_size
       offset_in_decompressed offset_in_decompressed
       = 0                   = 0
```

上图是最容易理解的情况：每个 mark 正好落在压缩块边界上，所以 `offset_in_decompressed_block = 0`。更一般的情况是，一个压缩块里可能包含多个 granule 的数据，mark 会落在解压后字节流的中间：

```
.bin 文件
  ┌──────────────────────── compressed block 0 ────────────────────────┐
  │ 解压后字节流:  G0 tail | G1 bytes.............. | G2 head          │
  └────────────────────────────────────────────────────────────────────┘
                  ↑
                  mark 1
                  offset_in_compressed_file = block0_start
                  offset_in_decompressed_block = G1 在解压块内的起始字节
```

这种非 0 偏移很常见，原因是“granule 边界”和“压缩块边界”由不同目标控制：

- granule 边界按行数和未压缩字节数决定，用于索引和 mark range。
- 压缩块边界按压缩缓冲区大小、`min_compress_block_size`、`max_compress_block_size` 和 flush 时机决定，用于 I/O。

写入时，`MergeTreeWriterStream::getCurrentMark` 记录当前已经写到文件里的压缩字节数，以及当前压缩缓冲区中尚未 flush 的未压缩偏移。如果 mark 边界出现时当前压缩块还没结束，`offset_in_decompressed_block` 就不是 0。

#### 只有 offset，如何知道读取多少 bytes

Mark 本身只描述“从哪里开始读”，不描述“读多少 bytes”。读取长度由更高层的行数目标和类型序列化协议共同决定：

```
读取任务
  │
  ├── mark range: [from_mark, to_mark)
  │     └── 通过 `MergeTreeIndexGranularity` 得到要读多少行
  │
  ├── 每个列/子流 seek 到 from_mark 对应的 offset
  │
  ├── `ISerialization` 按类型协议反序列化指定行数
  │     ├── 固定长度类型：行数 × 类型宽度
  │     ├── String：读 length，再读 length 个 bytes，重复到足够行数
  │     ├── Array：先读 offsets，再按 offsets 决定元素子流要读多少元素
  │     └── Nullable：读 null map，再读 nested data
  │
  └── 读够目标行数后停止；必要时会继续解压后续压缩块
```

所以 marks 不需要保存每个 granule 的 byte length。对于自适应 marks，`.mrk2` / `.mrk3` 里保存的 `rows_in_granule` 能告诉读取器这个 mark 覆盖多少行；对于一次读取多个 marks 的任务，`MergeTreeIndexGranularity` 可以把 `[from_mark, to_mark)` 转成总行数。具体每个子流消耗多少字节，则由该类型的序列化格式自然决定。

以 `Array(String)` 为例，mark 只把读取器定位到 `arr.size0` 和 `arr` 两个子流的起点：

```
rows_to_read = 3

arr.size0 子流:
  读取 3 个 offsets: [2, 2, 5]
  说明这 3 行一共有 5 个字符串元素

arr 子流:
  重复 5 次:
    读取 string length
    读取 string bytes
```

读取器并不是预先知道 `arr` 子流要读多少 bytes，而是先从 offsets 子流知道要读多少元素，再通过 `String` 的长度前缀逐个消费字节。读够 5 个元素后，这个子流就停在下一行或下一个 granule 的起点，供后续连续读取继续使用。

**Mark 文件扩展名**：

| 扩展名 | 含义 |
|--------|------|
| `.mrk` | 非自适应，未压缩，Wide |
| `.mrk2` | 自适应，未压缩，Wide |
| `.mrk3` | 自适应，未压缩，Compact |
| `.mrk4` | 自适应，未压缩，Compact（带子流） |
| `.cmrk*` | 压缩变体 |

**Mark 大小**：
- 非自适应 Wide：16 字节（2 × UInt64）
- 自适应 Wide：24 字节（3 × UInt64 — 多了 granule 行数）
- Compact：`(columns_num × 16) + 8` 字节（每列偏移 + 行数）

**内存压缩**：Marks 使用临时压缩格式，每 256 个 mark 一块。对每块存储最小值和位压缩的差值。典型内存占用：整数 ~3 字节/mark，字符串 ~5 字节/mark。

**加载**：`MergeTreeMarksLoader` 从磁盘加载 marks，可选异步加载（线程池），缓存在 `MarkCache` 中。

### 3.4 Granule、Mark 与压缩块的关系

granule 是“行范围”，mark 是“文件位置”，压缩块是“解压 I/O 单位”。三者相关但不等价：

```
行空间
  G0 rows 0..8191     G1 rows 8192..16383   G2 rows 16384..24575
  └──── mark 0 ─────┘ └──── mark 1 ───────┘ └──── mark 2 ───────┘

列 a 的 a.bin
  ┌──────── compressed block A ────────┬──────── compressed block B ────────┐
  │ bytes for tail of G0 + head of G1   │ bytes for tail of G1 + G2 ...      │
  └─────────────────────────────────────┴────────────────────────────────────┘
       ↑ mark 0 may point here              ↑ mark 2 may point here
                 ↑ mark 1 may point inside decompressed block A
```

读取 `mark 1` 时，`MergeTreeReaderStream` 先 seek 到 `offset_in_compressed_file` 指向的压缩块起点，解压这个块，再丢弃 `offset_in_decompressed_block` 之前的字节。这样可以避免从文件开头顺序解压，但代价是随机读取的最小解压单位仍是压缩块。

一个 granule 的同一列子流数据也可以跨多个压缩块。mark 只给出 granule 起点；如果反序列化目标行数时当前压缩块的数据不够，底层 `CompressedReadBuffer` 会继续读取并解压下一个压缩块。

```
行空间
  G7 rows 57344..65535
  └──────────────────── one granule ────────────────────┘

列 a 的 a.bin
  ┌──────── block X ────────┬──────── block Y ────────┬──────── block Z ────────┐
  │ previous G6 tail | G7-1 │          G7-2           │ G7-3 | next G8 head     │
  └─────────────────────────┴─────────────────────────┴─────────────────────────┘
                    ↑
                    mark 7 points to G7 start inside block X
```

这种情况下读取 `G7` 会从 `block X` 的 `offset_in_decompressed_block` 开始，读完 `block X` 中属于 `G7` 的部分后继续解压 `block Y`，必要时再读 `block Z`，直到该列类型反序列化出 `G7` 需要的行数。反过来，一个压缩块也可能同时包含上一个 granule 的尾部、当前 granule 的全部或部分、下一个 granule 的头部。

因此 granule 和压缩块是多对多关系：

| 关系 | 是否可能 | 说明 |
|------|----------|------|
| 一个 granule 完全落在一个压缩块内 | 可能 | 小 granule 或压缩块较大时常见 |
| 一个 granule 跨多个压缩块 | 可能 | 宽行、变长字段或压缩块较小时会出现 |
| 一个压缩块包含多个 granule 片段 | 可能 | granule 较小或压缩块 flush 较晚时会出现 |
| granule 边界总是压缩块边界 | 不保证 | 只有写入时刚好 flush 才会重合 |

#### 多主键列时 mark 指向哪个 `.bin`

容易混淆的一点是：主键索引 `primary.idx` 把多列 PK 的值平铺在一起（每个 mark 一组 `[col0, col1, ..., colN]`），那一个 mark 是不是只对应"某一个主键列"的文件位置？

答案是 **mark 不是按 PK 列归属的**。`primary.idx` 不存任何文件偏移，它只存主键值；真正描述"文件位置"的是每个**列子流**自己的 `.mrk2`，每个 `.bin` / 子流都有一份独立的 `.mrk2`。mark N 是一个**逻辑 granule 序号**，所有列、所有子流的"mark N"指代同一段行（约 `index_granularity` 行），但每个子流的"mark N"分别指向自己 `.bin` 内的物理位置。

以 `ORDER BY (tenant_id, ts)` + 表里再有非 PK 列 `payload Array(String)` 为例，Wide `Part` 的实际布局：

```
Part 目录
├── primary.idx                       ← 只存值: 每 mark 一行 (tenant_id, ts)
│       mark 0: [tenant_id=1,  ts=100]
│       mark 1: [tenant_id=1,  ts=200]
│       mark 2: [tenant_id=2,  ts=50 ]
│       （这里没有任何文件偏移）
│
├── tenant_id.bin   ── tenant_id.mrk2          ← 主键列 1 的数据文件 + marks
├── ts.bin          ── ts.mrk2                 ← 主键列 2 的数据文件 + marks
├── payload.size0.bin ── payload.size0.mrk2    ← 非 PK 列 Array 子流（offsets）
├── payload.bin     ── payload.mrk2            ← 非 PK 列 Array 子流（elements）
└── ...

  mark 1 的"文件位置"是 4 个：
    tenant_id.mrk2 [1]       → tenant_id.bin   内的 (offset_in_compressed, offset_in_decompressed)
    ts.mrk2 [1]              → ts.bin          内的位置
    payload.size0.mrk2 [1]   → payload.size0.bin 内的位置
    payload.mrk2 [1]         → payload.bin     内的位置

  这四个"mark 1"指向**同一段行**（granule 1），但**各自指向自己的 .bin**。
```

关键事实：

- **`primary.idx` 不含任何 mark/offset/字节位置**，只把每个 mark 起点的 PK 值平铺写入（见上文 3.1 节磁盘格式）。它的作用是给 `KeyCondition` 提供"按 mark 二分"的内存数组，让裁剪算出 `[from_mark, to_mark)` 区间。
- **每个列子流（包括 PK 列、非 PK 列、Array offsets、Nullable null map、LowCardinality dictionary 等所有子流）都有自己的 `.mrk2`**；mark N 在每份 `.mrk2` 中各占一条记录（24 字节：`offset_in_compressed_file` + `offset_in_decompressed_block` + `rows_in_granule`）。
- 主键由多列组成时，**PK 列之间是独立的列子流**，例如 `tenant_id.bin` 与 `ts.bin` 是各自压缩的两个文件，互不重叠；mark N 在 `tenant_id.mrk2` 里指向 `tenant_id.bin`，在 `ts.mrk2` 里指向 `ts.bin`，二者的 `offset_in_compressed_file` 数值通常完全不同。
- **查询的读列集合 = SELECT 列 ∪ WHERE/PREWHERE 引用列**。例如 `SELECT payload WHERE tenant_id = 1 AND ts BETWEEN 100 AND 200`：
  1. `KeyCondition` 在 `primary.idx` 的内存数组上二分得到**候选** mark range `[from, to)`——这是"可能包含命中行"的 granule 范围，**不是精确行集合**：边界 granule 内通常只有一部分行真正满足条件（mark M 起点 PK 值落在区间外、但 mark M 内某些行落在区间内是常见情况）。
  2. 因此读取器必须把过滤列也读进来做行级 `WHERE`：`tenant_id.bin` / `ts.bin` **会被打开**，各自走自己的 `.mrk2` seek，反序列化候选 mark range 的行，再用 `tenant_id = 1 AND ts BETWEEN 100 AND 200` 逐行过滤。
  3. PREWHERE（无论用户显式写 `PREWHERE` 还是被 `move_primary_key_columns_to_end_of_prewhere` 等优化自动搬运）会把这些过滤列**先**读：先扫 `tenant_id.bin` / `ts.bin` 算出存活行掩码，再用掩码去读 `payload.size0.bin` / `payload.bin`，让 `payload` 只读存活行覆盖的压缩块，省掉被过滤掉行的反序列化。
  4. **没被引用的列才完全不打开**。例如表里另有 `debug_blob` 列、查询既不 SELECT 也不 WHERE 它，那它的 `.bin` 在整次 SELECT 中不会被 seek。

  也就是说，PK 列被引用在 `WHERE` 时仍要走列读路径——`primary.idx` 只负责把 granule 集合从全表收敛到候选，行级精确过滤还是要回到对应列的 `.bin`。唯一能让 PK 过滤列免读的特殊情形是：`KeyCondition::alwaysUnknownOrTrue` 能证明候选 range 的**所有** granule 在该谓词上都恒为真（条件已被 PK 完全包含、且不在 range 边界处出现"部分匹配"），代码层面对应 `MergeTreeRangeReader` 跳过对应过滤步骤——但这条捷径很苛刻，不能作为一般规律。
- Compact `Part` 没有这种"每子流一文件"的展开。它只有一份 `data.mrk3` / `data.mrk4`，每条 mark 是一个**偏移数组**：每列（或每子流）一对 `(offset_in_compressed, offset_in_decompressed)`，再加一个 `rows_in_granule`。例如多主键 `(tenant_id, ts)` + `payload` 在 Compact 下，mark 1 形如：

  ```
  mark 1 in data.mrk3:
    [ tenant_id pos ][ ts pos ][ payload pos ][ rows_in_granule ]
                                                      ↑
        每列/子流一组 (offset_in_compressed, offset_in_decompressed)
  ```

  此时 mark 仍然不归属任何单一 PK 列，它把整 granule 内所有列的位置一次性记录下来，读取时按需取用对应列的 pair。

一句话总结：**主键是按值排序行的依据，决定 mark 的行边界；mark 的"文件位置"则是逐列子流分别存在各自 `.mrk2` 里的，与 PK 列数无关，也不存在"mark 属于某个 PK 列"这种说法。**

#### 压缩块何时 flush

写入列数据时，数据先进入 `CompressedWriteBuffer` 的未压缩缓冲区。flush 的本质是调用 `CompressedWriteBuffer::next`：把当前缓冲区里的未压缩 bytes 压缩成一个压缩块，写入 `.bin` 或 `data.bin`，然后清空缓冲区继续写。

当前主要有几类触发点：

1. **缓冲区满自动 flush**

   `CompressedWriteBuffer` 有一个内部未压缩缓冲区，大小来自 `max_compress_block_size`。写入超过当前缓冲区容量时，底层 `WriteBuffer` 会触发 `next`，形成一个压缩块。

   ```text
   uncompressed buffer
     [ bytes ... bytes ]  达到 max_compress_block_size
              │
              ▼
     compress + write as one compressed block
   ```

   如果启用了 adaptive write buffer，初始缓冲区可以较小，之后逐步增长到 `max_compress_block_size` 上限。

2. **Wide `Part` 写 mark 前的主动 flush**

   Wide 格式在写每个列子流的 mark 前，会检查当前压缩缓冲区里已经积累的未压缩 bytes。如果达到 `min_compress_block_size`，就先 flush，再记录 mark。

   逻辑可以简化为：

   ```text
   before writing mark for next granule:
     if current_uncompressed_offset >= min_compress_block_size:
         flush compressed block
     mark = (compressed_file_offset, decompressed_block_offset)
   ```

   这样做的效果是：如果当前压缩块已经足够大，就让新 mark 尽量落在新压缩块开头，使 `offset_in_decompressed_block = 0`。如果当前块还小于 `min_compress_block_size`，不会为了 mark 强行 flush，mark 会指向当前压缩块内部，因此 `offset_in_decompressed_block` 可能非 0。

3. **Compact `Part` 的 granule/列边界 flush**

   Compact 格式把多个列或子流写进同一个 `data.bin`。为了读取时定位更简单、更顺序，写入代码会在 granule 内的列/子流边界主动 flush：

   - 当切换到另一个压缩流时，前一个流会 `next`。
   - 如果开启 `compress_per_column_in_compact_parts`，每个列写完后会 flush。
   - 一个 granule 写完后，最后一个流也会 flush。

   因此 Compact `Part` 中，压缩块边界更倾向于贴近 granule 内的列/子流边界，而不是像 Wide 那样主要由 `min_compress_block_size` 和 `max_compress_block_size` 控制。

4. **收尾 finalize flush**

   `Part` 写完、主键索引或 skip index 写完、文件关闭前，`finalize` 会把缓冲区里剩余的 bytes flush 成最后一个压缩块。即使最后一个块小于 `min_compress_block_size`，也必须写出，否则尾部数据会丢失。

这几个规则解释了为什么 mark 和压缩块边界有时重合、有时不重合：

- 如果 mark 前缓冲区已经达到 `min_compress_block_size`，Wide 写入会先 flush，mark 通常落在新块开头。
- 如果 mark 前缓冲区还很小，为了避免过多小压缩块，Wide 写入不会 flush，mark 会落在当前块内部。
- 如果一段 granule 的数据超过 `max_compress_block_size`，即使没有到下一个 mark，缓冲区满也会自动 flush，所以一个 granule 会跨多个压缩块。
- Compact 写入会额外按列/子流边界 flush，因此压缩块边界更频繁，也更接近 Compact 的读取布局。

自适应 granularity 由 `computeIndexGranularity` 决定：行数上限来自 `index_granularity`，字节上限来自 `index_granularity_bytes`。当单行很宽时，一个 granule 的行数会少于默认值，避免一次 mark 读取过多未压缩数据。

---

## 4. 写入路径

### INSERT → Block → 磁盘 Part 的数据流

`writeTempPart` 不是从网络或解析器逐行收集数据，也不是自己等到某个固定行数后才开始排序。它接收的是上游 `INSERT` 管线已经形成的 `BlockWithPartition`，然后对这个单分区 `Block` 排序并写成一个临时 `Part`。排序之前真正的 records 缓冲主要发生在 insert block 形成阶段。

默认情况下，`MergeTree` 使用大块写入：`InterpreterInsertQuery` 会在写入管线中插入 `PlanSquashingTransform`。这个 transform 把上游产生的小 `Chunk` 累积成较大的 `Chunk`：

- `min_insert_block_size_rows` 默认是 `DEFAULT_INSERT_BLOCK_SIZE`，当前代码中为 `1048449`。
- `min_insert_block_size_bytes` 默认是 `DEFAULT_INSERT_BLOCK_SIZE * 256`，约 `256 MiB`。
- `use_strict_insert_block_limits` 默认关闭；非 strict 模式下，累计行数或累计字节数任一达到 min 阈值就可以生成 block。查询结束时，不足阈值的尾部数据也会 flush。

因此，一个常规 `INSERT` 可以理解为“先尽量攒到约 `1048449` 行或约 `256 MiB`，再交给 `MergeTreeSink`”。实际大小会受输入格式解析、尾块、内存上限、并行写入、物化视图、异步写入、用户设置以及是否跨分区影响。

这里的“攒”只发生在当前这条 `INSERT` 管线内部，不会等待其它 `INSERT` 一起凑大 block：

- 对 `INSERT ... VALUES` / `INSERT ... FORMAT`，客户端还在发送数据时，`PlanSquashingTransform` 会尽量把已经到达的小 `Chunk` 合成更大的输出；客户端输入结束后，不足阈值的尾部数据也会 flush。
- 对 `INSERT SELECT`，它聚合的是上游 `SELECT` 管线已经产生的 `Chunk`；上游结束后尾块同样会 flush。
- 默认非 strict 模式只要行数或字节数任一达到 min 阈值就可以输出，不要求两个阈值同时满足。
- 流式 `INSERT` 如果设置了 `input_format_max_block_wait_ms`，还可能因为等待超时刷出 partial block；为了让这批行及时可见，`MergeTreeSink` 会在当前 `consume` 后立即提交刚写出的延迟 part。

所以 `PlanSquashingTransform` 不会让一条小 `INSERT` 因为达不到 `1048449` 行而无限等待。它影响的是一条大 `INSERT` 内部首批行进入 `MergeTreeSink` 的时机，而不是跨请求的提交边界。

```
INSERT 输入
    │
    ├── Row-based format parser / INSERT SELECT pipeline
    │       生成较小的 `Chunk`
    │
    ▼
PlanSquashingTransform
    │  累积 `Chunk`
    │  默认非 strict: rows >= `min_insert_block_size_rows`
    │               或 bytes >= `min_insert_block_size_bytes`
    │  strict: 同时满足 min rows/min bytes，或触发 max rows/max bytes
    │
    ▼
ApplySquashingTransform
    │  把计划中的多个 `Chunk` 合成一个较大的 `Chunk`
    │
    ▼
MergeTreeSink::consume
    │  将 `Chunk` 转成 `Block`
    │
    ▼
MergeTreeDataWriter::splitBlockIntoParts
    │  无分区或全在同一分区: 原 `Block` 直接进入下一步
    │  跨多个分区: scatter 成多个 `BlockWithPartition`
    │
    ▼
MergeTreeDataWriter::writeTempPart
    │  对每个单分区 `BlockWithPartition` 分别写一个临时 `Part`
```

### 写入完成前的数据是否可查

不能。`MergeTree` 没有把正在 `INSERT` 的内存缓冲区当成读路径的一部分，也没有从未完成的临时 `Part` 中读取数据。普通 `SELECT` 只从已经进入 `Active` 状态的 `Part` 集合构造读取任务。

因此，写入路径里有三段对查询不可见的区间：

- `PlanSquashingTransform` 正在累积输入 `Chunk` 时，这些行还只是当前 `INSERT` 管线里的内存数据，没有进入表的 part 集合。
- `MergeTreeDataWriter::writeTempPart` 已经把数据写到 `tmp_` 临时目录后，这个 `Part` 仍处于 `Temporary` 状态，不在 `data_parts` 中。
- `renameTempPartAndAdd` / `renameTempPartAndReplace` 把临时目录改成最终名称并放入工作集时，先以 `PreActive` 状态登记。代码注释明确说明 `PreActive` 的含义是“在 `data_parts` 中，但不用于 `SELECT`”。

真正的可见性边界在提交阶段：

```
`finishDelayedChunk` / `finishDelayed`
    ├── `temp_part->finalize`
    ├── `commitPart`
    │     ├── `fillNewPartName` / 复制表分配 `block number`
    │     ├── `renameTempPartAndAdd` 或 `renameTempPartAndReplace`
    │     │     └── `Temporary` → `PreActive`
    │     └── `transaction.commit`
    │           └── `PreActive` → `Active`
    └── 提交成功后，后续 `SELECT` 才能从这个 `Part` 读到数据
```

查询侧也按这个边界工作：`MergeTreeData::getDataPartsVectorForInternalUsage` 的无参版本只返回 `{Active}` 的 regular parts；`MergeTreeDataSelectExecutor` 后续的分区裁剪、主键裁剪和 skip index 裁剪都只在这批 `Active` parts 上进行。已经开始执行并拿到 part snapshot 的 `SELECT` 不会半路补读正在提交的新 part；提交之后新启动的读取才会看到它。

这也是为什么 `MergeTreeSink` 可以把“写临时 part”和“提交 part”错开一拍来做流水线：`consume` 先写当前 `Chunk` 产生的临时 part，再把它暂存在 `delayed_chunk`；下一次 `consume` 或 `onFinish` 才调用 `finishDelayedChunk`。这段延迟只影响本次 `INSERT` 内部的提交时机，不会让未提交数据对外可查。`INSERT` 正常返回前，`onFinish` 会提交最后一个延迟的 part；如果查询和写入并发，查询能否看到这批数据取决于它取 part snapshot 的时间是否晚于 `transaction.commit`。

### `INSERT` 什么时候返回给用户

对普通同步 `INSERT` 到 `MergeTree`，返回点在提交完成之后，而不是 `writeTempPart` 写完临时目录之后：

下面流程图里的“输入解析 / `INSERT SELECT`”指写入管线上游的数据来源：

- “输入解析”不是解析 `INSERT` 语句本身，而是把客户端提交的数据格式解析成内部 `Chunk`，例如 `INSERT ... VALUES`、`INSERT ... FORMAT CSV`、`INSERT ... FORMAT JSONEachRow` 或 `FORMAT Native`。
- `INSERT SELECT` 则没有客户端数据格式解析阶段；数据来自上游 `SELECT` 查询管线，`SELECT` 执行结果同样以 `Chunk` 形式进入后续写入管线。

两者的共同点是：到达 `PlanSquashingTransform` 时，都已经是一批批内部 `Chunk`，之后才按 insert block 规则合并、分区、写临时 `Part` 并提交。

```
输入解析 / `INSERT SELECT`
    │
    ▼
`PlanSquashingTransform` flush 出待写 block
    │
    ▼
`MergeTreeSink::consume`
    │
    ├── `splitBlockIntoParts`
    ├── `writeTempPart` 写 `tmp_` 临时 `Part`
    └── 暂存到 `delayed_chunk`
    │
    ▼
`MergeTreeSink::onFinish`
    │
    └── `finishDelayedChunk`
          ├── `temp_part->finalize`
          ├── `commitPart`
          │     ├── `renameTempPartAndAdd`
          │     └── `transaction.commit`
          └── `Part` 进入 `Active`
    │
    ▼
返回给用户
```

因此，普通同步 `INSERT` 正常返回时，这批数据已经提交为 `Active` part；之后新启动的 `SELECT` 可以看到它。同步 `INSERT` 不会等待后台 merge，零级小 `Part` 合并成更大 `Part` 是后台任务。

异步写入需要单独看：

- 如果实际走 `async_insert` 且 `wait_for_async_insert = 0`，请求在数据进入异步队列后就可以返回，此时不保证已经写成 `Active` part，也不保证新 `SELECT` 立刻可见。
- 如果 `async_insert` 且 `wait_for_async_insert = 1`，客户端会等异步队列后台 flush 完成；成功返回时语义接近同步写入，已经完成真实写入和提交。

### 每次只插入一行时会发生什么

如果每条请求只插入一行：

```sql
INSERT INTO t VALUES (...)
```

这一行会被解析成很小的 `Chunk`。因为这条请求的输入随后结束，`PlanSquashingTransform` 会 flush 尾块，不会继续等到 `min_insert_block_size_rows` 或 `min_insert_block_size_bytes`。后续路径仍然完整执行：

```
1 行 `Chunk`
    → `Block`
    → `splitBlockIntoParts`
    → `writeTempPart`
    → `finalize`
    → `commitPart`
    → `Active` 零级 `Part`
    → 返回
```

这类插入的单次返回时间通常主要由解析、写小 `Part`、压缩、索引和元数据提交决定。非复制 `MergeTree` 常见是毫秒到几十毫秒量级，但会受磁盘、列数、索引、当前 part 数量、fsync 相关设置等影响；`ReplicatedMergeTree` 还要经过 `Keeper` 分配 block number 和写复制日志，`insert_quorum` 还会进一步增加等待。

正确性上，一行一行同步插入没有问题，返回后可见；工程代价是每次都会制造一个很小的零级 `Part`。高频一行写入会造成大量小 `Part`，增加后台 merge 压力，最终触发 `too many parts` 延迟或拒绝。吞吐型场景通常应在客户端批量写入，或者使用 `async_insert` 把多次小写入在服务端合并。

### `writeTempPart` 内部步骤

```
MergeTreeDataWriter::writeTempPart
    │
    ▼
writeTempPartImpl
    │
    ├── 1. 计算分区 MinMax、临时 Part 名称和待物化 skip index
    │
    ├── 2. 执行 sorting key / skip index 需要的表达式
    │
    ├── 3. 构造 sorting key 的 `SortDescription`
    │
    ├── 4. 如果当前 `Block` 未按 sorting key 有序，
    │      通过 `stableGetPermutation` 得到排序 permutation
    │
    ├── 5. 对特殊 `MergeTree` 模式可选执行 `mergeBlock`
    │      例如 `Replacing`、`Collapsing`、`Summing`、`Aggregating`
    │
    ├── 6. MergedBlockOutputStream → MergeTreeDataPartWriter
    │      ├── 写入列数据到 `.bin` / `data.bin`
    │      ├── 写入 marks 到 `.mrk*`
    │      ├── 构建并写入主键索引 `primary.idx`
    │      └── 构建并写入二级索引 `skp_idx_*.idx2`
    │
    └── 7. finalizePart
           ├── 写入 `checksums.txt`
           ├── 写入 `columns.txt`、`serialization.json` 等
           ├── 写入 MinMax 分区索引
           └── 写入 TTL 信息
    │
    ▼
提交 Part（重命名 `tmp_` → 最终名称）
```

排序作用域是当前这个单分区 `BlockWithPartition`。`writeTempPart` 不会等待多个 part 级 block 再统一排序；一次 `INSERT` 如果被拆成多个分区 block，就会分别排序并写出多个零级 `Part`。后续全局范围内的有序性由后台 merge 在同一分区内逐步合并维护。

### 底层写入前的缓冲边界

`writeTempPart` 排序完成后，`MergedBlockOutputStream` 把同一个 `Block` 交给具体 part writer。这里还有 granule 和压缩层面的缓冲，但它们不改变 `writeTempPart` 的排序粒度。

- `Wide` writer：`MergeTreeDataPartWriterWide::write` 根据 `index_granularity` / `index_granularity_bytes` 计算 granule，然后逐列、逐子流写入。默认 `index_granularity` 是 `8192` 行，`index_granularity_bytes` 默认是 `10 MiB`。如果上一批写入留下了未完成 mark，会通过 `rows_written_in_last_mark` 接续补齐。
- `Compact` writer：`MergeTreeDataPartWriterCompact::write` 有 `ColumnsBuffer`。当当前 `Block` 不足一个 mark/granule 时，会先把列数据累积到 `ColumnsBuffer`；达到 `current_mark_rows` 后写出，最后不足一个 granule 的尾部在 finalize 时写出。这是 granule 层缓冲，不是 `INSERT` block 层缓冲。
- 压缩层：`CompressedWriteBuffer` 还会按 `min_compress_block_size` / `max_compress_block_size` 控制压缩块 flush。mark 可以落在压缩块内部，也可以正好落在新压缩块开头。

### 详细步骤

**Step 1 — Block 形成**：输入解析或 `INSERT SELECT` 管线产生 `Chunk`，`PlanSquashingTransform` 按 `min_insert_block_size_rows`、`min_insert_block_size_bytes`、`max_insert_block_size`、`max_insert_block_size_bytes` 等设置规划输出 block。默认非 strict 模式只要求行数或字节数任一达到 min 阈值。

**Step 2 — Sink**：`MergeTreeSink` 接收已经形成的 `Chunk`，转成 `Block`。支持延迟处理以实现并行：上一个 `Chunk` 写入时可准备新 `Chunk`。

**Step 3 — 分区分割**：`splitBlockIntoParts` 按分区键评估每行，拆分为多个 `BlockWithPartition`，每个包含单个分区的行。

**Step 4 — 排序和即时合并**：`writeTempPartImpl` 根据 sorting key 构造 `SortDescription`，必要时通过 `stableGetPermutation` 得到排序 permutation。对于 `Replacing`、`Collapsing`、`Summing` 等模式，如果启用 `optimize_on_insert`，还会在当前 block 内通过 `mergeBlock` 做即时合并。

**Step 5 — 写入**：通过 `MergedBlockOutputStream` 包装 `MergeTreeDataPartWriterOnDisk`：
- `computeIndexGranularity` 决定每 granule 的行数（自适应或固定）
- Wide：`writeColumn` → `writeSingleGranule` 遍历子流，调用 `ISerialization::serializeBinaryBulkWithMultipleStreams`
- Compact：`writeDataBlock` 将所有列交错写入一个文件，必要时先用 `ColumnsBuffer` 凑齐当前 granule

**Step 6 — 索引构建**：与数据写入同步进行。`calculateAndSerializePrimaryIndex` 在每个 mark 边界写入 PK 值。`calculateAndSerializeSkipIndices` 聚合二级索引数据并写入 granule。

**Step 7 — 收尾**：写入元数据文件（checksums、columns、count、serialization 等）。

### 临时 Part 和重命名

Part 初始创建时使用 `tmp_` 前缀。成功写入并可选 fsync 后，重命名为最终名称（如 `all_1_1_0`）并在 Part 集合中激活。

---

## 5. 读取路径

### 查询 → 数据流

```
SELECT 查询
    │
    ▼
ReadFromMergeTree（QueryPlan Step）
    │
    ▼
MergeTreeDataSelectExecutor::read
    │
    ├── Step 1: selectPartsToRead
    │     ├── MinMax 索引过滤（分区裁剪）
    │     ├── PartitionPruner 评估分区表达式
    │     ├── 虚拟列过滤（_part = 'name'）
    │     └── UUID 过滤
    │
    ├── Step 2: filterPartsByPrimaryKeyAndSkipIndexes
    │     ├── markRangesFromPKRange
    │     │     KeyCondition 遍历主键索引（有序 PK 值数组）
    │     │     二分查找找到可能包含匹配数据的最小 mark 范围
    │     │     记录 SearchAlgorithm（BinarySearch vs GenericExclusionSearch）
    │     │
    │     └── filterMarksUsingIndex
    │           对每个有用的二级索引：
    │           读取候选 mark 范围的索引数据
    │           调用 `condition->mayBeTrueOnGranule`
    │           剪枝确定不匹配的 granule
    │           支持析取处理（PartialDisjunctionResult + boost::dynamic_bitset）
    │
    ├── Step 3: 创建 MergeTreeReadPool
    │
    └── Step 4: 每线程 MergeTreeReadTask
          │
          ▼
          MergeTreeSelectProcessor
              │
              ├── IMergeTreeReader::readRows(from_mark, max_rows, ...)
              │     ├── 在 .bin 文件中 seek 到 mark 位置
              │     ├── 解压并反序列化列数据
              │     └── 应用 PREWHERE 过滤
              │
              └── 返回过滤后的 Block
```

### 读取器差异

**Wide Reader**（`MergeTreeReaderWide`）：
- 每列打开独立的流（`MergeTreeReaderStream`）
- 对每个 mark 范围，seek 到 `offset_in_compressed_file`，解压块，读取 `offset_in_decompressed_block` 字节
- 支持预取、子流缓存、反序列化前缀缓存
- **优势**：可以只读需要的列

**Compact Reader**（`MergeTreeReaderCompact`）：
- 使用单一流
- 每个 mark 包含所有列的偏移量
- 读取是顺序遍历交错数据
- **优势**：小 Part 的顺序 I/O 更友好

### 后处理

读取后：评估缺失的默认列、应用 ALTER 转换、PREWHERE 过滤，结果作为 `Chunk` 返回给上层管线。

---

## 6. 合并过程

### 架构（`MergeTask`）

合并实现为**协程**，分为多个阶段。每个阶段的 `execute` 返回 `true` 继续或 `false` 完成。支持协作调度 — 合并在块之间可以暂停。

### 四个阶段

```
合并开始
    │
    ▼
Stage 1: ExecuteAndFinalizeHorizontalPart
    │  按排序顺序读取所有源 Part（水平合并）
    │  构建合并管线：读取 → 合并算法 → 写入新 Part
    │  写入主键 + 小列/收集列
    │  记录行来源到 RowsSourcesTemporaryFile（用于垂直合并）
    │  计算投影
    │
    ▼
Stage 2: VerticalMergeStage（可选，大列使用）
    │  每列独立从源 Part 读取
    │  使用 Stage 1 记录的行来源信息
    │  避免同时加载所有列，降低内存使用
    │
    ▼
Stage 3: MergeTextIndexStage
    │  将源 Part 的文本索引合并到新 Part
    │
    ▼
Stage 4: MergeProjectionsStage
    │  合并投影（每个投影本身是一个迷你 MergeTree）
    │  递归合并
    │
    ▼
合并完成
```

### 合并算法选择

| 算法 | 说明 | 适用场景 |
|------|------|---------|
| **水平合并** | 单次排序遍历合并所有列 | 小 Part |
| **垂直合并** | 先合并 PK + 小列，再逐列合并大列 | 大 Part、多列场景 |

选择依据 Part 大小和列数量（`chooseMergeAlgorithm`）。

### 合并选择策略（`MergeTreeDataMergerMutator`）

使用可插拔的 `MergeSelectorApplier` + `MergeSelectors` + `MergePredicates` 决定合并哪些 Part。启发式考虑：
- Part 数量和大小
- Level（合并深度）
- TTL 过期
- Part 是否正在被其他操作处理

---

## 7. Part 生命周期

### 状态机

```
                    ┌──────────────────────────────────────────────┐
                    │                                              │
Temporary ──→ PreActive ──→ Active ──→ Outdated ──→ Deleting      │
    │             │                         │                      │
    │             │                         └──→ DeleteOnDestroy   │
    │             │                                (移到其他磁盘)   │
    │             └──→ (回滚)                                     │
    └──────────────────────────────────────────────────────────────┘
```

| 状态 | 说明 |
|------|------|
| `Temporary` | 正在写入；不在 `data_parts` 中 |
| `PreActive` | 在 `data_parts` 中但对 `SELECT` 不可见 |
| `Active` | 对当前和未来的 `SELECT` 可见 |
| `Outdated` | 被覆盖 Part 取代；仅对运行中的 `SELECT` 可见 |
| `Deleting` | 被清理线程从磁盘删除 |
| `DeleteOnDestroy` | 数据已移到其他磁盘；析构时删除本地副本 |

### 命名规范（`MergeTreePartInfo`）

Part 名称格式：`<partition_id>_<min_block>_<max_block>_<level>[_<mutation>]`

示例：`all_1_5_2`
- `partition_id` = `all`（分区标识，来自分区键表达式）
- `min_block` = 1（插入块号范围起始）
- `max_block` = 5（插入块号范围结束）
- `level` = 2（合并深度，0 表示直接插入，每次合并递增）
- `mutation`（可选）= 变异版本号

特殊情况：
- **零级 Part**：`min_block == max_block`（单次插入）
- `MAX_LEVEL`（999999999）：用于 `DROP PARTITION` 操作
- **Patch Part**：前缀 `patch-<hash>-`

### Part Block：`block number` 不是压缩块，也不是内存 `Block`

在 `MergeTree` 里，`block` 这个词容易混淆，至少有三层含义：

| 概念 | 存在哪里 | 作用 |
|------|----------|------|
| 内存 `Block` | 查询/写入管线里的列批次 | 作为执行管线的批量处理单元，可能被拆分、排序、合并后写入 Part |
| Part 的 `block number` | `MergeTreePartInfo` 的 `min_block` / `max_block` | 表示这个 Part 覆盖了某个分区内的插入序列范围，用于命名、去重、合并覆盖判断、复制一致性 |
| 压缩块 | `.bin` 文件内部 | 控制压缩/解压的 I/O 单元，和 mark/granule 一起决定读放大 |

本节说的 **part block** 指第二种：`MergeTreePartInfo` 中的逻辑 `block number`。它不是磁盘上一个独立文件，也不是 `.bin` 中的压缩块。`block number` 更像是“某个分区内提交顺序的编号”。一个零级 Part 通常覆盖一个编号；合并后的 Part 覆盖一段编号区间。

```
分区 202401 的 active part 覆盖关系

插入后：
202401_12_12_0   202401_13_13_0   202401_14_14_0   202401_15_15_0
      │                 │                 │                 │
      └─ block 12       └─ block 13       └─ block 14       └─ block 15

一次后台合并后：
202401_12_13_1                    202401_14_15_1
      │                                  │
      └─ 覆盖 block 12..13              └─ 覆盖 block 14..15

再次合并后：
202401_12_15_2
      │
      └─ 覆盖 block 12..15
```

读取时，`Active` Part 在同一分区内应该形成互不重叠的覆盖集合。合并生成的新 Part 进入 `Active` 后，旧 Part 被新 Part 覆盖，状态变成 `Outdated`。覆盖关系由 `MergeTreePartInfo` 的分区 ID、`min_block`、`max_block`、`level` 和 `mutation` 判断。

### 何时新建 Part Block

最常见的新 `block number` 来自 `INSERT`。写入路径大致是：

```
INSERT 输入数据
    │
    ▼
按分区表达式拆分成多个 `BlockWithPartition`
    │
    ├── 分区 202401 → 写临时 Part：tmp_insert_202401_..._0
    │
    └── 分区 202402 → 写临时 Part：tmp_insert_202402_..._0
    │
    ▼
提交时为每个临时 Part 分配最终 `block number`
    │
    ├── 202401 分区拿到 block 12 → 202401_12_12_0
    └── 202402 分区拿到 block  8 → 202402_8_8_0
```

几个关键点：

- 新 `block number` 是按分区递增的。同一次 `INSERT` 如果写入多个分区，会在每个分区各自产生一个零级 Part 和一个分区内的 `block number`。
- 不是每次调用底层 `write` 都产生新的 `block number`。`.bin` 文件、mark、主键索引、skip index 在写临时 Part 的过程中会多次写入；这些都是同一个临时 Part 内部的文件写入，不会各自产生新的 part block。
- 更准确地说：**每个成功提交为 active Part 的零级 Part，才会拿到一个新的 `block number`**。如果一次 `INSERT` 最终只在一个分区提交一个零级 Part，就产生一个新的 `block number`；如果同一次 `INSERT` 被分区表达式拆到三个分区，就会在三个分区中各产生一个新的 `block number`。
- `MergeTreeDataWriter::writeTempPartImpl` 写临时 Part 时，数据已经落盘，但最终提交顺序还没有完全确定。代码里也明确说明，零级 Part 写入阶段还不知道最终 `_block_number` / `_block_offset`。
- 对 `ReplicatedMergeTree`，`ReplicatedMergeTreeSink` 在提交阶段调用 `allocateBlockNumber`，拿到分区内单调递增的编号后，把 `part->info.min_block` 和 `part->info.max_block` 都设置为该编号，再用最终 `MergeTreePartInfo` 重命名 Part。
- 复制表还会把正在提交但尚未出现在复制日志里的编号当作 `committing_blocks` 处理。这样后台合并在判断两个 Part 之间是否还有缺口时，不会误以为中间编号已经不存在。
- `block_id` 和 `block number` 不是一回事。`block_id` 用于插入去重，通常来自插入数据的哈希或显式 token；`block number` 用于确定 Part 覆盖范围。发生去重命中时，可以检测到重复 `block_id`，从而不提交新的 active Part。

也就是说，**新建 block 的时机不是管线里生成一个内存 `Block` 的时机，而是一个临时 Part 被提交为 active Part 的时机**。内存 `Block` 只是批处理容器；一次写入可能经过多个内存 `Block`，最终也可能因为分区拆分产生多个 Part。

可以把产生时机拆成两步看：

```
Step 1: 写临时 Part
    tmp_insert_202401_..._0
    这个阶段会写列文件、mark、索引、checksums，但还不是 active Part。
    对复制表来说，最终 `block number` 还没有确定。

Step 2: 提交临时 Part
    为分区 202401 分配下一个 `block number`，例如 12。
    将 `min_block = 12`、`max_block = 12` 写入 `MergeTreePartInfo`。
    将临时 Part 重命名/提交为 202401_12_12_0。
    从这个时刻开始，它才是一个覆盖 block 12 的 active Part。
```

如果写临时 Part 成功，但提交阶段因为去重命中、异常或事务回滚没有进入 `Active`，那么它不会成为一个可见的新 part block。

这里的“提交临时 Part”在代码里不是指写完每个文件后立刻提交，而是 `MergeTreeSink` / `ReplicatedMergeTreeSink` 把一个完整临时 Part finalize 之后，将它加入表的 active part 集合的动作。

普通写入管线的时序更接近下面这样：

```
consume 第 1 个输入 `Chunk`
    │
    ├─ `splitBlockIntoParts`：按分区拆成 `BlockWithPartition`
    ├─ `writeNewTempPart` / `writeTempPart`：写临时 Part 目录
    └─ 暂存到 `delayed_chunk` / `delayed_parts`
       此时还没有对查询可见，也还不能算成功提交的 active Part

consume 第 2 个输入 `Chunk`
    │
    ├─ 先调用 `finishDelayedChunk` / `finishDelayed`
    │     ├─ `temp_part->finalize`
    │     ├─ 计算 deduplication hashes
    │     ├─ `commitPart`
    │     └─ 成功后进入 `Active`
    │
    └─ 再写第 2 个输入 `Chunk` 产生的新临时 Part

onFinish
    │
    └─ 如果还有最后一个延迟的临时 Part，也调用
       `finishDelayedChunk` / `finishDelayed` 把它提交
```

所以更精确地说，临时 Part 的提交时机是：

- 正常情况下，**下一次 `consume` 到来时，先提交上一批已经写好的临时 Part**。
- 插入流结束时，`onFinish` 会提交最后一批延迟的临时 Part。
- 如果 `input_format_max_block_wait_ms` 非零，流式 `INSERT` 会因为超时刷出 partial block；为了让刚写出的数据及时可见，会在当前 `consume` 后立即调用 `finishDelayedChunk` / `finishDelayed`。
- 如果存在依赖物化视图并启用同步等待相关逻辑，也可能在当前 `consume` 后立即提交。
- 如果临时 Part 太多或打开流太多，超过 `max_insert_delayed_streams_for_parallel_write` 相关阈值，也会先 flush 并提交已有延迟 Part，避免同时持有太多打开文件流。

对非复制表，`commitPart` 的核心动作是：

```
`fillNewPartName`
    为 Part 填最终名称和 `block number`

deduplication log 检查
    如果命中重复 `block_id`，返回冲突，不把它加入 `Active`

`renameTempPartAndAdd`
    把临时目录改成最终 Part 目录，并把 Part 放入工作集

`transaction.commit`
    提交内存状态，Part 变成可见的 `Active`
```

对 `ReplicatedMergeTree`，`commitPart` 还要通过 Keeper 做复制一致性提交：

```
`allocateBlockNumber`
    在分区内申请并锁定下一个 `block number`

设置 `part->info.min_block = block_number`
设置 `part->info.max_block = block_number`
重命名为最终 Part 名称

构造 Keeper transaction
    写复制日志、写 deduplication block、登记 Part、释放 block number lock

本地事务提交
    Part 进入本副本的 `Active` 集合
```

因此，**新的 part block 真正产生在 `commitPart` 成功这一步，而不是 `writeTempPart` 写文件这一步**。`writeTempPart` 只是在磁盘上准备候选数据；`commitPart` 成功后，这个候选数据才有最终 Part 名称、最终 `block number`，并对查询可见。

### 何时合并 Part Block

`MergeTree` 的后台合并合并的是 **Part**，不是直接合并 `block number` 本身。`block number` 是合并结果 Part 名称中的覆盖范围。

```
待合并 Part：
202401_12_12_0 + 202401_13_13_0 + 202401_14_16_2

合并结果：
202401_12_16_3

规则：
min_block = min(12, 13, 14) = 12
max_block = max(12, 13, 16) = 16
level     = max(0, 0, 2) + 1 = 3
```

能否合并由 `MergeTreeDataMergerMutator`、`MergeSelectorApplier`、具体 `MergeSelector` 和 `MergePredicate` 共同决定。核心约束包括：

- 只能合并同一个分区内的 Part；不同分区永远不会合并。
- 候选 Part 必须是可合并的 active Part，不能已经被其他后台任务占用。
- 对复制表，`MergePredicate` 会检查两个候选 Part 的 `block number` 区间之间是否存在尚未提交的 `committing_blocks` 或复制队列中还没处理完的虚拟 Part。如果中间可能还有编号会出现，就不能先把两边合并到一起。
- 合并选择器会根据 Part 数量、大小、`level`、TTL、磁盘空间、后台池状态和相关设置挑选一段合适的 Part。
- `merge_max_block_size` 和 `merge_max_block_size_bytes` 只控制合并执行过程中一次从源 Part 读入内存的行数/字节数；它们影响内存占用和吞吐，不决定 Part 名称中的 `min_block` / `max_block`。

合并完成后，新 Part 的覆盖范围是源 Part 覆盖范围的并集。源 Part 不会立刻从磁盘消失，而是先变成 `Outdated`，等没有查询再持有引用后由清理线程删除。

### Part Block 与 `_block_number` / `_block_offset`

`_block_number` 和 `_block_offset` 是可持久化的虚拟列，和 Part 名称中的 `block number` 相关，但粒度不同：

- Part 名称里的 `min_block` / `max_block` 描述整个 Part 覆盖的提交编号范围。
- `_block_number` 描述每一行最初来自哪个提交编号。
- `_block_offset` 描述一行在该提交编号内的相对位置。

零级 Part 在插入写临时文件时还不知道最终 `block number`，所以依赖 `_block_number` 的提交顺序投影不会在插入阶段写出，而是在后续合并阶段生成。合并时，行来自多个源 Part，`_block_number` / `_block_offset` 会随行一起保留，因此合并结果虽然是一个更大的 Part，仍然可以追踪每行的原始提交顺序。

```
合并前：
202401_12_12_0: (_block_number = 12, _block_offset = 0..999)
202401_13_13_0: (_block_number = 13, _block_offset = 0..499)

合并后：
202401_12_13_1:
    行仍保留各自的 `_block_number` / `_block_offset`
    Part 名称只表示它覆盖 block 12..13
```

### 清理

`MergeTreeCleanupThread` 定期执行：
1. 删除不再被任何查询引用的 `Outdated` Part
2. 清理过期的临时目录
3. 无工作时使用指数退避

---

## 8. 分区和排序

### 分区

`MergeTreePartition` 存储每个 Part 的分区键值。不同分区的 Part 永远不会合并。分区表达式在插入时评估，决定 Part 属于哪个分区目录。

分区可以是：
- 自定义表达式（如 `toYYYYMM(date)`）
- 遗留格式：6 位 `YYYYMM`

每个分区维护自己的块号序列和合并层级。

### 排序

Part 内的数据按 **排序键**（sorting key）排序，排序键可以和主键不同。排序键定义行的物理顺序。

写入时，`mergeBlock` 使用排序键派生的排序描述对块进行 `stableSort`。

- **主键**：用于稀疏索引，控制索引裁剪
- **排序键**：控制物理排列顺序
- 通常主键是排序键的前缀，这样索引支持高效的范围扫描

---

## 9. 复制（DataPartsExchange）

### 架构

`ReplicatedMergeTree` 使用两个组件进行副本同步：

**Service**（`DataPartsExchange::Service`）：一个 `InterserverIOEndpoint`，通过 HTTP 向其他副本提供 Part 数据。当其他副本请求一个 Part 时：
1. `findPart(name)` 在本地定位 Part
2. `sendPartFromDisk` 流式传输所有 Part 文件（数据、marks、索引、校验和、投影）

**Fetcher**（`DataPartsExchange::Fetcher`）：从其他副本下载 Part：
1. `fetchSelectedPart` 向源副本的 Service 发起 HTTP 请求
2. `downloadPartToDisk` 接收流，在本地磁盘重建文件，验证校验和
3. 支持通过 `ThrottlerPtr` 限速
4. 支持 S3 零拷贝复制

### 副本同步流程

```
副本 A 写入新 Part
    │
    ▼
ZooKeeper 日志条目创建
    │
    ▼
副本 B/C 在复制队列中看到日志条目
    │
    ▼
本地没有该 Part → Fetcher 从拥有它的副本下载
    │
    ▼
验证校验和 → 激活 Part
```

---

## 10. 后台操作

### BackgroundJobsAssignee

每个 MergeTree 表最多有三个后台调度器：

| 类型 | 职责 |
|------|------|
| **DataProcessing** | 合并、变异、从复制队列 fetch |
| **Moving** | 在存储卷/磁盘间移动 Part |
| **Streaming** | 流式处理相关的后台工作 |

### 调度机制

每个调度器有一个 `BackgroundSchedulePoolTaskHolder`，在后台调度池中运行 `threadFunc`。函数委托给 `IBackgroundOperation::scheduleDataProcessingJob`。如果没找到工作，调用 `postpone` 使用指数退避（最大 `task_sleep_seconds_when_no_work_max`，默认 600 秒）。如果找到工作，调用 `trigger` 立即重新调度。

### 后台执行器（MergeTreeBackgroundExecutor）

任务提交到专门的线程池执行器：

| 执行器 | 运行的任务 |
|--------|-----------|
| **MergeMutateExecutor** | `MergeTask`（合并）和 `MutateTask`（变异） |
| **FetchesExecutor** | Part fetch 任务 |
| **MovesExecutor** | Part 移动任务 |
| **CommonExecutor** | 其他后台工作 |

### 清理线程（MergeTreeCleanupThread）

独立调度运行。删除 `Outdated` Part 和过期的 `tmp_` 目录。使用 `iterate` 处理一批，返回"完成工作量"指标控制调度频率。

### 其他后台任务

- **TTL 合并**：`TTLMergeSelector` 识别有过期 TTL 的 Part，触发合并删除过期数据
- **重压缩**：基于 TTL 的重压缩合并
- **Part 检查**：`PartCheckThread` 验证 Part 完整性
- **去重日志**：`MergeTreeDeduplicationLog` 维护块 ID 用于去重

---

## 磁盘布局总览图

```
<storage_root>/
├── all/                              ← 分区目录
│   ├── all_1_1_0/                    ← 0 级 Part（单次插入）
│   │   ├── checksums.txt             ← 文件校验和
│   │   ├── columns.txt               ← Schema
│   │   ├── columns_substreams.txt    ← 列子流顺序
│   │   ├── count.txt                 ← 行数
│   │   ├── primary.idx               ← 稀疏主键索引
│   │   ├── minmax_date.idx           ← 分区 MinMax 索引
│   │   ├── col1.bin                  ← 列数据（压缩）
│   │   ├── col1.mrk2                 ← 列 Marks
│   │   ├── col2.bin
│   │   ├── col2.mrk2
│   │   ├── skp_idx_bf.idx2           ← Bloom Filter 二级索引
│   │   ├── skp_idx_bf.mrk2
│   │   ├── serialization.json        ← 序列化信息
│   │   ├── default_compression_codec.txt
│   │   ├── metadata_version.txt
│   │   ├── ttl.txt                   ← TTL 信息
│   │   └── uuid.txt
│   │
│   ├── all_1_5_2/                    ← 2 级 Part（合并 1..5 的结果）
│   │   └── ... (相同结构)
│   │
│   └── tmp_merge_all_1_5_2/          ← 临时合并目录
│       └── ...
│
├── detached/                          ← 分离/损坏的 Part
└── tmp/                               ← 临时插入 Part
```

---

## Part 文件格式细节

这一节补充“Part 目录中的文件”里各类文件的保存内容结构。实际磁盘格式会随版本演进，下面按当前代码路径描述核心结构和读取含义。

### `columns.txt`

`columns.txt` 是文本格式的列清单，来自 `NamesAndTypesList::writeText`。它只记录这个 `Part` 里真实存在的列名和类型，不记录默认表达式、物化表达式、codec、TTL 等表级元数据。

伪格式：

```text
columns format version: 1
<columns_count> columns:
`<column_name_0>` <type_name_0>
`<column_name_1>` <type_name_1>
...
```

示例：

```text
columns format version: 1
3 columns:
`tenant_id` UInt64
`event_time` DateTime64(3)
`payload` String
```

读取 `Part` 时，`columns.txt` 用来确认这个 `Part` 的物理列集合。查询需要的缺失列会再结合当前表元数据，用默认值或 `ALTER` 转换补齐。

### `columns_substreams.txt`

`columns_substreams.txt` 是文本格式的“列到物理子流”映射，来自 `ColumnsSubstreams::writeText`。它是复杂类型、动态子列、Compact 带子流 marks 时定位文件名和子流顺序的依据。

伪格式：

```text
columns substreams version: 1
<columns_count> columns:
<substreams_count> substreams for column `<column_name>`:
	<substream_file_name_0>
	<substream_file_name_1>
...
```

示例：

```text
columns substreams version: 1
2 columns:
1 substreams for column `id`:
	id
3 substreams for column `arr`:
	arr.size0
	arr.null
	arr
```

对于 Wide `Part`，这些子流名通常直接对应 `<substream>.bin` 和 `<substream>.mrk2`。对于 Compact `Part`，它们对应 `data.mrk4` 里的子流位置序号。

### `checksums.txt`

`checksums.txt` 是 `Part` 文件集合的完整性清单。当前完整格式写入头部 `checksums format version: 4`，后面是压缩过的二进制 payload。payload 结构来自 `MergeTreeDataPartChecksums::write`：

```text
checksums format version: 4\n
<compressed payload>
```

payload 解压后是：

```text
<files_count as VarUInt>
repeat files_count:
  <file_name as binary string>
  <file_size as VarUInt>
  <file_hash as UInt128 little-endian>
  <is_compressed as bool>
  if is_compressed:
    <uncompressed_size as VarUInt>
    <uncompressed_hash as UInt128 little-endian>
```

这里的 `is_compressed` 表示校验和条目是否额外保存未压缩数据的大小和哈希，不等于“这个文件名看起来是否是压缩数据”。`.bin`、压缩的 `primary.cidx` 等会保存未压缩侧信息；普通文本元数据文件通常只保存文件大小和文件哈希。

复制、attach、`CHECK TABLE`、fetch 后校验都会依赖它。ZooKeeper 中的轻量级校验和是 `MinimalisticDataPartChecksums`，保存聚合哈希，不替代本地 `checksums.txt` 的逐文件清单。

### `count.txt`

`count.txt` 是文本格式的一行整数，保存 `Part` 总行数。

```text
<rows_count>
```

它用于快速得到 `Part` 行数，避免为了统计行数读取列数据。`Compact` `Part` 也要求它存在。

### `primary.idx` 与 `primary.cidx`

主键索引文件保存每个主键列在每个 mark 起点处的值。未压缩时文件名是 `primary.idx`；启用 `compress_primary_key` 时文件名是 `primary.cidx`，外层使用 ClickHouse 压缩流，解压后的逻辑内容相同。

逻辑结构：

```text
repeat marks_count:
  repeat primary_key_columns_count:
    <value serialized by column type serialization>
optional final mark row:
  repeat primary_key_columns_count:
    <last row key value>
```

例如主键是 `(tenant_id UInt64, event_time DateTime)`，则每个索引行依次写入 `tenant_id` 的二进制值和 `event_time` 的二进制值。它不保存 mark 编号、行号或偏移；这些信息由 `MergeTreeIndexGranularity` 和 marks 文件共同提供。

```
primary.cidx 解压后的逻辑序列
  mark 0: key column 0, key column 1, ...
  mark 1: key column 0, key column 1, ...
  mark 2: key column 0, key column 1, ...
```

查询时，`KeyCondition` 在内存里的主键列数组上判断 mark range；真正跳到数据文件的位置仍要靠 `<column>.mrk2` 或 `data.mrk3`。

### `minmax_*.idx`

`minmax_*.idx` 是每个被 MinMax 跟踪的列一个文件。默认主要用于分区键相关列，也可以由设置扩展到更多列。文件名里的列名可能经过转义或哈希缩短。

每个文件只保存这个 `Part` 在该列上的最小值和最大值：

```text
<min value serialized by column type serialization>
<max value serialized by column type serialization>
```

它是 `Part` 级别索引，不是 granule 级别索引。查询裁剪时，如果条件和 `[min, max]` 超矩形不相交，整个 `Part` 可以跳过。

### `<column-or-substream>.bin`

Wide `Part` 中，`.bin` 是列子流的压缩数据文件。它不是单纯的“一个列一个连续未压缩数组”，而是若干 ClickHouse 压缩块拼接而成。

逻辑结构：

```text
compressed block 0
compressed block 1
compressed block 2
...
```

每个压缩块内部保存该子流的一段序列化字节。对于固定长度类型，解压后可以按类型宽度读取；对于 `String`、`Array`、`Nullable`、`LowCardinality` 等类型，要通过对应 `ISerialization` 的子流协议解释。

```
UInt64 column:
  .bin 解压后: [u64][u64][u64]...

String column:
  .bin 解压后: [varUInt length][bytes][varUInt length][bytes]...

Array(UInt64):
  arr.size0.bin 解压后: [offset][offset][offset]...
  arr.bin       解压后: [u64 element][u64 element]...
```

`.bin` 文件本身不保存“第 N 行从哪里开始”的行号索引。定位由 marks 文件提供，行级边界由序列化协议和 granule 行数恢复。

### `<column-or-substream>.mrk2`

Wide 自适应 granularity 的 marks 文件通常是 `.mrk2`。它按 mark 顺序保存该子流在 `.bin` 文件里的位置。

逻辑结构：

```text
repeat marks_count:
  <offset_in_compressed_file as UInt64 little-endian>
  <offset_in_decompressed_block as UInt64 little-endian>
  <rows_in_granule as UInt64 little-endian>
```

含义：

- `offset_in_compressed_file`：目标压缩块在 `.bin` 文件中的起始偏移。
- `offset_in_decompressed_block`：解压该块后，目标数据在未压缩块内的偏移。
- `rows_in_granule`：从该 mark 开始这个 granule 覆盖的行数，自适应 granularity 下可能小于默认 `index_granularity`。

`.mrk2` 不保存 byte length。读取器从 mark 定位到起点后，根据要读取的行数调用对应列类型的反序列化逻辑；固定长度类型按宽度消费，变长类型按长度前缀、offsets 或 null map 等子流协议消费。若目标行数跨过当前压缩块，底层压缩 buffer 会继续读下一个压缩块。

非自适应旧格式 `.mrk` 没有 `rows_in_granule`；压缩 marks 变体使用 `.cmrk*`，逻辑字段仍是这些定位信息。

### `data.bin` 与 `data.mrk3` / `data.mrk4`

Compact `Part` 把所有列写入一个 `data.bin`，按 granule 顺序组织。每个 granule 内再按列或子流顺序写入。

```text
data.bin
  granule 0:
    column/substream 0 bytes
    column/substream 1 bytes
    ...
  granule 1:
    column/substream 0 bytes
    column/substream 1 bytes
    ...
```

`data.mrk3` 或 `data.mrk4` 保存每个 granule 中每个列或子流的位置。没有子流 marks 时，位置数量等于列数；有 `columns_substreams.txt` 并启用子流 marks 时，位置数量等于总子流数。

逻辑结构：

```text
repeat marks_count:
  repeat columns_or_substreams_count:
    <offset_in_compressed_file as UInt64 little-endian>
    <offset_in_decompressed_block as UInt64 little-endian>
  <rows_in_granule as UInt64 little-endian>
```

这就是为什么 Compact 适合小 `Part`：它把大量小列文件合并成一个数据文件和一个 marks 文件。但读取少量列时，定位仍发生在同一个 `data.bin` 上。

### `skp_idx_<name>.idx2` 与 `skp_idx_<name>.mrk2`

Skip index 文件和列数据类似，也有数据文件和 marks 文件。默认一个索引一个数据子流，文件名通常是 `skp_idx_<name>.idx2`；部分索引类型，如 text index，可以有多个子流和不同后缀。

一个 `.idx2` 条目不是对应一行，而是对应一个 **索引 granule**。索引 granule 的序号和数据 mark range 的关系由索引定义里的 `GRANULARITY` 决定：

```text
index_granule_no = 0 -> data marks [0, GRANULARITY)
index_granule_no = 1 -> data marks [GRANULARITY, 2 * GRANULARITY)
index_granule_no = 2 -> data marks [2 * GRANULARITY, 3 * GRANULARITY)
...
```

如果表的 `index_granularity = 8192` 且 skip index `GRANULARITY = 4`，那么一个 `.idx2` 条目通常覆盖约 `32768` 行。自适应 granularity 下，行数可能变化，但 mark 映射关系仍按 mark 计数。

`.mrk2` 的定位结构和普通 Wide marks 类似：

```text
repeat index_granules_count:
  <offset_in_compressed_file>
  <offset_in_decompressed_block>
  <rows marker / compatibility granularity>
```

`.idx2` 的 payload 取决于索引类型：

| 索引类型 | `.idx2` 中的逻辑内容 |
|----------|----------------------|
| `minmax` | 每个索引 granule 的每个表达式列写入 min、max |
| `set` | 去重后的值集合，超过限制时记录为空/不可用状态 |
| `bloom_filter` | Bloom filter 参数和位数组 |
| `ngrambf_v1` / `tokenbf_v1` | token 或 n-gram 的 Bloom filter 位数组 |
| text index | dictionary、postings 等多个子流 |

Skip index 的粒度通常是 `index_granularity * skip_index_granularity` 行。查询时先用主键得到候选 mark range，再加载这些范围对应的 skip index granule 做二次剪枝。

剪枝结果不会直接返回行号，而是返回保留下来的数据 mark ranges。被判定为“不可能匹配”的索引 granule 会把对应数据 marks 从读取计划中删除；被判定为“可能匹配”的 granule 继续进入真实列读取和 `WHERE` 过滤。

### `serialization.json`

`serialization.json` 是 JSON 格式的序列化信息，只在需要持久化非默认序列化策略时写入。典型内容包括某列是否使用稀疏序列化、`LowCardinality` 相关序列化统计、动态类型或子列相关信息。

它不保存列数据本身，而是告诉读取器如何解释 `.bin` 或 `data.bin` 中的字节流。没有这个文件时，读取器按类型的默认序列化方式处理。

### `default_compression_codec.txt`

`default_compression_codec.txt` 是一行文本，保存该 `Part` 写入时使用的默认压缩 codec 描述，例如 `CODEC(LZ4)` 或包含参数的 codec 表达式。

这个文件描述默认 codec；具体到列或子流时，数据文件中的压缩块头和序列化信息仍会参与实际读写。它主要用于后续合并、变异、统计文件写入等场景保持 codec 选择。

### `metadata_version.txt`

`metadata_version.txt` 是文本格式的元数据版本号。它把 `Part` 和表元数据演进关联起来，用于判断 `Part` 是在哪个 metadata version 下写出的。部分存储可以把 metadata version 存在 `Part` attributes 里，此时新 `Part` 可能不写这个文件。

```text
<metadata_version>
```

### `uuid.txt`

`uuid.txt` 保存 `Part` 的唯一标识符，文本形式写入。它用于区分同名或迁移场景下的物理 `Part`，也参与复制和对象存储相关路径的唯一性管理。

```text
<uuid>
```

### `ttl.txt`

`ttl.txt` 是带文本头的 JSON 风格内容，来自 `MergeTreeDataPartTTLInfos::write`。只有存在非空 TTL 信息时才写入。

伪格式：

```text
ttl format version: 1
{
  "columns": [
    {"name": "<column>", "min": <unix_time>, "max": <unix_time>, "finished": 0|1}
  ],
  "table": {"min": <unix_time>, "max": <unix_time>, "finished": 0|1},
  "rows_where": [...],
  "moves": [...],
  "recompression": [...],
  "group_by": [...]
}
```

它保存的是这个 `Part` 内 TTL 表达式计算出的时间范围。后台 TTL merge selector 用这些范围决定是否需要触发删除、移动、重压缩或聚合类 TTL 合并。

### `version.txt`

`version.txt` 保存事务和可见性相关元数据。它不是列数据的一部分，主要用于支持事务/MVCC 路径下对 `Part` 版本的判断。普通非事务路径下，并不是所有 `Part` 都需要这个文件。

### 统计文件

统计文件保存列级统计信息，例如 min、max、ndv、直方图等。它们用于估算和优化，不改变 `.bin` 的真实数据内容。统计可能按 packed 方式写入，也可能在 Wide 存储里拆成独立压缩文件；具体文件名和结构取决于统计类型和写入策略。

可以把它们理解为“优化器辅助元数据”：缺失统计信息不应影响数据正确读取，但会影响估算质量和某些优化选择。

---

## 性能关键设计总结

MergeTree 引擎通过以下机制实现高性能：

1. **列式存储** — 只读取查询需要的列
2. **稀疏主键索引** — 内存占用极小（每 ~8192 行一个条目），支持 O(log N) 范围查找
3. **Mark 定位** — O(1) 在压缩文件中随机访问，无需完整解压
4. **自适应粒度** — granule 大小根据数据特征自动调整
5. **多级过滤** — 分区裁剪 → MinMax → 主键 → 二级索引 → 数据读取，层层缩小范围
6. **协作式合并** — 协程式合并在块之间让出，避免阻塞
7. **垂直合并** — 大列独立合并，降低内存峰值
8. **预取和缓存** — Mark 缓存、子流缓存、异步预取
