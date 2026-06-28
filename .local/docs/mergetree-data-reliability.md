# `MergeTree` 数据可靠性、事务、快照与多副本机制分析

本文基于当前代码树分析 `MergeTree` 与 `ReplicatedMergeTree` 的写入提交、恢复、事务、snapshot/MVCC 以及多副本一致性实现。核心结论先列在前面：

- 普通 `MergeTree` 的 `INSERT INTO` 主路径不是传统数据库里“先写行级 `WAL`，再刷数据页”的模式。它写的是完整 immutable data part：先写临时 part 目录，生成列文件、mark、索引、校验和与元数据，然后 `finalize`，最后通过 rename 和内存状态切换把 part 放入 active working set。
- 普通 `MergeTree` 向用户返回成功的时刻，是相关临时 part 已经 `finalize`，`commitPart` 成功，`MergeTreeData::Transaction::commit` 把 part 标记为 `Active` 并更新工作集之后。默认情况下，这不等价于每次都做磁盘 `fsync`；强掉电持久性取决于 `fsync_after_insert` 与 `fsync_part_directory`。
- 机器异常重启后的恢复依赖 part 目录的原子可见性、`checksums.txt`、part 名称区间和启动扫描。完整 rename 成正式 part 且校验通过的 part 会重新加载；仍是 `tmp_`/`tmp_insert_` 等临时目录的未提交 part 会被清理。
- `MergeTree` 有事务/MVCC 基础设施，但它是受限且实验性质的能力。源码注释明确说明，当前不支持跨多 host 或涉及 `ReplicatedMergeTree` 表的事务。事务的全局提交顺序由 Keeper/ZooKeeper 中 `/clickhouse/txn/log/csn-` 顺序节点提供，`CSN` 同时作为 commit timestamp 和 snapshot version。
- snapshot 的实现不是复制数据，而是基于 part 级多版本：每个 part 有 `creation_tid`/`creation_csn` 与 `removal_tid`/`removal_csn`，读请求按 snapshot `CSN` 过滤 `Active`/`Outdated` parts。
- `ReplicatedMergeTree` 的一致性不是数据副本之间直接跑 `Raft`。它使用 Keeper/ZooKeeper 存储复制元数据、全局复制日志、去重节点和 quorum 状态；ClickHouse Keeper 自身通过 `NuRaft` 实现一致性，ZooKeeper 则通过 ZooKeeper 自身的一致性协议提供线性化元数据操作。

## 全局架构与模块关系

下图给出 `MergeTree` 数据可靠性涉及的核心模块、它们之间的调用与数据流，方便后文按模块对应展开。

```
                                客户端 INSERT / SELECT / TRANSACTION
                                              │
   ┌──────────────────────────────────────────┼──────────────────────────────────────────┐
   │                                          ▼                                          │
   │                 ┌────────────────────────────────────────────────┐                  │
   │                 │   StorageMergeTree / StorageReplicatedMergeTree │                  │
   │                 └────────────────────────────────────────────────┘                  │
   │   写路径                  │              │              │           读路径           │
   │   ┌──────────────────────┘              │              └──────────────────────┐    │
   │   ▼                                     ▼                                     ▼    │
   │ ┌──────────────────┐         ┌──────────────────────┐            ┌────────────────┐ │
   │ │  MergeTreeSink   │         │ ReplicatedMergeTreeSink│            │ StorageSnapshot│ │
   │ └──────┬───────────┘         └──────┬───────────────┘            └──────┬─────────┘ │
   │        │ writeNewTempPart           │ writeNewTempPart                  │           │
   │        ▼                            ▼                                   ▼           │
   │ ┌──────────────────┐         ┌──────────────────────┐            ┌────────────────┐ │
   │ │MergeTreeDataWriter│         │MergeTreeDataWriter   │            │getVisibleData..│ │
   │ │+MergedBlockOutput │         │+EphemeralLockInZK    │            │filterVisible..│ │
   │ └──────┬───────────┘         └──────┬───────────────┘            └──────┬─────────┘ │
   │        │ part 文件落盘              │ block_number / Keeper ops          │ isVisible │
   │        ▼                            ▼                                   ▼           │
   │ ┌────────────────────────────────────────────────────────────────────────────┐     │
   │ │                            MergeTreeData                                    │     │
   │ │  data_parts_indexes (multi_index)  +  Transaction (precommitted_parts)      │     │
   │ │  renameTempPartAndAdd  ►  preparePartForCommit  ►  Transaction::commit      │     │
   │ │  Temporary → PreActive → Active → Outdated → Deleting → DeleteOnDestroy     │     │
   │ └────────────┬────────────────────────────────┬──────────────────────────────┘     │
   │              │ 事务部分                       │ 副本部分                            │
   │              ▼                                ▼                                    │
   │ ┌──────────────────────────┐      ┌──────────────────────────────┐                │
   │ │ MergeTreeTransaction     │      │ ReplicatedMergeTreeQueue     │                │
   │ │  creating_parts          │      │  log_pointer / queue / parts │                │
   │ │  removing_parts          │      │  virtual_parts/future_parts  │                │
   │ │  beforeCommit→afterCommit│      │  pullLogsToQueue / processEntry              │
   │ └────────┬─────────────────┘      └──────────────┬───────────────┘                │
   │          │ assigned_csn                          │ GET_PART / MERGE_PARTS / ...   │
   │          ▼                                       ▼                                │
   │ ┌──────────────────────────┐      ┌──────────────────────────────┐                │
   │ │ VersionMetadata          │      │ DataPartsExchange (HTTP)     │                │
   │ │  txn_version.txt         │      │  fetchSelectedPart           │                │
   │ │  creation_csn/removal_csn│      └──────────────┬───────────────┘                │
   │ │  isVisible / canBeRemoved│                     │                                │
   │ └──────────┬───────────────┘                     │                                │
   │            │                                     │                                │
   │            ▼                                     ▼                                │
   │ ┌─────────────────────────────────────────────────────────────────────────┐      │
   │ │                              TransactionLog                              │      │
   │ │   running_list / snapshots_in_use / tid_to_csn / latest_snapshot         │      │
   │ │   beginTransaction / commitTransaction / getCSN / getOldestSnapshot      │      │
   │ └───────────────────────────────────┬─────────────────────────────────────┘      │
   │                                     │                                            │
   │                                     ▼                                            │
   │ ┌─────────────────────────────────────────────────────────────────────────┐      │
   │ │                          Keeper / ZooKeeper                              │      │
   │ │  /clickhouse/txn/log/csn-N    (事务 CSN 顺序节点 — 全局事务序)            │      │
   │ │  /clickhouse/tables/<sh>/<t>/log/log-N (复制日志 — 复制全局序)            │      │
   │ │  /...,/replicas, /block_numbers, /blocks, /quorum, /mutations            │      │
   │ │  → ClickHouse Keeper 用 NuRaft 维持一致；ZooKeeper 用 ZAB                 │      │
   │ └─────────────────────────────────────────────────────────────────────────┘      │
   └─────────────────────────────────────────────────────────────────────────────────────┘

   后台线程：
   ┌────────────────────────┬────────────────────────┬─────────────────────────────┐
   │ MergeTreeCleanupThread │ ReplicatedMergeTreeRestartingThread │PartCheckThread │
   │ 清理 tmp_/老 part      │ session 重建 + 启动恢复  │ 损坏 part 自愈              │
   └────────────────────────┴────────────────────────┴─────────────────────────────┘
```

三大子系统的职责边界：

- **`MergeTreeData` 核心**：管理 part 集合（`data_parts_indexes`）、part 状态机和 `MergeTreeData::Transaction`。是写路径和读路径的公共枢纽。
- **事务子系统**：`MergeTreeTransaction` + `TransactionLog` + `VersionMetadata` 三层。`TransactionLog` 提供全局 `CSN` 序，`VersionMetadata` 把 `CSN` 落到每个 part 的 `txn_version.txt` 文件。
- **副本子系统**：`ReplicatedMergeTreeSink` 把本地 part 提交点与 Keeper `multi` 提交点合一；`ReplicatedMergeTreeQueue` 让每副本异步追赶全局复制日志；`PartCheckThread`/`RestartingThread` 提供损坏自愈和重启恢复。

读写关键不变量：part 不可变；提交点是"完整 part + 状态机切换 + 元数据持久化"三者的原子组合；可见性由 part 状态 + `VersionInfo::isVisible` + Keeper 共同决定。

## 普通 `MergeTree` 的 `INSERT` 写入链路

### 从 `Storage` 到 `Sink`

普通 `MergeTree` 写入入口会创建 `MergeTreeSink`。`MergeTreeSink::consume` 的逻辑可以概括为：

1. 从输入 `Chunk` 构造 `Block`。
2. 按 partition 拆成 `BlocksWithPartition`，源码在 `MergeTreeDataWriter::splitBlockIntoParts`。
3. 对每个 partition 调用 `writeNewTempPart`，实际落到 `MergeTreeDataWriter::writeTempPart`。
4. 写好的临时 part 先放入 `delayed_chunk`；随后 `finishDelayedChunk` 负责完成提交。

这里有一个细节：`MergeTreeSink` 采用延迟提交流水线，写下一批数据前会提交上一批 `delayed_chunk`。如果是 streaming `INSERT` 且 `input_format_max_block_wait_ms` 非零，或者物化视图依赖要求同步提交，则会立即 `finishDelayedChunk`。

### 临时 part 写到什么程度

`MergeTreeDataWriter::writeTempPartImpl` 是临时 part 的主要写入函数。它做的事情包括：

- 生成临时目录名，常见前缀是 `tmp_insert_`。
- 根据 partition key 计算 `partition_id`，为新 part 构造初始 `MergeTreePartInfo`。
- 如有 sorting key，计算排序表达式并对 block 生成 permutation。
- 根据表设置选择 part 类型、index granularity 和压缩 codec。
- 创建 `MergedBlockOutputStream` 写入列数据、mark、primary/minmax/skipping index、projection 等文件。
- 调用 `finalizeIndexGranularity` 与 `finalizePartAsync`。
- 把输出流与异步 finalizer 放入 `MergeTreeTemporaryPart`。

这个阶段 part 仍然是临时 part。它的文件已经写入临时目录，但还没有以最终 part 名称出现在 active 数据集里。若进程在这个阶段异常退出，启动恢复时这类 `tmp_` 目录不会被当成正式数据。

### `finalize` 与 `commitPart`

`MergeTreeSink::finishDelayedChunk` 对每个 partition 执行：

1. `partition.temp_part->finalize` 等待所有 stream finalizer 完成。
2. 从 `DeduplicationInfo` 取去重 hash。
3. 调用 `MergeTreeSink::commitPart`。
4. 成功后预热缓存、写 `PartLog`，并触发后台 merge。

`MergeTreeSink::commitPart` 的普通表逻辑是：

- 创建 `MergeTreeData::Transaction transaction(storage, context->getCurrentTransaction().get())`。
- 持有 parts lock。
- 调用 `storage.fillNewPartName` 填最终 part 名。
- 如果启用了非副本表去重，则向 `storage.getDeduplicationLog` 写 block id，冲突时返回 conflict 并让上层过滤重复行后重写 temp part。
- 调用 `storage.renameTempPartAndAdd(part, transaction, lock, false)`。
- 调用 `transaction.commit(lock)`。

`renameTempPartAndAdd` 内部会进入 `renameTempPartAndReplaceImpl`，通过 `preparePartForCommit` 把 part 从 `Temporary` 切成 `PreActive`，必要时执行 `part->renameTo(part->name, true)`，并把 part 插入 `data_parts_indexes`。随后 `MergeTreeData::Transaction::commit` 会：

- 对 precommitted part 调用底层 `DataPartStorage` 的 `commitTransaction`。
- 调用 `MergeTreeTransaction::addNewPartAndRemoveCovered` 写 part 版本信息。
- 在 `NOEXCEPT_SCOPE` 中把 covered parts 置为 `Outdated`，把新 part 置为 `Active`。
- 更新数据量、行数、part 数和 serialization hints。
- 清理 transaction 临时状态，唤醒等待 `PreActive` part 的线程。

因此，普通 `MergeTree` 的同步 `INSERT` 返回成功时，至少满足：

- part 文件已经通过 `MergedBlockOutputStream` 写完并 `finalize`；
- 临时 part 已经改名为正式 part 名；
- 新 part 已经进入 `data_parts_indexes`，状态为 `Active`；
- 被覆盖的旧 part 已经转为 `Outdated`；
- 如果在显式 `MergeTree` transaction 中，还写入了对应的 part version metadata。

### 深入：`delayed_chunk` 流水线机制

`MergeTreeSink::consume` 并不在写完 part 之后立即 commit，而是把 `temp_part` 挂到 `delayed_chunk` 上，等下一次 `consume` 调用开始时才 `finishDelayedChunk` 前一批。源码位置 `src/Storages/MergeTree/MergeTreeSink.cpp:98-236`。

```cpp
// consume()
auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(
    std::move(block), max_parts_per_block, metadata_snapshot, context);

for (auto & current_block : part_blocks)
{
    auto result = current_deduplication_info->deduplicateSelf(...);
    temp_part = writeNewTempPart(current_block);

    if (total_streams + current_streams > max_insert_delayed_streams_for_parallel_write)
        finishDelayedChunk();  // 流量太大时强制刷上一批

    partitions.emplace_back(/* 填入 delayed_chunk */);
}
// 本批挂入 delayed_chunk，不立即 commit
```

延迟的目的是**写盘-提交流水线并行化**：当前 `consume` 在做磁盘 I/O 与压缩时，上一批 `temp_part` 的 `Finalizer`（fsync 等异步收尾）可以并发执行，吞吐显著提升。两种情况退化为同步提交：

- `input_format_max_block_wait_ms != 0`：streaming `INSERT`，需要 part 立即可见；
- `synchronously_commit_part_for_dependent_views == true`：存在物化视图依赖时，必须先 commit part 再触发 MV，避免可见性竞争。

源码引用：`MergeTreeSink.cpp:229-233`。

### 深入：`writeTempPartImpl` 的临时目录生成

`MergeTreeDataWriter::writeTempPartImpl` 是本地 part 落盘的主函数，源码位置 `src/Storages/MergeTree/MergeTreeDataWriter.cpp:654-1005`。临时目录名约定（第 704-709 行）：

```cpp
std::string temp_prefix = "tmp_insert_";
const auto & temp_postfix = data.getPostfixForTempInsertName();
if (!temp_postfix.empty())
    temp_prefix += temp_postfix + "_";
std::string part_dir = temp_prefix + part_name;
// 例：tmp_insert_all_1_1_0、tmp_insert_abc_all_2_2_0
```

`writeTempPartImpl` 的关键步骤：

1. 计算 `minmax_idx`，构造 `new_part_info`（`partition_id` + 临时 `block_number` + level）。
2. `getTemporaryPartDirectoryHolder(part_dir)`：把目录名登记到 `temporary_parts` 集合，防止后台清理误删。
3. 应用 sorting key 表达式生成 permutation；若 `optimize_on_insert=true`，做一次内存级 merge。
4. `volume->reserve(expected_size)` 预留磁盘空间。
5. `data_part_storage->beginTransaction()` 开磁盘事务（rename 失败可以回滚）。
6. 若 `fsync_part_directory=true`，立刻获取目录 `sync_guard`（目录条目同步落盘）。
7. `MergedBlockOutputStream::writeWithPermutation` 写列文件、mark、index、minmax、projection。
8. `finalizePartAsync(..., fsync_after_insert)` 构造 `Finalizer::Impl`，**只构造不执行**。
9. 将 `(stream, finalizer)` 收纳进 `temp_part->streams`。

注意 `finalizePartAsync` 返回的 `Finalizer` 暂未执行；实际的 fsync 在后续 `temp_part->finalize()` 时 `Finalizer::Impl::finish` 内完成（`MergedBlockOutputStream.cpp:146-167`）。这就是延迟流水线的物理基础。

`fsync_after_insert` 控制文件内容 fsync，`fsync_part_directory` 控制目录条目 fsync，二者相互独立，决定了"机器掉电后已返回成功的 part 是否仍存活"。

### 深入：`renameTempPartAndAdd` 与 part 状态机

提交链路的核心调用栈：

```
MergeTreeSink::commitPart
  └─ storage.renameTempPartAndAdd(part, transaction, lock, false)        // MergeTreeData.cpp:5479
       └─ renameTempPartAndReplaceImpl                                    // MergeTreeData.cpp:5397
            ├─ part->assertState({Temporary})                             // 当前必须是临时态
            ├─ checkPartPartition / checkPartDuplicate
            ├─ getPartHierarchy(part->info, Active, lock)                 // 查找覆盖关系
            └─ preparePartForCommit(part, transaction, lock, need_rename) // MergeTreeData.cpp:5332
                 ├─ part->is_temp = false
                 ├─ part->setState(DataPartState::PreActive)
                 ├─ part->renameTo(part->name, true)                       // tmp_ → 正式名
                 ├─ data_parts_indexes.insert(part)                        // 进入索引，但 SELECT 仍看不到
                 └─ transaction.addPart(part, need_rename)
  └─ transaction.commit(lock)                                              // MergeTreeData.cpp:8684
       ├─ part->getDataPartStorage().commitTransaction()                   // 磁盘事务真正生效
       ├─ MergeTreeTransaction::addNewPartAndRemoveCovered                 // 写版本链
       ├─ NOEXCEPT_SCOPE
       │    ├─ modifyPartState(part, Active)                               // 对外可见
       │    └─ for 覆盖的 part: modifyPartState(Outdated)
       ├─ 更新 字节/行 统计
       └─ preactive_parts_cv.notify_all()                                  // 唤醒等待 PreActive 的 SELECT
```

完整的 part 状态机：

```
   ┌──────────────┐
   │  Temporary   │  writeTempPart 完成，目录是 tmp_insert_xxx，未注册到 data_parts_indexes
   └──────┬───────┘
          │ preparePartForCommit
          │ ── 目录改名为正式名；插入 data_parts_indexes（但状态仍非 Active）
          ▼
   ┌──────────────┐
   │  PreActive   │  对 SELECT 仍不可见；事务尚未 commit
   └──┬────────┬──┘
      │        │
      │        └────────── Transaction::rollback ──────────────┐
      │                                                        ▼
      │                                                ┌────────────────┐
      │                                                │   Outdated     │  ← 也可能因新 part 覆盖到达
      │                                                └───────┬────────┘
      │                                                        │ clearOldPartsFromFilesystem
      │ Transaction::commit                                    ▼
      │   ── modifyPartState(Active)                   ┌────────────────┐
      │                                                │   Deleting     │  ← 物理删除前的过渡态
      ▼                                                └───────┬────────┘
   ┌──────────────┐  被新 part 覆盖（INSERT/MERGE）            │ rollbackDeletingParts
   │   Active     │  ── modifyPartState(covered, Outdated) ────│ 可能回到 Outdated
   └──┬───────────┘                                            ▼
      │ swapActivePart                                  └─►(物理删除)
      │ ── 磁盘迁移完成后旧副本进入此态等待析构
      ▼
   ┌──────────────────┐
   │ DeleteOnDestroy  │
   └──────────────────┘
```

状态转换触发函数对照表（`src/Storages/MergeTree/MergeTreeDataPartState.h` 定义枚举）：

| 转换 | 触发函数 | 说明 |
|---|---|---|
| `Temporary` → `PreActive` | `preparePartForCommit` | rename 临时目录、插入 `data_parts_indexes` |
| `PreActive` → `Active` | `Transaction::commit` 中 `modifyPartState(Active)` | 对外可见 |
| `PreActive` → `Outdated`/`Temporary` | `Transaction::rollback` | 还原 |
| `Active` → `Outdated` | `Transaction::commit`（对被覆盖 part） | 新 part 覆盖旧 part |
| `Outdated` → `Deleting` | `clearOldPartsFromFilesystem` | 准备物理删除 |
| `Deleting` → `Outdated` | `rollbackDeletingParts` | 删除失败回退 |
| `Active` → `DeleteOnDestroy` | `swapActivePart`（磁盘迁移） | 等待析构释放 |

### 深入：`data_parts_indexes` 多索引结构

`data_parts_indexes` 是 `boost::multi_index_container`，定义在 `src/Storages/MergeTree/MergeTreeData.h:1523-1549`：

```cpp
using DataPartsIndexes = boost::multi_index_container<DataPartPtr,
    boost::multi_index::indexed_by<
        boost::multi_index::ordered_unique<     // 索引 1：按 PartInfo 排序
            boost::multi_index::tag<TagByInfo>,
            global_fun<..., dataPartPtrToInfo>>,
        boost::multi_index::ordered_unique<     // 索引 2：按 (State, PartInfo)
            boost::multi_index::tag<TagByStateAndInfo>,
            global_fun<..., dataPartPtrToStateAndInfo>>
    >>;
```

`data_parts_by_info` 让 `getPartIfExists(name)` O(log n)；`data_parts_by_state_and_info` 让 `getDataPartsStateRange(Active)` 按状态连续遍历，避免每次都扫全表。

### 深入：锁层级与并发集合

写路径上的关键锁与互斥集合：

| 锁 / 集合 | 作用范围 | 用途 |
|---|---|---|
| `parts_lock` (SharedMutex) | 整张表 | 修改 `data_parts_indexes` 与 part 状态机的总入口 |
| `operation_with_data_parts_mutex` | 独立 | 互斥 DROP/ATTACH 与 merge |
| `clear_old_temporary_directories_mutex` | 独立 | `clearOldTemporaryDirectories` `try_lock`，失败直接跳过 |
| `currently_submerging_emerging_mutex` | 独立 | 标记正在 merge 的大 part，防止重复调度 |
| `temporary_parts` 集合 | `MergeTreeData` 成员 | 标记仍在写入的 `tmp_*` 目录，保护它们不被清理 |
| `currently_merging_mutating_parts` | 独立 | 标记正在 merge/mutate 的源 part，避免并发删除 |

`parts_lock` 内嵌套关系是单向的（不会反过来获取其他锁），代码注释见 `MergeTreeData.h:1068`：

```
parts_lock (write)
  └─► data_part_storage.commitTransaction()
  └─► data_parts_indexes 修改
  └─► modifyPartState()
  └─► sizes_lock（统计聚合）
```

### 深入：启动恢复时的 part-loading-tree

`MergeTreeData::loadDataParts`（`MergeTreeData.cpp:2241` 起）的关键设计：**把磁盘上所有可解析的 part 名构造成层级树，只取顶层节点作为候选**。

```
扫描各磁盘表目录
  ├─ 跳过 startsWith(name, "tmp")
  ├─ 跳过 "format_version.txt" / "detached"
  ├─ 解析 part name → MergeTreePartInfo
  └─ 收集 parts_to_load_by_disk

PartLoadingTree::build
  ├─ 节点按 partition_id 分组
  ├─ 同 partition 内按 (min_block, max_block, level) 排序
  └─ 父子关系：父 part 完全覆盖子 part 区间
                                 │
                                 ▼
PartLoadingTree::traverse
  └─ 只取每条父链的顶层节点（最大覆盖 part）作为加载候选
                                 │
                                 ▼
loadDataPartsFromDisk（并发）
  └─ 每个候选 part：
      ├─ 读 columns / checksums / primary_index
      ├─ 校验 checksums.txt
      ├─ 加载 VersionMetadata（如果存在 txn_version.txt）
      └─ preparePartForCommit → Transaction::commit
```

这套机制让恢复时**不需要重放 WAL**：覆盖关系直接由 part name 编码，被覆盖的旧 part 一律忽略，只加载真正的最新版本。

### 深入：临时目录清理时机

启动时清理：`StorageMergeTree.cpp:233`

```cpp
clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_", "tmp-fetch_"});
// lifetime=0 表示立即清理所有匹配前缀
```

运行时清理：`MergeTreeCleanupThread` 周期调用，默认阈值是 `temporary_directories_lifetime`（通常 86400 秒）。清理逻辑（`MergeTreeData.cpp:3239-3256`）做了双重保护：

1. 检查目录 mtime 是否超过 deadline；
2. 检查 `temporary_parts` 集合中是否还登记着该目录名（活跃 INSERT/merge 持有）；

只清理同时满足"过期"+"无活跃持有者"的孤儿目录，避免误删进行中的写入。

## 这里的 `WAL` 到底是什么

用户常会把“写成功点”理解成 `WAL` 持久化点。对 `MergeTree` 来说需要区分三类日志/元数据：

| 场景 | 是否是传统 `WAL` | 用途 |
| --- | --- | --- |
| 普通 `MergeTree` `INSERT` | 不是 | 通过完整 part 目录、原子 rename、checksum 和启动扫描保证恢复。 |
| `TransactionLog` | 不是行级 redo log | 事务提交序列日志，存在 Keeper/ZooKeeper 中，用 `CSN` 判定事务提交与 snapshot 可见性。 |
| `ReplicatedMergeTree` `/log` | 不是本地 redo log | 复制日志，告诉所有副本要拉取/生成哪个 part。 |

也就是说，普通 `MergeTree` 没有“每行先 append 到 `WAL`，成功返回后后台刷 page”的流程。它的最小提交单元是 immutable part。`WAL` 语义更多出现在事务和复制元数据层，而不是普通单机 part 写入的数据恢复层。

## `fsync` 与掉电持久性

`MergeTreeSettings.cpp` 定义了两个关键设置：

- `fsync_after_insert`：对每个 inserted part 做 `fsync`。注释明确说会显著降低 insert 性能，尤其不推荐用于 wide parts。
- `fsync_part_directory`：part 操作后对 part directory 做 `fsync`。

`MergeTreeDataWriter::writeTempPartImpl` 会把 `fsync_after_insert` 传给 `finalizePartAsync`；创建临时目录后，如果 `fsync_part_directory` 为真，会获取 directory sync guard。事务版本元数据写入 `txn_version.txt.tmp` 后会 `finalize`、`sync`，再根据 `fsync_part_directory` 处理目录同步，并 `replaceFile` 成 `txn_version.txt`。

因此需要分清两种故障：

- 进程异常退出但 OS 和磁盘状态正常：已 rename 且校验通过的正式 part 可恢复；临时目录会被清理。
- 机器掉电或存储层丢失 page cache：如果没有开启相关 `fsync`，已经返回成功的数据仍可能受底层文件系统/磁盘写回策略影响。开启 `fsync_after_insert` 与 `fsync_part_directory` 后，ClickHouse 会牺牲写入性能换取更强的本地持久性。

## 启动恢复：哪些数据会回来

`MergeTreeData` 构造与 `startup` 路径负责恢复：

- 构造期间调用 `initializeDirectoriesAndFormatVersion`，随后 `loadDataParts`。
- `loadDataParts` 遍历 storage policy 下所有 disk 的表目录，跳过 `tmp` 前缀、`format_version.txt` 和 `detached`。
- 能按 part name 解析的目录会进入 part loading tree。树结构用于处理覆盖关系，只把顶层“最大覆盖”的 part 作为 active 候选加载。
- 每个 part 会读取 columns、checksums、marks、index、TTL、transaction version metadata 等，校验通过后进入内存工作集。
- `startup` 中调用 `clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_", "tmp-fetch_"})`，清理旧临时目录。

恢复时的基本判定是：

| 重启前状态 | 重启后结果 |
| --- | --- |
| 仍在 `tmp_insert_...` 临时目录 | 不作为正式数据加载，后续清理。 |
| 已 rename 成正式 part，但未进入内存 `Active` 即进程退出 | 启动扫描会发现正式 part，校验通过后加载。 |
| 正式 part 缺文件或 checksum 不一致 | 不能作为正常 active part 使用，副本表会尝试从其他副本恢复。 |
| 事务 part 只有 `txn_version.txt.tmp` | 视为未完成事务创建，构造 `RolledBackCSN`，后续可清理。 |
| 老 part 没有事务版本文件 | 按 `NonTransactionalTID`/`NonTransactionalCSN` 处理，兼容历史数据。 |

这里的可靠性来自“完整 part 才可见”。`MergeTree` 不需要重放半个 part 的数据操作，因为半成品不会进入 active 集合。

## 事务支持：支持什么，不支持什么

源码中的事务类包括：

- `TransactionLog`
- `MergeTreeTransaction`
- `VersionMetadata`
- `VersionInfo`
- `MergeTreeTransactionHolder`

但是它不是所有 ClickHouse 表引擎的完整分布式事务。`TransactionLog.h` 注释明确写到：涉及多个 host 或 `ReplicatedMergeTree` 表的事务目前不支持。`MergeTreeTransaction::checkNotOrdinaryDatabase` 还要求表不属于 deprecated `Ordinary` database，因为事务依赖表 UUID。

从代码结构看，事务主要支持在单 host 上对支持事务的 `MergeTree` part 进行 MVCC 管理。它的粒度是 part，不是行级 undo/redo。

### 深入：特殊 `TID` / `CSN` 常量语义

`src/Common/TransactionID.h:36-114` 定义了一组"保留 ID"，它们承载着事务子系统的状态机语义，理解它们是看懂可见性判断和恢复逻辑的前提：

| 常量 | 数值 | 语义 |
|---|---|---|
| `UnknownCSN` | 0 | 事务尚未提交（含进行中、回滚后未持久化） |
| `NonTransactionalCSN` | 1 | 非事务写入（如普通 `INSERT` 或老数据） |
| `CommittingCSN` | 2 | 事务正处于 `beforeCommit` 到 `afterCommit` 的窗口期 |
| `EverythingVisibleCSN` | 3 | 调试用：等价于"看到一切" |
| `MaxReservedCSN` | 32 | Keeper 顺序节点初始化时快进到此，避免冲突 |
| `RolledBackCSN` | `UINT64_MAX` | 事务已回滚；`creation_csn` 写入此值代表 part 必删 |
| `NonTransactionalTID` | `{1, 1, Nil}` | 非事务操作的 TID 占位 |
| `DummyTID` | `{1, 2, Nil}` | `loadMetadata` 检测到 `txn_version.txt.tmp` 残留时设置的占位 |

### `TID` 与 `CSN`

事务 ID 是 `TransactionID`，由三部分构成：

- `start_csn`：事务开始时看到的最新 `CSN`，也是该事务的 snapshot。
- `local_tid`：本机递增 ID。
- `host_id`：持久化 server UUID，用于跨 server 唯一性。

提交序号是 `CSN`。`TransactionLog::commitTransaction` 会向 Keeper/ZooKeeper 的 `transaction_log.zookeeper_path` 下创建顺序节点：

```text
/clickhouse/txn/log/csn-
```

节点内容是事务 `TID`。创建成功后，顺序节点的编号就是该事务的 commit timestamp。这个创建顺序节点的 `multi` 是事务提交点。

### 事务提交流程

`TransactionLog::commitTransaction` 的关键步骤：

1. 调用 `txn->beforeCommit`，把事务状态从 `UnknownCSN` CAS 成 `CommittingCSN`，并等待相关 mutation。
2. 收集 `txn->getRequestsOnCommit`。
3. 非只读事务追加创建 `/clickhouse/txn/log/csn-` 顺序节点的请求。
4. 使用 Keeper/ZooKeeper `multi` 提交这些请求。
5. 从创建出的 znode 名称解析 `allocated_csn`。
6. 调用 `finalizeCommittedTransaction`。

`MergeTreeTransaction::afterCommit` 会在把事务原子状态切成最终 `CSN` 前，先持久化所有 part 的版本元数据：

- 新 part 写 `creation_csn = assigned_csn`。
- 被删除/覆盖的 part 写 `removal_csn = assigned_csn`。
- mutation 写 mutation `CSN`。

源码注释说明了故障恢复语义：如果进程在这个循环中退出，Keeper/ZooKeeper 里的 `CSN` znode 加上 part 磁盘上的 `creation_tid`/`removal_tid` 足以在重启后恢复缺失的 `creation_csn`/`removal_csn`。

### 深入：事务提交时序图

下图展示一次完整事务提交时客户端、`MergeTreeTransaction`、`TransactionLog`、Keeper 与 part 磁盘元数据之间的交互：

```
Client    MergeTreeTransaction    TransactionLog        Keeper            磁盘(txn_version.txt)
  │              │                       │                 │                       │
  │ COMMIT       │                       │                 │                       │
  ├──────────────▶                       │                 │                       │
  │       beforeCommit()                 │                 │                       │
  │       ├ 等待 mutation                 │                 │                       │
  │       └ CAS(csn: Unknown → Committing)                 │                       │
  │              │                       │                 │                       │
  │              ├──commitTransaction───▶│                 │                       │
  │              │              requests = [..., makeCreate("/txn/log/csn-", TID, Sequential)]
  │              │                       │── multi ───────▶│                       │
  │              │                       │◀── csn-0012345 ─│                       │
  │              │              allocated_csn = 12345      │                       │
  │              │                       │                 │                       │
  │              │              finalizeCommittedTransaction(txn, 12345)            │
  │              │                       │                 │                       │
  │       afterCommit(12345)             │                 │                       │
  │       ├ for each creating_part:      │                 │                       │
  │       │   setAndStoreCreationCSN(12345) ────────────────────────────────────────▶
  │       │   (写 txn_version.txt.tmp → fsync → rename)    │                       │
  │       ├ for each removing_part:      │                 │                       │
  │       │   setAndStoreRemovalCSN(12345) ─────────────────────────────────────────▶
  │       └ CAS(csn: Committing → 12345) │                 │                       │
  │              │              snapshots_in_use.erase()   │                       │
  │              │              running_list.erase(tid_hash)│                      │
  │              │              waitStateChange 唤醒等待者  │                       │
  │◀─── OK ──────│                       │                 │                       │
```

关键设计意图：

1. **`CSN` 由 Keeper 顺序节点决定**，没有单点 leader 也无需依赖外部时钟，顺序节点的递增编号天然是全局单调时间戳。
2. **CSN 先在 Keeper 落盘，再写 part 元数据**：即使中途进程崩溃，重启后只要 `creation_tid` 能在 Keeper 中找到对应的 `csn-` 节点，就可以"补写" `creation_csn`。Keeper 是事务可见性的真相源（source of truth），磁盘上的 `creation_csn` 只是缓存。
3. **`csn` 字段最后才置位**：`afterCommit` 把所有 part 的版本元数据写完之后才把事务对象的 `csn` 原子翻转，这样别的事务在 `waitStateChange` 唤醒后看到 `csn` 已生效，就一定能看到对应 part 的可见性元数据。

### 深入：`TransactionLog` 内部结构

`TransactionLog` 维护若干内存结构（`src/Interpreters/TransactionLog.h`）：

| 字段 | 类型 | 用途 |
|---|---|---|
| `latest_snapshot` | `std::atomic<CSN>` | 当前最新已提交 `CSN`；新事务以此为 `start_csn` |
| `running_list` | `tid_hash → MergeTreeTransactionPtr` | 活跃事务表 |
| `snapshots_in_use` | 有序 `std::list<CSN>` | 当前所有活跃快照；`front()` 是最老快照（GC 依据） |
| `tid_to_csn` | `tid_hash → CSN` | 已提交事务的 CSN 缓存（避免每次都查 Keeper） |
| `unknown_state_list` | vector | `multi` 返回硬件错误时进入此列表，后台线程 `tryFinalizeUnknownStateTransactions` 二次确认 |

`beginTransaction` 的伪代码（`TransactionLog.cpp:396-414`）：

```cpp
std::lock_guard lock{running_list_mutex};
CSN snapshot = latest_snapshot.load();              // 1. 读取最新 CSN 作为快照基线
LocalTID ltid = 1 + local_tid_counter.fetch_add(1); // 2. 本地原子递增，无 Keeper 调用
auto snapshot_lock = snapshots_in_use.insert(snapshots_in_use.end(), snapshot);
                                                    // 3. 登记快照到有序链表
txn = std::make_shared<MergeTreeTransaction>(snapshot, ltid, ServerUUID::get(), snapshot_lock);
running_list.try_emplace(txn->tid.getHash(), txn);
```

注意 `beginTransaction` **不与 Keeper 通信**，这让事务开启极轻量。只有 commit 才会创建 `csn-` znode，因此短事务的 Keeper 压力很小。

`loadLogFromZooKeeper`（`TransactionLog.cpp:177-218`）首次启动时：

1. 若 `/clickhouse/txn/log` 不存在，原子创建 `tail_ptr` 节点 + 一批占位 `csn-` 节点把序号快进到 `MaxReservedCSN`；
2. `getChildren` 列出所有 `csn-XXXXXXXXXX` 节点；
3. 批量 `get` 每个节点内容（序列化的 TID），构造 `tid_to_csn` 缓存；
4. `tail_ptr` 记录"安全删除线"：所有 `start_csn < tail_ptr` 的 TID 已确保把 `creation_csn` 写到磁盘，对应的 `csn-` znode 可以从 Keeper 删除以省空间。

### 回滚流程

`TransactionLog::rollbackTransaction` 调用 `txn->rollback`。`MergeTreeTransaction::rollback` 会：

- 把事务 `csn` 从 `UnknownCSN` CAS 成 `RolledBackCSN`。
- 对创建中的 part 做删除/移出 active。
- 对被移除 part 解除删除锁或恢复状态。
- 处理 rollback 时需要发给 Keeper/ZooKeeper 的请求。

源码注释同样强调：如果在 rollback 过程中进程异常退出，启动时会看到该 `TID` 没有提交记录，从而丢弃这些未提交修改。

## Snapshot/MVCC 是怎么实现的

`MergeTree` 的 snapshot 是 part 级 MVCC。它不是复制一份数据文件，也不是维护行级 undo chain，而是用 part 版本信息判定可见性。

### part 版本文件

事务版本信息存储在 part 目录下的 `txn_version.txt`，临时文件名是 `txn_version.txt.tmp`。`VersionInfo` 里包含：

```text
creation_tid
creation_csn
removal_tid
removal_csn
storing_version
```

含义如下：

| 字段 | 含义 |
| --- | --- |
| `creation_tid` | 创建该 part 的事务 ID。 |
| `creation_csn` | 创建事务提交后的 `CSN`；未知时为 `UnknownCSN`。 |
| `removal_tid` | 删除/覆盖该 part 的事务 ID。 |
| `removal_csn` | 删除/覆盖事务提交后的 `CSN`。 |
| `storing_version` | 版本文件乐观更新用的持久化版本号。 |

`VersionMetadataOnDisk::storeInfoToDataPartStorage` 的写法是先创建 `txn_version.txt.tmp`，写入内容并 `sync`，然后 `replaceFile` 为 `txn_version.txt`。启动加载时如果发现 tmp 文件但没有正式文件，会把该 part 视为创建未完成，构造 `DummyTID` 与 `RolledBackCSN`。

### 可见性判断

读路径通过 `MergeTreeData::getVisibleDataPartsVector` 获取可见 part：

- 非事务读：直接取 `Active` parts。
- 事务读：取 `Active` 和 `Outdated` parts，然后调用 `filterVisibleDataParts`。

`filterVisibleDataParts` 调用 `part->version->isVisible(snapshot_version, current_tid)`。`VersionInfo::isVisible` 的规则可以简化为：

- 如果 part 创建 `CSN` 大于 snapshot，不可见。
- 如果 part 删除 `CSN` 小于等于 snapshot，不可见。
- 如果当前事务正在删除它，不可见。
- 如果 part 创建 `CSN` 小于等于 snapshot 且未删除，可见。
- 如果当前事务创建它，可见。
- 如果只有 `TID` 还没有 `CSN`，则查 `TransactionLog::getCSN`，确认该事务是否已经提交。

这就是 `MergeTree` 的 snapshot isolation 基础：读事务固定一个 snapshot `CSN`，写事务通过 part 元数据发布创建/删除版本。

### 深入：`isVisible` 决策树

`VersionInfo::isVisible(snapshot, current_tid)` 的完整判定逻辑（`VersionInfo.cpp:138-192`、`VersionMetadata.cpp:46-101`）可以画成下面的决策树。**优化目标是：尽可能不查 Keeper**——快路径直接在内存里判断，只有 CSN 还没回填到内存时才回落到 `TransactionLog::getCSN` 慢路径。

```
isVisible(snapshot, current_tid)
│
├─ removal_tid.isNonTransactional()          ──► false（已被非事务删除）
│
├─ current_tid.isNonTransactional()
│      └─ return removal_tid.isEmpty()       ── 无事务上下文的快路径
│
├─【快 false 路径】
│   ├─ creation_csn != 0 && snapshot < creation_csn         ──► false
│   ├─ removal_csn   != 0 && removal_csn <= snapshot         ──► false
│   └─ removal_tid   == current_tid                          ──► false
│
├─【快 true 路径】
│   ├─ creation_csn != 0 && creation_csn <= snapshot && removal_tid.isEmpty()              ──► true
│   ├─ creation_csn != 0 && creation_csn <= snapshot && removal_csn != 0 && snapshot < removal_csn ──► true
│   └─ creation_tid == current_tid                                                          ──► true
│
└─【慢路径：CSN 尚未回填到内存】
    ├─ snapshot <= creation_tid.start_csn                    ──► false
    │      （提交 CSN 必然 > start_csn；snapshot 比 start_csn 还老 ⇒ 必不可见）
    ├─ csn = TransactionLog::getCSN(creation_tid)
    │      └─ 0 ──► false（创建事务尚未提交）
    ├─ rcsn = TransactionLog::getCSN(removal_tid)
    └─ return creation_csn <= snapshot && (rcsn == 0 || snapshot < rcsn)
```

### 深入：可见性联动表

不同 `(creation_csn, removal_csn, removal_tid)` 状态对应的可见性结论（设当前快照 `S`）：

| `creation_csn` | `removal_csn` | `removal_tid` | 对 `snapshot = S` 的可见性 |
|---|---|---|---|
| 0 (Unknown) | 0 | Empty | 不可见（创建事务未提交） |
| 0 (Unknown) | 0 | = `current_tid` | 不可见（自己正在删 it） |
| > S | 0 | Empty | 不可见（创建太新） |
| ≤ S | 0 | Empty | **可见** |
| ≤ S | 0 | ≠ `current_tid` | 走慢路径查 Keeper |
| ≤ S | > S | ≠ `current_tid` | **可见**（删除在快照之后） |
| ≤ S | ≤ S | any | 不可见（删除在快照前已提交） |
| `RolledBackCSN` | any | any | 不可见（`canBeRemoved` 立即为 true） |
| `NonTransactionalCSN` | `NonTransactionalCSN` | `NonTransactionalTID` | 不可见（已被非事务删除） |

### 深入：`storing_version` 乐观并发控制

`VersionMetadataOnDisk::storeInfoUnlocked`（`VersionMetadataOnDisk.cpp:186-244`）使用 `storing_version` 字段防止两个事务并发修改同一 part 的版本元数据造成 ABA 问题：

1. 读取磁盘（或 `deferred_persist_info`）拿到当前 `storing_version`；
2. 若 `new_info.storing_version != expected` ⇒ 返回 `TOO_OLD_VERSION`（`std::unexpected`）；
3. 否则递增 `storing_version`，写入新内容；
4. 上层 `updateInfoWithRefreshDataThenStoreAndSetMetadata` 最多重试 20 次。

这是经典的乐观并发：竞争者一次只能有一个赢，输的重读重试。

### 深入：`tryLockRemovalTID` 排他锁

`removal_tid` 实际上是 part 上的"删除排他锁"，由原子变量 `removal_tid_lock_hash` 实现（`VersionMetadataOnDisk.cpp:107-168`）：

```cpp
std::atomic<TIDHash> removal_tid_lock_hash = 0;
// 加锁：CAS(0 → tid.getHash())
bool locked = removal_tid_lock_hash.compare_exchange_strong(
    expected_removal_lock_value /*=0*/,
    removal_lock_value /*=tid.getHash()*/);
// 解锁：CAS(tid.getHash() → 0)
```

它是无锁的排他写者锁，确保同一个 part 同一时刻只能被一个事务"标记为待删除"。

### `StorageSnapshot`

查询执行层拿到的是 `StorageSnapshot`。`MergeTreeData::createStorageSnapshot` 会：

- 调用 `getPossiblySharedVisibleDataPartsRanges` 取当前 query 可见 parts。
- 构造 `SnapshotData`，保存 `parts`、`mutations_snapshot` 和 metadata version。
- 如果启用 `enable_shared_storage_snapshot_in_query`，同一个 query context 内可以缓存并共享 storage snapshot。

因此 `snapshot` 在执行层表现为一个稳定的 `parts` 集合加 mutation 视图。后台 merge/mutation 可以继续运行，但当前 query 已拿到的 part 视图不会因为新 part 提交而改变。

### 旧版本何时能删除

`VersionMetadata::canBeRemoved` 用 `TransactionLog::getOldestSnapshot` 判断过期 part 是否仍可能被运行中的事务看到。只有满足以下条件之一才可以安全删除：

- 创建事务已经回滚。
- 非事务 removal。
- 删除 `CSN` 已经小于等于当前最老 snapshot。
- 创建/删除的事务状态可由 `TransactionLog` 确认，且没有运行中事务还需要旧版本。

这避免了读事务还持有旧 snapshot 时，后台清理把它需要的 `Outdated` part 删除。

### 深入：`txn_version.txt` 写入步骤

`VersionMetadataOnDisk::storeInfoToDataPartStorage`（`VersionMetadataOnDisk.cpp:323-348`）严格按 crash-safe 模式写入：

```
1. data_part_storage.createFile("txn_version.txt.tmp")
2. writeFile("txn_version.txt.tmp") ← new_info.writeToBuffer()
3. buf->finalize(); buf->sync()                ← 对 .tmp 文件 fsync
4. [可选] getDirectorySyncGuard()              ← 目录 fsync（fsync_part_directory）
5. replaceFile("txn_version.txt.tmp", "txn_version.txt")  ← 原子 rename
```

`loadMetadata`（`VersionMetadataOnDisk.cpp:48-93`）处理三种磁盘状态：

| 情形 | 条件 | 处理 |
|---|---|---|
| 非事务 part | `txn_version.txt` 不存在且 `.tmp` 不存在 | `creation_tid = NonTransactionalTID`，`creation_csn = NonTransactionalCSN` |
| 写入中崩溃 | `txn_version.txt` 不存在，但 `.tmp` 存在 | 视为回滚：`creation_tid = DummyTID`，`creation_csn = RolledBackCSN`；删除 `.tmp` |
| 正常 | `txn_version.txt` 存在 | 直接反序列化加载 |

情形 2 中 part 会被 `MergeTreeData::loadDataPart` 标记为 `Outdated` 并清理，避免半成品参与可见性判定。

## 数据可靠性的几个关键不变量

`MergeTree` 的可靠性来自几个代码层不变量：

1. part 不可变：写入、merge、mutation 都生成新 part，不原地修改旧 part 的列文件。
2. part 名称编码区间：part name 中的 partition、min block、max block、level、mutation version 表示覆盖关系。
3. 临时目录不可见：`tmp_`/`tmp_insert_` 前缀目录不会作为 active part 加载。
4. 正式 part 有 checksum：读和启动加载会校验 part 元数据与 checksums。
5. 状态机分层：`Temporary` -> `PreActive` -> `Active`，旧 part 转 `Outdated`，清理再删除。
6. 提交先完整写 part，再切可见性：读路径只读 active/snapshot-visible part，不读半成品。
7. 事务版本元数据可恢复：即使 `creation_csn`/`removal_csn` 还没写完，只要 Keeper/ZooKeeper 中有事务提交 `CSN`，重启后仍可推导可见性。

## `ReplicatedMergeTree` 写入成功点

`ReplicatedMergeTree` 的写入入口是 `ReplicatedMergeTreeSink`。它同样先写本地临时 part，但提交路径多了 Keeper/ZooKeeper 元数据事务。

### 本地 part 与 Keeper `multi`

`ReplicatedMergeTreeSink::commitPart` 的核心流程：

1. 通过 `storage.allocateBlockNumber` 在 `/block_numbers/<partition>/block-` 下创建 ephemeral sequential lock，获得分区内 block number。
2. 用 block number 改写 part 的 `min_block`/`max_block` 和最终 part name。
3. 构造 Keeper/ZooKeeper `ops`：
   - 创建 `/log/log-` 顺序节点，内容是 `ReplicatedMergeTreeLogEntryData`。
   - 删除 block number lock。
   - 如果启用 quorum，创建 `/quorum/status` 或 `/quorum/parallel/<part>`。
   - 创建 shared data lock。
   - 调用 `storage.getCommitPartOps` 创建去重 block id 节点和 `/replicas/<replica>/parts/<part>` 元数据。
4. 本地调用 `storage.renameTempPartAndAdd(part, transaction, lock, true)`，此时 rename 纳入 `MergeTreeData::Transaction`。
5. 调用 `transaction.renameParts`，先把本地 part 文件 rename 到正式目录。
6. 调用 Keeper/ZooKeeper `tryMultiNoThrow(ops)`。
7. 如果 `multi` 成功，设置 `new_part_was_committed_to_zookeeper_after_rename_on_disk`，再 `transaction.commit`，让本地 part 变为 `Active`。

因此，非 quorum 的副本表 `INSERT` 返回成功，通常意味着：

- 本地 part 已经完整写完并正式 rename；
- Keeper/ZooKeeper 中已经原子提交复制日志、去重节点和本 replica 的 part 元数据；
- 本地 part 已经提交为 `Active`；
- 后台任务会调度其他副本从复制日志拉取该 part。

### Keeper 状态未知时怎么处理

如果 Keeper/ZooKeeper `multi` 返回硬件/连接类错误，代码不会简单假设失败并删本地 part。它进入恢复分支：

- 重试检查 `/replicas/<replica>/parts/<part>` 是否存在。
- 如果存在，说明 Keeper 元数据已提交，继续本地 `transaction.commit`。
- 如果确认不存在，则回滚本地 rename，把 part 还原为 temporary，重新分配 block number 后重试。
- 如果重试耗尽仍无法确认，则本地提交 part，但调用 `enqueuePartForCheck` 安排后续自动检查；向用户抛 `UNKNOWN_STATUS_OF_INSERT`，提示状态未知。

这段逻辑解决的是”客户端没收到成功/失败，但 Keeper 可能已经提交”的典型分布式提交未知状态问题。去重节点也会帮助用户重试 `INSERT` 时避免重复数据。

### 深入：`commitPart` 的 `CommitRetryContext` 状态机

`ReplicatedMergeTreeSink::commitPart`（`ReplicatedMergeTreeSink.cpp:717`）用一个 `CommitRetryContext` 状态机驱动，分为四个状态：

```
                ┌─────────────────────────────┐
                │       LOCK_AND_COMMIT       │ ← 初始态
                └──────┬──────────┬───────────┘
       multi ZOK      │          │ 硬件错误
                       │          ├──────► retry / UNKNOWN_STATUS_OF_INSERT
                       │          │
                       │          │ ZNODEEXISTS on dedup path
                       │          ├──────────┐
                       │          │          ▼
                       ▼          │  ┌──────────────────┐
                ┌─────────────┐   │  │ RESOLVE_CONFLICTS│
                │   SUCCESS   │   │  └────────┬─────────┘
                └─────────────┘   │           │ 读 conflict 节点中的 part_name
                                  │           ▼
                                  │  ┌─────────────────────────────┐
                                  │  │ FILTER_CONFLICTS_AND_RETRY  │
                                  │  └──────────┬──────────────────┘
                                  │             │ 过滤掉重复行 → 重写 temp_part
                                  └─────────────┘ → 回到 LOCK_AND_COMMIT
```

核心阶段 `commit_new_part_stage`（`ReplicatedMergeTreeSink.cpp:863`）的步骤：

```
1. detectConflictsInAsyncBlockIDs           — 内存缓存预过滤（async insert 场景）
2. allocateBlockNumber                       — Keeper ephemeral sequential 拿 block_number
3. 用 block_number 改写 part->info.min_block/max_block 与 part name
4. 组装 Keeper multi ops（见下表）
5. renameTempPartAndAdd + transaction.renameParts()   — 本地磁盘重命名
6. zookeeper->tryMultiNoThrow(ops)                    — 1 个 RTT 原子提交
7. 成功：transaction.commit() + block_number_lock.assumeUnlocked()
   失败：进入 RESOLVE_CONFLICTS 或硬件错误分支
```

### 深入：`allocateBlockNumber` 与 `EphemeralLockInZooKeeper`

`StorageReplicatedMergeTree::allocateBlockNumber`（`StorageReplicatedMergeTree.cpp:7479`）的关键代码：

```cpp
String block_numbers_path = fs::path(zookeeper_table_path) / “block_numbers”;
String partition_path     = fs::path(block_numbers_path) / partition_id;

// 确保 partition 节点存在
ops.push_back(zkutil::makeCheckRequest(replica_path + “/host”, -1)); // 防 DROP
ops.push_back(zkutil::makeCreateRequest(partition_path, “”, Persistent));
ops.push_back(zkutil::makeSetRequest(block_numbers_path, “”, -1));   // 触发 partition 版本变更

auto lock = createEphemeralLockInZooKeeper(
    partition_path + “/block-”,      // PersistentSequential 前缀
    zookeeper_path + “/temp”,
    zookeeper,
    zookeeper_block_id_paths,        // dedup 路径预检
    znode_data);
```

`createEphemeralLockInZooKeeper`（`EphemeralLockInZooKeeper.cpp:53`）做的事：

1. 若有去重路径，先把 `checkNotExists(/blocks/<dedup_hash>)` 操作加进同一个 `multi`；
2. 在 `block_numbers/<partition>/` 下 `createEphemeralSequential`，节点名是 `block-0000000123`；
3. 若 dedup 路径已存在 → `ZNODEEXISTS`，返回冲突路径，上层走 RESOLVE_CONFLICTS；
4. 成功则从路径末尾解析得到 block_number。

要点：**`block-` 是 ephemeral sequential 节点**——session 存活期间占用该 number；commit 成功时同一个 `multi` 内 `remove` 它（持久化释放）；session 异常断开则 Keeper 自动清理，该 block_number 被永久放弃（不会复用）。这就让 block_number 的分配天然具有崩溃恢复语义。

### 深入：Keeper `multi` 提交的完整 ops 列表

下表列出一次普通 quorum `INSERT` 在 `tryMultiNoThrow` 中包含的 op 序列：

| # | 类型 | 路径 | 作用 |
|---|---|---|---|
| 1 | `create` (PersistentSequential) | `/log/log-` | 创建复制日志条目，内容为 `GET_PART` |
| 2 | `remove` | `/block_numbers/<partition>/block-NNN` | 释放 block_number lock |
| 3 | `create` | `/quorum/status` 或 `/quorum/parallel/<part>` | quorum 状态节点（仅 quorum 模式） |
| 4 | `check` | `<replica>/is_active @ version` | 防止本副本中途丢失活跃状态 |
| 5 | `check` | `<replica>/host @ version` | 防止本副本被替换 |
| 6 | `create` | zero-copy shared lock | S3 场景的对象引用计数 |
| 7 | `create` (Persistent) | `/blocks/<dedup_hash>` | 去重记录，value = part name |
| 8 | `create` (Persistent) | `<replica>/parts/<part>[+/columns,/checksums]` | 副本 part 元数据 |

`get_commit_part_ops` 写法 `StorageReplicatedMergeTree.cpp:9688`：minimalistic 模式下 `<part>` 节点本身就携带 `ReplicatedMergeTreePartHeader`（含 columns 哈希 + checksums），无需额外子节点；否则会创建 `columns` 和 `checksums` 子节点。

### 深入：Keeper znode 树结构

```
/clickhouse/tables/<shard>/<table>/                  ← zookeeper_path
├── log/                                             ← 全局复制日志（PersistentSequential）
│   ├── log-0000000001                               ← GET_PART / MERGE_PARTS / ... 序列化
│   └── log-0000000002
├── replicas/
│   └── replica1/                                    ← replica_path
│       ├── is_active   (Ephemeral)                  ← 副本存活心跳
│       ├── host                                     ← interserver host:port
│       ├── log_pointer                              ← 已消费到的 /log 序号
│       ├── mutation_pointer                         ← 已应用到的 mutation znode
│       ├── is_lost                                  ← “0” 正常 / “1” 待克隆
│       ├── metadata_version                         ← schema 版本号
│       ├── min_unprocessed_insert_time              ← 监控用
│       ├── queue/                                   ← 本副本待执行队列
│       │   └── queue-0000000001                     ← 从 /log 复制过来
│       └── parts/                                   ← 本副本声明持有的 part
│           └── 20240101_1_1_0                       ← part header 或子节点
├── block_numbers/                                   ← 每分区 block_number 分配器
│   └── 20240101/
│       └── block-0000000001  (Ephemeral)            ← 占用中的 block_number
├── blocks/                                          ← sync insert dedup（新格式）
│   └── <unified_hash>                               ← value = part_name
├── async_blocks/                                    ← async insert dedup
├── deduplication_hashes/                            ← 统一 hash dedup
├── quorum/
│   ├── status                                       ← non-parallel quorum 状态（全局唯一）
│   ├── parallel/                                    ← parallel quorum 状态
│   │   └── 20240101_1_1_0
│   ├── last_part                                    ← 每分区最后达成 quorum 的 part
│   └── failed_parts/                                ← quorum 失败的 part
├── mutations/                                       ← mutation 定义（PersistentSequential）
│   └── 0000000001
├── temp/                                            ← block_number lock holder
│   ├── LEGACY_LOCK_INSERT
│   └── LEGACY_LOCK_OTHER
└── leader_election/                                 ← merge leader 选举
```

`/clickhouse/txn/log/csn-N` 与此树**并列**，是事务子系统专用、所有表共享的全局事务日志。

### 深入：`commitPart` 端到端时序图

```
Client     Sink (r1)          本地FS          Keeper            其他副本 (r2)
  │           │                  │              │                     │
  │ INSERT    │                  │              │                     │
  ├──────────▶│                  │              │                     │
  │           │ writeTempPart    │              │                     │
  │           ├─────────────────▶│              │                     │
  │           │ (tmp_ 目录写入)   │              │                     │
  │           │                  │              │                     │
  │           │ allocateBlockNumber (1 RTT)     │                     │
  │           │  multi[ checkNotExists(/blocks/H),                    │
  │           │         createEphemeral(/block_numbers/.../block-) ]  │
  │           ├──────────────────────────────▶│                       │
  │           │◀────── block-N ───────────────│                       │
  │           │                  │              │                     │
  │           │ renameTempPart   │              │                     │
  │           ├─────────────────▶│              │                     │
  │           │ (原子重命名磁盘)   │              │                     │
  │           │                  │              │                     │
  │           │ tryMulti (1 RTT)                │                     │
  │           │  ops=[ create /log/log-N (GET_PART),                  │
  │           │        remove /block_numbers/.../block-N,             │
  │           │        create /quorum/status,                          │
  │           │        check  is_active@v, host@v,                    │
  │           │        create /blocks/<hash>=part,                    │
  │           │        create <r1>/parts/<part> ]                     │
  │           ├──────────────────────────────▶│                       │
  │           │◀── ZOK ──────────────────────│                       │
  │           │ transaction.commit()           │                       │
  │           │ (part 进入 active 集合)         │                       │
  │           │                  │              │                     │
  │           │ [if quorum] waitForQuorum      │   pullLogsToQueue    │
  │           │                  │              │◀────────────────────│
  │           │                  │              │ /log/log-N          │
  │           │                  │              ├────────────────────▶│
  │           │                  │              │                     │ fetchSelectedPart
  │           │                  │              │                     │── HTTP POST ─▶ r1
  │           │                  │              │                     │◀── 数据流 ────
  │           │                  │              │                     │ commitPart
  │           │                  │              │   updateQuorum      │ (本地 + Keeper)
  │           │                  │              │◀────────────────────│
  │           │                  │              │ set /quorum/status (加入 r2)
  │           │                  │              │ 若 ≥ required → 删 /quorum/status
  │           │◀── watch event ──│              │                     │
  │           │ waitForQuorum 退出              │                     │
  │           │                  │              │                     │
  │◀── OK ────│                  │              │                     │
```

### 深入：`UNKNOWN_STATUS_OF_INSERT` 处理决策树

当 `tryMultiNoThrow` 返回硬件错误（连接断、超时），客户端无法分辨”Keeper 已提交但应答丢失”和”Keeper 未提交”。处理决策树（`ReplicatedMergeTreeSink.cpp:997-1136`）：

```
tryMulti 超时 / 连接断开
        │
        ▼
   retryLoop（zookeeper->setKeeper 重连）
        │
        ├─ exists(/quorum/failed_parts/<part>)
        │     └─ YES ──► TABLE_IS_READ_ONLY（Restarting Thread 接管清理）
        │
        ├─ exists(<replica>/parts/<part>)
        │     ├─ YES ──► 提交其实已成功
        │     │         transaction.commit() → SUCCESS
        │     │         （Keeper 元数据齐备，本地 part 也已 rename）
        │     │
        │     └─ NO  ──► multi 没有执行成功（要么 Keeper 没收到，要么 rollback 过）
        │               rollback 本地 rename → 重新进入下一次 retryLoop
        │
        └─ retryLoop 耗尽（insert_keeper_max_retries 次）
               actionAfterLastFailedRetry:
                 transaction.commit()           ← 保留本地 part，避免误删数据
                 enqueuePartForCheck(part, 300s) ← 兜底，后台延迟核对
                 抛 UNKNOWN_STATUS_OF_INSERT
                       │
                       ▼
                300 秒后 PartCheckThread:
                 ├─ Keeper 有此 part ──► OK，部分元数据补齐即可
                 └─ Keeper 无此 part ──► 删除本地 part（实际未写入）
```

这个设计的关键决策：**宁可保留本地 part 抛 `UNKNOWN_STATUS_OF_INSERT`，也不立即删除**。如果删除了，万一 Keeper 其实提交成功，副本数据就丢了；如果保留，最多是 part 处于”本地有、Keeper 无”的孤儿状态，由 `PartCheckThread` 异步纠正即可（同时去重 hash 也会防止用户重试时写出第二份）。

## 多副本一致性协议

`ReplicatedMergeTree` 的一致性基础是 Keeper/ZooKeeper 的线性化元数据操作。ClickHouse 自己不在数据副本之间直接实现共识协议：

- 使用 ClickHouse Keeper 时，Keeper server 通过 `NuRaft` 维护元数据状态机。
- 使用 Apache ZooKeeper 时，ClickHouse 依赖 ZooKeeper 提供的顺序节点、`multi` 原子操作、watch、session 和一致性语义。
- `ReplicatedMergeTree` 在 Keeper/ZooKeeper 上构建复制日志、part 元数据、block number、dedup block id、quorum 状态和每副本 queue。

### 全局复制日志

插入 part 时会创建：

```text
/<table_path>/log/log-
```

这是 persistent sequential 节点。日志内容是 `ReplicatedMergeTreeLogEntryData`，常见类型包括：

- `GET_PART`
- `ATTACH_PART`
- `MERGE_PARTS`
- `DROP_RANGE`
- `REPLACE_RANGE`
- `MUTATE_PART`
- `ALTER_METADATA`
- `DROP_PART`

对普通 insert，写入者创建 `GET_PART` log entry，并把 `source_replica` 设为当前 replica。其他副本看到这个 entry 后，从 source replica 或其他拥有该 part 的副本 fetch part。

### 每副本 queue

`ReplicatedMergeTreeQueue::pullLogsToQueue` 做日志分发：

1. 读取本 replica 的 `/log_pointer`。
2. 列出全局 `/log` 下从 pointer 开始的 entry。
3. 批量读取这些 log entry。
4. 用一次 Keeper/ZooKeeper `multi` 把它们复制到：

```text
/<table_path>/replicas/<replica>/queue/queue-
```

并更新 `/log_pointer`。

之后本地后台线程执行 queue entry。执行成功后从 `/queue` 删除对应节点。内存里的 `current_parts`、`virtual_parts`、`future_parts` 用于避免并发任务产生冲突 part。

### 深入：`ReplicatedMergeTreeQueue` 数据结构

`src/Storages/MergeTree/ReplicatedMergeTreeQueue.h:88-178` 中的关键字段：

| 字段 | 语义 |
|---|---|
| `current_parts` | 本副本磁盘上已就绪的 active part 集合（与 `data_parts_indexes` 中 `Active` 一致） |
| `virtual_parts` | `current_parts` + 队列中所有待执行 entry 的目标 part；用来判断 merge/mutate 的可行性 |
| `future_parts` | 正在后台执行中的 entry 目标 part（防止重复 schedule） |
| `mutations_by_partition` | `partition_id → block_number → MutationStatus`；用来快速查 part 还需要应用哪些 mutation |
| `mutation_pointer` | 已处理到的 mutation znode 名（持久化到 `<replica>/mutation_pointer`） |
| `queue` | 内存队列：从 `<replica>/queue` 加载，按 entry 顺序排列 |

### 深入：`selectEntryToProcess` 调度策略

`ReplicatedMergeTreeQueue.cpp:2114`：

```cpp
for (auto it = queue.begin(); it != queue.end(); ++it)
{
    if ((*it)->currently_executing) continue;
    if (shouldExecuteLogEntry(**it, (*it)->postpone_reason, ...))
    {
        entry = *it;
        queue.splice(queue.end(), queue, it);  // 移到队尾，给其他 entry 机会
        break;
    }
    ++(*it)->num_postponed;
    (*it)->last_postpone_time = time(nullptr);
}
```

`shouldExecuteLogEntry`（`ReplicatedMergeTreeQueue.cpp:1644`）的过滤条件：

- **指数退避**：`postpone_time = 1 << num_tries`，上限为 `max_postpone_time_for_failed_replicated_*_ms`；
- **future_parts 冲突**：目标 part 已在 `future_parts` 中则跳过，避免并发执行；
- **DROP 冲突**：与正在执行的 `DROP_RANGE`/`DROP_PART` 区间相交时跳过；
- **fetch 池容量**：`canExecuteFetch` 检查并发 fetch 数上限。

被跳过的 entry 会被 `splice` 到队尾，下一轮循环再考虑——这是一个简单的环形调度，避免单个慢 entry 阻塞队列前进。

### 深入：副本异步追赶完整流程

```
                                公共 /log/log-N
                                       │
                          pullLogsToQueue (周期触发 + watch)
                          (atomic: 拷贝条目 + 推进 log_pointer)
                                       │
                                       ▼
                       本副本 /queue/queue-XXXXXX（持久化）
                                       │
                          后台 task 线程 selectEntryToProcess
                                       │
                       ┌───────────────┴────────────────┐
                       │                                │
              currently_executing?              shouldExecute?
                   ──► skip                      ──► splice 到队尾（backoff）
                                                       │
                                                       ▼
                                       CurrentlyExecuting RAII 注册
                                       (future_parts.insert(target))
                                                       │
                                                       ▼
                                            executeLogEntry()
                       ┌──────────────┬─────────────┬────────────┐
                  GET_PART        MERGE_PARTS   MUTATE_PART   DROP_RANGE
                       │
                  executeFetch()
                       │
                  findReplicaHavingCoveringPart()
                       │
                  fetchPart()  ──HTTP──►  tmp-fetch_/ 临时目录
                       │
                  checkPartChecksumsAndCommit()
                       │
                  原子 multi: createRequest(<r>/parts/<part>) + rename → Active
                       │
                       ▼
                  removeProcessedEntry()
                       │
                  删除 /queue/queue-XXXXXX
```

### 深入：`fetchSelectedPart` 数据传输

`src/Storages/MergeTree/DataPartsExchange.cpp:397` 是副本间 part 拉取的入口：

1. HTTP POST 到 source 副本的 interserver 端口：
   `http://<host>:<port>/?endpoint=<replica_path>&part=<part_name>&...`
2. **协商 zero-copy**：客户端发送支持的磁盘 capability（s3/azure），服务端通过 `remote_fs_metadata` cookie 决定走哪条路径；
3. **zero-copy 路径**（S3 等共享存储）：
   - 服务端返回对象 `part_id`；客户端用 `checkUniqueId` 校验本地磁盘已有该对象；
   - 仅传输元数据（columns/checksums），数据文件通过 S3 hardlink 复用；
   - 创建 `lockSharedDataTemporary`（临时 ephemeral zero-copy 锁），等 part 提交后再持久化；
4. **普通路径**：流式传输所有列文件、mark、index；
5. 下载完成后用服务端发来的 `data_checksums` 校验；
6. 调用 `commitPart`（走 `writeExistingPart` 路径）提交本地 + Keeper。

### 深入：`executeLogEntry` 的幂等性

`StorageReplicatedMergeTree.cpp:2494` 的 `executeLogEntry` 在执行任何动作前先做一次幂等性检查：

```
if entry.type == GET_PART or ATTACH_PART:
    if exists(<replica>/parts/<part>) && local part 已就绪:
        return true   ← 直接成功，无需 fetch
```

这意味着同一个 log entry 在网络重试或重启场景下被执行多次也是安全的——只要 Keeper 元数据齐备、本地 part 在 active 集合中，就视作执行完成。

`/quorum/failed_parts/<part>` 也参与短路：若发现 part 已经被标记为 quorum 失败，直接跳过 `GET_PART`，避免无意义 fetch。

### 数据文件如何收敛

Keeper/ZooKeeper 中不存储真正列数据，只存 part 元数据和 checksum。数据文件通过副本间 HTTP/interserver 传输或共享存储获取。收敛依赖：

- part name 的区间语义：同一个 part name 表示同一个逻辑数据范围。
- `columns` 与 `checksums`：`getCommitPartOps` 会在 `/replicas/<replica>/parts/<part>` 下写最小 part header，或写 `columns` 和 `checksums` 子节点。
- fetch 后本地校验 checksum。
- 如果本地 part 损坏或缺失，`ReplicatedMergeTreeQueue::createLogEntriesToFetchBrokenParts` 会安排重新 fetch。
- 后台 `PartCheckThread` 和 restarting thread 会检查本地 active parts 与 Keeper metadata 是否一致。

因此，多副本数据一致性并不是通过同步写多份数据文件实现的，而是通过"Keeper 上已提交的 part 集合 + 每副本队列异步追赶 + checksum 校验"实现最终收敛。

## 副本启动恢复与自愈

`ReplicatedMergeTree` 的启动恢复涉及两个后台线程协作：`ReplicatedMergeTreeRestartingThread`（处理 session 重建和重启）与 `ReplicatedMergeTreePartCheckThread`（处理损坏 part 的自愈）。

### `ReplicatedMergeTreeRestartingThread`

`src/Storages/MergeTree/ReplicatedMergeTreeRestartingThread.cpp:81` 的运行循环：

```cpp
void ReplicatedMergeTreeRestartingThread::run()
{
    bool replica_is_active = runImpl();
    if (replica_is_active)
    {
        consecutive_check_failures = 0;
        task->scheduleAfter(check_period_ms);          // 正常：定期心跳检查
    }
    else
    {
        // 指数退避：100ms * (n+1)(n+2)/2，上限 10000ms
        task->scheduleAfter(next_failure_retry_ms);
    }
}

bool runImpl()
{
    if (!storage.is_readonly && !storage.getZooKeeper()->expired())
        return true;                                   // session 健康，无需重启
    if (storage.getZooKeeper()->expired())
        partialShutdown();                              // 停止所有后台任务
    storage.setZooKeeper();                             // 建立新 session
    if (!tryStartup()) return false;
    // 成功后启动后台任务
    storage.background_operations_assignee.start();
    storage.queue_updating_task->activateAndSchedule();
    storage.part_check_thread.start();
    ...
}
```

`tryStartup`（`ReplicatedMergeTreeRestartingThread.cpp:195`）的完整序列：

```
tryStartup()
  ├─ removeFailedQuorumParts()                      ← 清理失败 quorum 残留 part
  ├─ activateReplica()                              ← multi: create /is_active(Ephemeral) + set /host
  ├─ storage.cloneReplicaIfNeeded(zk)               ← is_lost=1 时克隆
  ├─ queue.initialize(zk)
  ├─ queue.load(zk)                                 ← 从 /queue 恢复内存队列
  ├─ queue.createLogEntriesToFetchBrokenParts()     ← broken → GET_PART
  ├─ queue.pullLogsToQueue(zk, {}, LOAD)            ← 追赶公共日志
  ├─ fixReplicaMetadataVersionIfNeeded(zk)          ← 对齐 metadata_version
  ├─ queue.removeCurrentPartsFromMutations()
  └─ updateQuorumIfWeHavePart()                     ← 补登 fetch 完成但未上报的 quorum part
```

`activateReplica`（`ReplicatedMergeTreeRestartingThread.cpp:341`）：

```cpp
ops: createRequest(is_active, active_node_identifier, Ephemeral)
   + setRequest(host, address.toString())
// is_active 是 ephemeral 节点，session 断开自动消失，是副本"活着"的唯一证据
storage.replica_is_active_node = EphemeralNodeHolder::existing(is_active_path, *zk);
```

### 启动恢复时序图

```
进程启动 / ZK Session 过期
        │
ReplicatedMergeTreeRestartingThread::run()
        │
        ├─► partialShutdown()                  [仅 session 过期]
        │   停止: background_operations, queue_updating, merge_selecting,
        │         cleanup_thread, part_check_thread, ...
        │
        ├─► storage.setZooKeeper()             [重建 ZK 连接]
        │
        ├─► tryStartup()
        │   │
        │   ├─ loadDataParts()                 [一般在 startupImpl 已完成]
        │   │
        │   ├─ checkParts() / checkPartsImpl
        │   │   ├─ 拉取 <replica>/parts 列表（期望集合）
        │   │   ├─ 本地无 active covering part 的 → parts_to_fetch
        │   │   └─ 本地有但 ZK 没注册的 → unexpected_data_parts
        │   │      ├─ covered by expected part → 忽略
        │   │      ├─ 空 part → empty unexpected
        │   │      └─ 非空 → 累计；超阈值抛 sanity check 异常
        │   │
        │   ├─ removeFailedQuorumParts()
        │   ├─ activateReplica()               [创建 ephemeral /is_active]
        │   ├─ cloneReplicaIfNeeded()          [is_lost=1 时走 cloneReplica]
        │   ├─ queue.load()                    [从 ZK 恢复 in-memory 队列]
        │   ├─ createLogEntriesToFetchBrokenParts()
        │   ├─ pullLogsToQueue(LOAD)           [追赶公共 log，更新 log_pointer]
        │   ├─ fixReplicaMetadataVersionIfNeeded()
        │   └─ updateQuorumIfWeHavePart()
        │
        └─► setNotReadonly()
            启动后台任务: background_ops / queue_updating / merge_selecting /
                         cleanup / part_check / mutations_finalizing
```

### `checkParts` 对账

`StorageReplicatedMergeTree.cpp:1926` 的 `checkPartsImpl` 是启动时本地 part 和 Keeper 期望集合对账的核心：

```
expected = zk.getChildren(<replica>/parts)        ← 期望存在的 part
for p in expected:
    if not getActiveContainingPart(p):
        parts_to_fetch.append(p)                  ← Keeper 有，本地无 → 拉取

local active = data_parts_by_state(Active)
for p in local active:
    if p.name not in expected:
        if p is covered by some expected part:
            ignore                                ← 旧版本，无害
        elif p is empty:
            empty unexpected                      ← 特殊处理
        else:
            unexpected counter++
            若超过阈值 → 抛 sanity 异常，启动失败
```

启动时该函数被调用两次：
1. 第一次（乐观）只看 Active；
2. 若失败，调用 `waitForOutdatedPartsToBeLoaded` 等 Outdated 加载完，再调一次。

### `ReplicatedMergeTreePartCheckThread`

它负责运行时损坏 part 的检测与自愈，触发场景有：
- `removePartAndEnqueueFetch` 主动入队；
- `enqueuePartForCheck` 兜底（`UNKNOWN_STATUS_OF_INSERT` 时延迟 300s 后核对）；
- 启动时 `checkParts` 发现缺失。

入队与调度（`ReplicatedMergeTreePartCheckThread.cpp:68, 564`）：

```cpp
void enqueuePart(name, delay_seconds)
{
    std::lock_guard lock(parts_mutex);
    if (parts_set.contains(name)) return;       // 幂等
    parts_queue.emplace_back(name, now + delay);
    parts_set.insert(name);
    task->schedule();
}
```

核心检测 `checkPartImpl`（`ReplicatedMergeTreePartCheckThread.cpp:290`）先读 ZK、再读本地（防止提交 race），然后按下述决策树自愈：

```
checkPartImpl(part_name)
        │
        ▼
   读 zk.exists(<replica>/parts/<part>) → exists_in_zk
   读 local = getPartIfExists(part_name, {PreActive})
            or getActiveContainingPart(part_name)
        │
   ┌────┴───────────────────────────────────────────┐
   │ 状态                                            │
   ├─────────────────────────────────────────────────┤
   │ exists_in_zk=T, local=F                          │
   │   ├─ part 处于 Outdated → RecheckLater          │
   │   └─ 否则 → TryFetchMissing                     │
   │                                                  │
   │ exists_in_zk=T, local=T                          │
   │   ├─ name 不精确匹配（仅 covering） → DoNothing │
   │   └─ checkDataPart + 与 ZK header 比对           │
   │        ├─ OK ──► DoNothing                       │
   │        └─ 损坏 ──► broken                         │
   │           └─ Detach 到 detached/broken-          │
   │              + removePartAndEnqueueFetch         │
   │                                                  │
   │ exists_in_zk=F, local=T                          │
   │   ├─ part 年龄 > MAX_AGE → DetachUnexpected      │
   │   └─ 年龄 ≤ MAX_AGE → RecheckLater （等 ZK 注册）│
   └─────────────────────────────────────────────────┘

   TryFetchMissing 路径:
        │
        ▼
   searchForMissingPartOnOtherReplicas()
        ├─ 找到（其他副本有 / 可由 merge 重建） ──► break，等 queue 自然 GET_PART
        └─ 未找到
              │
              ▼
        onPartIsLostForever()
              ├─ part 是 MERGE/MUTATE 结果 → 递归检查 source parts
              └─ part level == 0 → createEmptyPartInsteadOfLost
                                     （ProfileEvents::ReplicatedDataLoss++）
```

`removePartAndEnqueueFetch`（`StorageReplicatedMergeTree.cpp:4780`）的原子操作：

```cpp
// 1. broken part 及其覆盖的 parts 克隆到 detached/broken-*
part->makeCloneInDetached("broken", ...);
// 2. 从 working set 移除
removePartsFromWorkingSet(..., {broken_part}, ...);
// 3. 一次 multi:
//      delete <replica>/parts/<part>
//      create <replica>/queue/queue-<X> = GET_PART(part)
// queue 会重新 fetch 该 part
```

### `cloneReplica`：失活副本恢复

`StorageReplicatedMergeTree.cpp:3957` 的 `cloneReplicaIfNeeded` 决策路径：

```
cloneReplicaIfNeeded()
        │
   tryGet(<replica>/is_lost)
        │
   ┌────┴────────────────────────────┐
   │ "0"                              │ "1"
   │ ──► return (正常)                │
   │                                  ▼
   │                          is_new_replica? (is_lost_stat.version == 0)
   │                                  │
   │                          选源副本：遍历 /replicas
   │                          过滤条件：
   │                          ├─ /is_lost != "0" → 跳过
   │                          ├─ /log_pointer 不可读 → 跳过
   │                          └─ 选 replication_lag 最小
   │                             (lag = max_log_idx - log_pointer + queue_size)
   │                                  │
   │                          全部都 lost ──► ALL_REPLICAS_LOST 异常
   │                                  │
   │                          cloneReplica(source_replica)
   │                          ├─ CAS 抓 source 的 log_pointer + queue 一致快照
   │                          ├─ 复制 source queue 中的 GET_PART 到本副本
   │                          ├─ 枚举 source active parts，缺失的创建 GET_PART
   │                          ├─ set 本副本 log_pointer = source log_pointer
   │                          └─ 校验 source /is_lost version 未变（防中途也丢失）
   │                                  │
   │                          zk.set(<replica>/is_lost, "0")  ← 恢复完成
```

CAS 校验是关键：只有在源副本的 `log_pointer` 和 `is_lost` 版本号在 clone 期间未变化的前提下，clone 的快照才是一致的，否则要重试。

### Keeper 节点状态语义速查

| 节点 | 类型 | 含义 |
|---|---|---|
| `<replica>/is_active` | Ephemeral | Session 存活即在线；断线自动消失，是"是否活着"的唯一证据 |
| `<replica>/is_lost` | Persistent | `"0"` 正常 / `"1"` 需要 cloneReplica 恢复 |
| `<replica>/log_pointer` | Persistent | 已拉取至公共 `/log` 的最大序号 |
| `<replica>/metadata_version` | Persistent | 对应 DDL/ALTER 的 schema 版本号 |
| `<replica>/parts/<part>` | Persistent | 副本声明持有的 part，内容 = `ReplicatedMergeTreePartHeader` |
| `<replica>/queue/queue-NNN` | Persistent | 待执行 entry（从 `/log` 拷贝） |
| `<replica>/host` | Persistent | interserver 地址，fetch 路由依据 |

## Quorum insert

`insert_quorum` 会把写成功点从“本 replica 提交 + Keeper 日志提交”推进到“足够多 replica 已确认该 part”。

`ReplicatedMergeTreeSink::checkQuorumPrecondition` 会：

- 统计 `/replicas` 下活跃 replica 数。
- 检查活跃数是否满足 quorum。
- 对非 parallel quorum，检查是否已有未完成的 `/quorum/status`。
- 记录当前 replica 的 `/is_active` 和 `/host` version，后续在提交 `multi` 里用 check request 防止 replica 会话变化。

提交时 `get_quorum_ops` 会创建 quorum status 节点，初始内容包含当前 replica。其他副本执行复制日志并拿到 part 后，会把自己加入 quorum entry；达到 required replicas 后删除 quorum status 节点。`finishDelayed` 在提交成功后调用 `resolveQuorum` 等待 quorum 达成。

因此：

- 无 quorum：写请求成功后，其他副本可以异步追赶。
- 有 quorum：写请求需要等待 quorum 达成，失败或超时会向用户返回错误。
- parallel quorum 使用 `/quorum/parallel/<part>`，允许多个 quorum insert 并行；非 parallel quorum 一次只允许一个未完成 quorum part。

### 深入：quorum 状态机与跨副本协作

quorum 的状态推进涉及写副本（创建 status 节点）和其他副本（在 fetch 完成后把自己加入 status）。

```
                  Sink (r1) 提交 INSERT
                          │
                          ▼
              ┌─────────────────────────┐
              │  create /quorum/status  │  replicas = {r1}, required = N
              └────────────┬────────────┘
                           │
              其他副本 fetch /log/log-X 成功
              并写入本地 part 完成
                           │
                           ▼
              ┌─────────────────────────┐
              │   updateQuorumIfWeHave  │  CAS set，把自身加入 replicas
              │   Part (StorageReplicated│
              │   MergeTree.cpp:5255)   │
              └────────────┬────────────┘
                           │
                  ┌────────┴────────┐
                  │ replicas.size   │
                  │ >= required ?   │
                  └─┬────────────┬──┘
                YES │            │ NO
                    ▼            ▼
            ┌──────────┐    继续等待其他副本 fetch
            │ 删除      │
            │ /quorum/  │
            │ status    │
            │ 更新       │
            │ /quorum/   │
            │ last_part  │
            └──────────┘
                    │
                    ▼
            Sink (r1) 上的 watch 触发
            waitForQuorum 退出 → 返回客户端 OK

  失败路径（无活跃副本能 fetch）:
              ┌──────────────────────────┐
              │ create /quorum/failed_parts/<part> │
              │ remove /blocks/<dedup_hash>        │ ← 允许下次重试用同 hash
              └──────────────────────────┘
                          │
                          ▼
                  Sink waitForQuorum 超时
                  → UNKNOWN_STATUS_OF_INSERT
```

`updateQuorumIfWeHavePart` 用 CAS（带 stat.version）更新 quorum 节点，避免并发副本同时加入造成丢更。

## 去重与重试语义

`ReplicatedMergeTree` 的 insert dedup 依赖 Keeper/ZooKeeper 节点：

- `getDeduplicationPaths` 根据 `DeduplicationHash` 构造 block id 路径。
- `getCommitPartOps` 对每个 block id path 创建 persistent 节点，节点内容为 part name。
- 如果创建时遇到 `ZNODEEXISTS`，说明相同 block 已经由某次 insert 提交。
- `ReplicatedMergeTreeSink` 会读取冲突节点中的 part name，把重复 token/row 过滤后重试；若整个 block 都重复，则返回 `INSERT_WAS_DEDUPLICATED`。

这对“客户端超时但服务端其实提交了”的场景很重要。用户重试同一 insert，如果 block id 一致，就会被识别为重复而不是写出第二份相同数据。

## 单机和多副本可靠性对比

| 维度 | 普通 `MergeTree` | `ReplicatedMergeTree` |
| --- | --- | --- |
| 数据提交单元 | 本地 immutable part | 本地 immutable part + Keeper metadata |
| 全局顺序来源 | 本地 block number / part set | Keeper 顺序节点、block number、replication log |
| `INSERT` 成功点 | part finalize、rename、active commit | 本地 part commit + Keeper `multi` 成功；quorum 时还要等待 quorum |
| 数据恢复 | 启动扫描正式 part，清理临时目录 | 启动扫描 + 与 Keeper `/parts` 对账 + fetch broken/missing parts |
| 去重 | 本地 `DeduplicationLog`，受窗口配置影响 | Keeper block id 节点，跨副本生效 |
| 多版本 | part 级 `creation_csn`/`removal_csn` | 常规复制表事务不支持完整事务；复制自身用 part set 和 log entry 版本演进 |
| 共识协议 | 无 | 依赖 Keeper/ZooKeeper；ClickHouse Keeper 使用 `NuRaft` |

## 关键源码索引

普通写入与恢复：

- `src/Storages/MergeTree/MergeTreeSink.cpp`
  - `MergeTreeSink::consume`
  - `MergeTreeSink::finishDelayedChunk`
  - `MergeTreeSink::commitPart`
- `src/Storages/MergeTree/MergeTreeDataWriter.cpp`
  - `MergeTreeDataWriter::writeTempPart`
  - `MergeTreeDataWriter::writeTempPartImpl`
- `src/Storages/MergeTree/MergeTreeData.cpp`
  - `MergeTreeData::renameTempPartAndAdd`
  - `MergeTreeData::renameTempPartAndReplaceImpl`
  - `MergeTreeData::preparePartForCommit`
  - `MergeTreeData::Transaction::commit`
  - `MergeTreeData::loadDataParts`
  - `MergeTreeData::clearOldTemporaryDirectories`
  - `MergeTreeData::createStorageSnapshot`
  - `MergeTreeData::getVisibleDataPartsVector`
  - `MergeTreeData::filterVisibleDataParts`
- `src/Storages/MergeTree/MergeTreeSettings.cpp`
  - `fsync_after_insert`
  - `fsync_part_directory`

事务与 MVCC：

- `src/Interpreters/TransactionLog.h`
- `src/Interpreters/TransactionLog.cpp`
  - `TransactionLog::beginTransaction`
  - `TransactionLog::commitTransaction`
  - `TransactionLog::finalizeCommittedTransaction`
  - `TransactionLog::rollbackTransaction`
  - `TransactionLog::getCSN`
  - `TransactionLog::getOldestSnapshot`
- `src/Interpreters/MergeTreeTransaction.cpp`
  - `MergeTreeTransaction::beforeCommit`
  - `MergeTreeTransaction::afterCommit`
  - `MergeTreeTransaction::rollback`
  - `MergeTreeTransaction::addNewPartAndRemoveCovered`
- `src/Interpreters/MergeTreeTransaction/VersionInfo.cpp`
  - `VersionInfo::isVisible`
- `src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp`
  - `VersionMetadata::isVisible`
  - `VersionMetadata::canBeRemoved`
- `src/Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.cpp`
  - `VersionMetadataOnDisk::loadMetadata`
  - `VersionMetadataOnDisk::storeInfoToDataPartStorage`

副本与一致性：

- `src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp`
  - `ReplicatedMergeTreeSink::finishDelayed`
  - `ReplicatedMergeTreeSink::commitPart`
  - `ReplicatedMergeTreeSink::checkQuorumPrecondition`
- `src/Storages/StorageReplicatedMergeTree.cpp`
  - `StorageReplicatedMergeTree::allocateBlockNumber`
  - `StorageReplicatedMergeTree::getCommitPartOps`
- `src/Storages/MergeTree/ReplicatedMergeTreeQueue.cpp`
  - `ReplicatedMergeTreeQueue::initialize`
  - `ReplicatedMergeTreeQueue::load`
  - `ReplicatedMergeTreeQueue::pullLogsToQueue`
  - `ReplicatedMergeTreeQueue::createLogEntriesToFetchBrokenParts`
- `src/Storages/MergeTree/ReplicatedMergeTreeLogEntry.h`
  - `ReplicatedMergeTreeLogEntryData`
- `src/Coordination/KeeperServer.h`
  - `KeeperServer` 与 `NuRaft` 入口

## 一句话总结

`MergeTree` 可靠性的核心不是传统行级 `WAL`，而是 part 级不可变文件、原子发布、校验、启动扫描、事务 `CSN` 和 Keeper 元数据。普通表靠本地 part 状态机恢复；事务靠 Keeper `CSN` 与 part version metadata 做 MVCC；副本表靠 Keeper/ZooKeeper 的线性化复制日志、去重节点、quorum 状态和各副本 queue 保持一致。
