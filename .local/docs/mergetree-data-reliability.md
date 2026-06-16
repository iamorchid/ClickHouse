# `MergeTree` 数据可靠性、事务、快照与多副本机制分析

本文基于当前代码树分析 `MergeTree` 与 `ReplicatedMergeTree` 的写入提交、恢复、事务、snapshot/MVCC 以及多副本一致性实现。核心结论先列在前面：

- 普通 `MergeTree` 的 `INSERT INTO` 主路径不是传统数据库里“先写行级 `WAL`，再刷数据页”的模式。它写的是完整 immutable data part：先写临时 part 目录，生成列文件、mark、索引、校验和与元数据，然后 `finalize`，最后通过 rename 和内存状态切换把 part 放入 active working set。
- 普通 `MergeTree` 向用户返回成功的时刻，是相关临时 part 已经 `finalize`，`commitPart` 成功，`MergeTreeData::Transaction::commit` 把 part 标记为 `Active` 并更新工作集之后。默认情况下，这不等价于每次都做磁盘 `fsync`；强掉电持久性取决于 `fsync_after_insert` 与 `fsync_part_directory`。
- 机器异常重启后的恢复依赖 part 目录的原子可见性、`checksums.txt`、part 名称区间和启动扫描。完整 rename 成正式 part 且校验通过的 part 会重新加载；仍是 `tmp_`/`tmp_insert_` 等临时目录的未提交 part 会被清理。
- `MergeTree` 有事务/MVCC 基础设施，但它是受限且实验性质的能力。源码注释明确说明，当前不支持跨多 host 或涉及 `ReplicatedMergeTree` 表的事务。事务的全局提交顺序由 Keeper/ZooKeeper 中 `/clickhouse/txn/log/csn-` 顺序节点提供，`CSN` 同时作为 commit timestamp 和 snapshot version。
- snapshot 的实现不是复制数据，而是基于 part 级多版本：每个 part 有 `creation_tid`/`creation_csn` 与 `removal_tid`/`removal_csn`，读请求按 snapshot `CSN` 过滤 `Active`/`Outdated` parts。
- `ReplicatedMergeTree` 的一致性不是数据副本之间直接跑 `Raft`。它使用 Keeper/ZooKeeper 存储复制元数据、全局复制日志、去重节点和 quorum 状态；ClickHouse Keeper 自身通过 `NuRaft` 实现一致性，ZooKeeper 则通过 ZooKeeper 自身的一致性协议提供线性化元数据操作。

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

这段逻辑解决的是“客户端没收到成功/失败，但 Keeper 可能已经提交”的典型分布式提交未知状态问题。去重节点也会帮助用户重试 `INSERT` 时避免重复数据。

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

### 数据文件如何收敛

Keeper/ZooKeeper 中不存储真正列数据，只存 part 元数据和 checksum。数据文件通过副本间 HTTP/interserver 传输或共享存储获取。收敛依赖：

- part name 的区间语义：同一个 part name 表示同一个逻辑数据范围。
- `columns` 与 `checksums`：`getCommitPartOps` 会在 `/replicas/<replica>/parts/<part>` 下写最小 part header，或写 `columns` 和 `checksums` 子节点。
- fetch 后本地校验 checksum。
- 如果本地 part 损坏或缺失，`ReplicatedMergeTreeQueue::createLogEntriesToFetchBrokenParts` 会安排重新 fetch。
- 后台 `PartCheckThread` 和 restarting thread 会检查本地 active parts 与 Keeper metadata 是否一致。

因此，多副本数据一致性并不是通过同步写多份数据文件实现的，而是通过“Keeper 上已提交的 part 集合 + 每副本队列异步追赶 + checksum 校验”实现最终收敛。

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
