# MergeTree 倒排索引（Text Index）实现解析

## 1. 定位

ClickHouse 当前的“倒排索引”实现对应 `MergeTree` skip index 的 `text` 类型，核心代码在：

- `src/Storages/MergeTree/MergeTreeIndexText.h`
- `src/Storages/MergeTree/MergeTreeIndexText.cpp`
- `src/Storages/MergeTree/MergeTreeIndexConditionText.cpp`
- `src/Storages/MergeTree/MergeTreeReaderTextIndex.cpp`
- `src/Storages/MergeTree/TextIndexUtils.cpp`
- `src/Storages/MergeTree/MergeTreeIndexTextPostingListCodec.cpp`

从接口上看，它仍然挂在 `IMergeTreeIndex` 体系下，但和普通 skip index 有两个重要差异：

1. 普通 skip index 的一个 index granule 通常覆盖若干个 `MergeTree` marks；`text` index 是按整个 `Part` 构建一个逻辑 granule，代码注释里称为 `calculated on the whole and has infinite granularity`。
2. 普通 skip index 通常只回答“某个 mark range 是否可能命中”；`text` index 除了可以裁剪 mark range，还可以在 direct read 优化下直接生成虚拟过滤列。

逻辑上，它保存的是：

```text
token -> posting list(row ids in this Part)
```

其中 row id 是 `Part` 内部的行号，不是全表行号，也不是主键值。

示意：

```text
Part rows
  row 0: "clickhouse storage engine"
  row 1: "merge tree index"
  row 2: "clickhouse text index"

Text index
  clickhouse -> [0, 2]
  storage    -> [0]
  engine     -> [0]
  merge      -> [1]
  tree       -> [1]
  index      -> [1, 2]
  text       -> [2]
```

## 2. SQL 层定义

典型定义：

```sql
CREATE TABLE docs
(
    id UInt64,
    body String,
    INDEX body_text body TYPE text(
        tokenizer = splitByNonAlpha,
        preprocessor = lower(body),
        dictionary_block_size = 512,
        dictionary_block_frontcoding_compression = 1,
        posting_list_block_size = 1048576,
        posting_list_codec = 'bitpacking'
    )
)
ENGINE = MergeTree
ORDER BY id;
```

主要参数对应 `MergeTreeIndexTextParams`：

| 参数 | 代码字段 | 作用 |
|------|----------|------|
| `tokenizer` | `MergeTreeIndexText::tokenizer` | 把字符串拆成 token |
| `preprocessor` | `params.preprocessor` / `MergeTreeIndexTextPreprocessor` | tokenization 前先改写输入列，例如 `lower` |
| `dictionary_block_size` | `params.dictionary_block_size` | 每个 dictionary block 保存多少个 token，默认 `512` |
| `dictionary_block_frontcoding_compression` | `params.dictionary_block_frontcoding_compression` | dictionary block 内 token 是否使用 front-coding |
| `posting_list_block_size` | `params.posting_list_block_size` | 大 posting list 拆分为多个段的目标 row id 数量，默认 `1024 * 1024` |
| `posting_list_codec` | `posting_list_codec` | posting list 编码，当前支持 `none` 和 `bitpacking` |

支持的查询函数由 `MergeTreeIndexConditionText::isSupportedFunction` 决定，包括 `hasToken`、`hasAnyTokens`、`hasAllTokens`、`hasPhrase`、`equals`、`like`、`ilike`、`startsWith`、`endsWith`、`match`、`multiSearchAny`、`mapContainsKey`、`mapContainsValue`、`has`、`hasAny`、`hasAll` 等。

## 3. Part 内磁盘文件

`MergeTreeIndexText::getSubstreams` 返回三个子流：

| 子流类型 | 文件名模式 | 说明 |
|----------|------------|------|
| `Regular` | `skp_idx_<index>.idx` | text index header + dictionary sparse index |
| `TextIndexDictionary` | `skp_idx_<index>.dct.idx` | dictionary blocks |
| `TextIndexPostings` | `skp_idx_<index>.pst.idx` | posting list blocks |

每个子流也有对应 marks 文件，扩展名由 `Part` marks 类型决定，例如 `.mrk2` 或压缩 marks 变体。和普通 skip index 兼容，text index 写入一个 mark；读取时 `makeTextIndexInputStream` 使用 `MergeTreeReaderStreamSingleColumnWholePart`，因为 text index 逻辑上总是整 `Part` 一个 granule。

磁盘布局示意：

```text
all_1_1_0/
  checksums.txt
  columns.txt
  ...
  skp_idx_body_text.idx        ← header + sparse index
  skp_idx_body_text.mrk2
  skp_idx_body_text.dct.idx    ← dictionary blocks
  skp_idx_body_text.dct.mrk2
  skp_idx_body_text.pst.idx    ← posting list payload
  skp_idx_body_text.pst.mrk2
```

三者的关系：

```text
skp_idx_body_text.idx
  └── sparse index:
        first token of dictionary block -> offset in .dct.idx

skp_idx_body_text.dct.idx
  ├── dictionary block 0:
  │     tokens + token posting metadata
  ├── dictionary block 1:
  │     tokens + token posting metadata
  └── ...

skp_idx_body_text.pst.idx
  ├── posting list block for token A
  ├── posting list block for token B
  └── ...
```

## 4. Header 与 Sparse Index

`TextIndexSerialization::serializeHeader` 写入 `skp_idx_<index>.idx`。当前格式版本是 `TextIndexHeader::Version::WithCodec`。

逻辑结构：

```text
<version as VarUInt>
<posting_list_codec_type as VarUInt>
<num_sparse_index_tokens as VarUInt>
<sparse tokens as ColumnString binary bulk>
<dictionary block offsets as ColumnUInt64 binary bulk>
```

`sparse tokens` 保存每个 dictionary block 的第一个 token。`dictionary block offsets` 保存对应 block 在 `.dct.idx` 文件里的压缩文件偏移。

示意：

```text
Dictionary tokens sorted:
  block 0: apple, banana, clickhouse, ...
  block 1: merge, mutation, part, ...
  block 2: storage, text, token, ...

Sparse index:
  apple   -> offset(block 0 in .dct.idx)
  merge   -> offset(block 1 in .dct.idx)
  storage -> offset(block 2 in .dct.idx)
```

查询 token `part` 时：

1. 在 sparse tokens 上做 upper bound。
2. 找到 `merge` 对应的 dictionary block。
3. seek 到 `.dct.idx` 的 offset。
4. 在该 dictionary block 内二分查找 `part`。

这类似 `MergeTree` 主键稀疏索引：先用小索引定位较小的数据块，再在块内查找。

## 5. Dictionary Block

`TextIndexSerialization::serializeTokensAndPostings` 把所有 token 排序后按 `dictionary_block_size` 切块。每个 dictionary block 的逻辑结构：

```text
<tokens_format as VarUInt>
<num_tokens as VarUInt>
<tokens payload>
repeat num_tokens:
  <TokenPostingsInfo>
  if EmbeddedPostings:
    <embedded posting list payload>
```

`tokens_format` 有两种：

- `RawStrings`：按 `SerializationString::serializeBinaryBulk` 写 token。
- `FrontCodedStrings`：第一个 token 写完整字符串，后续 token 写和前一个 token 的公共前缀长度 `lcp` 以及剩余后缀。

front-coding 示例：

```text
tokens:
  click
  clickhouse
  clicking

front-coded:
  "click"
  lcp=5, suffix="house"
  lcp=5, suffix="ing"
```

dictionary block 内 token 保持字典序，因此读取 block 后可以用二分查找 token。每个 token 后面紧跟 `TokenPostingsInfo`，描述它的 posting list 在哪里、覆盖哪些 row id 范围、是否内嵌。

## 6. TokenPostingsInfo

`TokenPostingsInfo` 是 dictionary block 里每个 token 的 posting list 元数据。

字段含义：

| 字段 | 含义 |
|------|------|
| `header` | 位标志，来自 `PostingsSerialization::Flags` |
| `cardinality` | posting list 中 row id 数量 |
| `offsets` | 每个 posting list block 在 `.pst.idx` 中的 offset |
| `ranges` | 每个 block 覆盖的 row id 闭区间 |
| `embedded_postings` | 小 posting list 直接内嵌在 dictionary block 内 |

序列化逻辑：

```text
<header as VarUInt>
<cardinality as VarUInt>

if EmbeddedPostings:
  <posting list payload embedded in dictionary block>
else:
  if not SingleBlock:
    <num_postings_blocks as VarUInt>
  repeat postings block:
    <offset_in_pst_file as VarUInt>
    <min_row_id as VarUInt>
    <max_row_id as VarUInt>
```

`header` 的重要 flags：

| Flag | 含义 |
|------|------|
| `RawPostings` | posting list 用 VarUInt row ids 直接写 |
| `EmbeddedPostings` | posting list 不在 `.pst.idx`，直接写在 dictionary block |
| `SingleBlock` | 只有一个 postings block，不需要额外写 block 数 |
| `IsCompressed` | posting list 用 codec 编码 |
| `HasBlockIndex` | bitpacking 段内带 per-block metadata，支持 cursor 快速跳转 |

## 7. Posting List 存储

posting list 是 token 到 `Part` 内 row id 的有序集合。内存里使用 Roaring bitmap：

```cpp
using PostingList = roaring::Roaring;
```

写入时按 cardinality 选择不同路径：

| 条件 | 存储方式 |
|------|----------|
| `cardinality <= MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS`，当前为 `6` | raw VarUInt row ids，内嵌在 dictionary block |
| `cardinality <= MAX_CARDINALITY_FOR_RAW_POSTINGS`，当前为 `12` | raw VarUInt row ids，写入 `.pst.idx` |
| 更大且 `posting_list_codec = 'none'` | Roaring portable serialization |
| 更大且 `posting_list_codec = 'bitpacking'` | delta + bitpacking 编码 |

### 7.1 Raw postings

小 posting list 直接写 row ids：

```text
token = "rare"
posting list = [7, 120]

payload:
  VarUInt(7)
  VarUInt(120)
```

内嵌小 postings 的意义是避免一次额外随机读 `.pst.idx`。查询只要读 dictionary block，就已经拿到了这些 token 的 row ids。

### 7.2 Roaring postings

未启用 posting list codec 时，大 posting list 会写 Roaring bitmap：

```text
<num_uncompressed_bytes as VarUInt>
<Roaring portable bytes>
```

如果 posting list 超过 `posting_list_block_size`，会拆成多个 block。每个 block 的 offset 和 row id 范围写进 `TokenPostingsInfo`：

```text
token = "clickhouse"
posting blocks:
  block 0: offset=1000, range=[0, 900000]
  block 1: offset=1800, range=[900001, 1800000]
```

读取某个 mark range 时，只需要读和目标 row range 相交的 postings blocks。

### 7.3 Bitpacking postings

`posting_list_codec = 'bitpacking'` 时，`PostingListCodecBitpackingImpl` 使用以下思路：

1. row ids 严格递增。
2. 转成 delta/gap 序列。
3. 每个固定大小 block 计算最大 bit width。
4. 写 `[1 byte bits][bitpacked payload]`。
5. posting list 再按 `posting_list_block_size` 切成 segment。
6. segment 后追加 Index Section，保存每个 packed block 的 `last_row_id` 和 `relative_offset`。

segment 逻辑结构：

```text
<codec_type as VarUInt>
<payload_bytes as VarUInt>
<cardinality as VarUInt>
<first_row_id as VarUInt>
<bitpacked payload>
<num_block_metas as VarUInt>
<last_row_id_0 as VarUInt> ...
<relative_offset_0 as VarUInt> ...
```

这个 Index Section 的目的，是让 `PostingListCursor` 可以按 row range 跳到 segment 内部较接近的位置，而不必总是从头解码整个 posting list。

## 8. 写入流程

### 8.1 普通 INSERT / 合并写新 Part

构建入口是 `MergeTreeIndexAggregatorText`。它用 `MergeTreeIndexTextGranuleBuilder` 逐行处理被索引列：

```text
Block
  │
  ├── preprocessor.processColumn
  │
  ├── tokenizer.forEachToken
  │
  └── token + current_row -> PostingListBuilder::add
```

`PostingListBuilder` 对低频 token 有一个小对象优化：前几个 row ids 先放在栈上小数组里，超过阈值后再转为 Roaring bitmap，减少大量低频 token 的分配开销。

写入一个 index granule 时：

```text
tokens_map
  │
  ├── 按 token 字典序排序
  │
  ├── 每 dictionary_block_size 个 token 写一个 dictionary block
  │
  ├── 每个 token 的 postings 写入 dictionary block 或 .pst.idx
  │
  └── dictionary block 的第一个 token + offset 写入 sparse index header
```

### 8.2 临时 segment

`BuildTextIndexTransform` 用于 materialization 或 merge 过程中构建 text index。它不会无限制把所有 token 都堆在内存里，而是按一个阈值周期性 flush 临时 segment：

```text
processed tokens > 100,000,000
  -> writeTemporarySegment
  -> text_index_tmp/<prefix>_<segment_no>_<index_name>...
```

每个临时 segment 本身也是完整的三子流 text index。后续 `MergeTextIndexesTask` 再把多个 segment 合并成最终 `Part` 的 text index 文件。

## 9. 查询路径

查询条件由 `MergeTreeIndexConditionText` 转成 RPN，和其他索引条件类似。不同的是，它会提取 text search query：

```text
WHERE hasAllTokens(body, ['clickhouse', 'storage'])

TextSearchQuery
  function_name = hasAllTokens
  search_mode = All
  tokens = ['clickhouse', 'storage']
```

### 9.1 普通 skip index 裁剪

读取 text index 时，`MergeTreeIndexGranuleText::deserializeBinaryWithMultipleStreams` 打开三路流：

```text
Regular             -> .idx header / sparse index
TextIndexDictionary -> .dct.idx dictionary blocks
TextIndexPostings   -> .pst.idx postings
```

处理步骤：

1. 读 `.idx` header，拿到 sparse index 和 posting codec。
2. 对查询 token，根据 sparse index 找到可能包含 token 的 dictionary block。
3. 读需要的 dictionary blocks。
4. 在 block 内查找 token，得到 `TokenPostingsInfo`。
5. 对需要的 token 读取 posting list block。
6. 用 posting list 的 row id range 判断当前 `MergeTree` mark range 是否可能命中。

示意：

```text
Query: hasAllTokens(body, ['clickhouse', 'storage'])

Sparse index
  clickhouse -> dictionary block 12
  storage    -> dictionary block 41

Dictionary block 12
  clickhouse -> TokenPostingsInfo(offsets=[...], ranges=[[10, 9200], [30000, 70000]])

Dictionary block 41
  storage -> TokenPostingsInfo(offsets=[...], ranges=[[0, 8000], [50000, 90000]])

Intersection
  clickhouse rows ∩ storage rows ∩ current mark rows
```

如果某个 mark range 对应的 row ids 与最终 posting list 没有交集，则 `mayBeTrueOnGranule` 返回 `false`，该 mark range 可以跳过。

### 9.2 Direct Read from Text Index

`MergeTreeReaderTextIndex` 是 direct read 优化路径。它不只是返回“某个 granule 可能命中”，而是直接从 posting lists 生成虚拟 `UInt8` 列：

```text
__text_index_<index_name>_<function_name>_<hash> UInt8
```

对于：

```sql
SELECT count()
FROM docs
WHERE hasToken(body, 'clickhouse');
```

计划可以把 `hasToken(body, 'clickhouse')` 替换为虚拟列：

```text
WHERE __text_index_body_text_hasToken_<hash>
```

读取某个 mark 时：

```text
mark -> rows [mark_start, mark_end]
posting list for "clickhouse" -> [0, 2, 100, 8193, ...]

virtual column for this mark:
  row mark_start + 0 -> 0 or 1
  row mark_start + 1 -> 0 or 1
  ...
```

这样可以避免读取原始 `body` 列并执行字符串函数。对精确 token 函数，例如 `hasToken`、`hasAnyTokens`、`hasAllTokens`，direct read 可以是 `Exact`。对 `like`、`startsWith`、`endsWith`、某些 preprocessor 或 tokenizer 场景，direct read 可能只能作为 `Hint`：虚拟列先减少候选行，原始谓词仍要执行以保证正确性。

### 9.3 Pattern 查询

对于 `like`、`match` 等模式查询，代码会尝试从模式中提取必要 token 或扫描 dictionary blocks 找匹配 token。若匹配 token 太多，超过 `text_index_like_max_postings_to_read`，分析会进入 bypass：

```text
pattern scan too broad
  -> cannot build complete postings condition
  -> conservatively return true or use fallback expression
```

这保证不会因为不完整的倒排分析跳过真实匹配行。

## 10. 与 MergeTree 存储的协同

### 10.1 以 Part 为单位

Text index 文件是 `Part` 的一部分，进入 `checksums.txt`，随 `Part` 一起移动、fetch、校验和删除。它不是全表全局索引：

```text
Table
  Part all_1_1_0
    body_text index: row ids relative to all_1_1_0

  Part all_2_2_0
    body_text index: row ids relative to all_2_2_0
```

查询多个 `Part` 时，每个 `Part` 分别读取自己的 text index。posting list 中的 row id 只在该 `Part` 内有意义。

### 10.2 与 mark range 协同

`MergeTree` 读取仍以 mark range 调度。Text index 的 posting list 是 row id 级别；读取器通过 `MergeTreeIndexGranularity` 把 mark 转成 row range：

```text
mark 10 -> rows [81920, 90111]

posting list("clickhouse") = [5, 90000, 90001, 130000]

intersection with mark 10:
  [90000, 90001] -> mark 10 可能命中
```

普通裁剪路径输出的是保留或删除 mark range；direct read 路径则在 mark 内生成逐行 `UInt8` 虚拟列。

### 10.3 与主键和其他 skip indexes 协同

查询流程可以理解为：

```text
Partition pruning
  -> Part MinMax
  -> primary key mark ranges
  -> normal skip indexes
  -> text index analysis
  -> data read / direct read virtual columns
```

Text index 不替代主键索引。主键负责把物理有序范围缩小，text index 负责在候选范围内按 token row ids 精细过滤。代码中也有专门的 `MergeTreeIndexReadResultPool` 和 partial disjunction 处理，避免 text index 与其他 skip indexes 混用时重复或错误访问 granule。

### 10.4 与 merge 协同

`MergeTree` 后台 merge 会生成新 `Part`。Text index 有两种处理方式：

1. **直接 merge index**：`MergeTextIndexesTask` 读取多个源 `Part` 或临时 segment 的 dictionary blocks 和 postings，按 token 字典序归并。
2. **重建 index**：在 TTL、mutation 或需要重新计算行映射的场景中，通过 `BuildTextIndexTransform` 从合并后的数据流重新构建。

直接 merge 的关键问题是 row id 变化。源 `Part` 的 posting list 是源 `Part` 内 row id，新 `Part` 的 row id 取决于合并结果。`MergeTextIndexesTask::adjustPartOffsets` 使用 `MergedPartOffsets` 做映射：

```text
source part 0 row 10 -> new part row 17
source part 1 row  3 -> new part row 18

token "clickhouse":
  source postings:
    part 0: [10]
    part 1: [3]

  adjusted postings:
    [17, 18]
```

然后把同一 token 的 postings 做 OR 合并，写入新 `Part` 的 `.pst.idx` 和 `.dct.idx`。

### 10.5 与 mutation/materialize 协同

`ALTER TABLE ... MATERIALIZE INDEX` 或 mutation 路径会走 `BuildTextIndexTransform`。如果待 materialize 的 `Part` 行数超过 `UInt32` 上限，代码会拒绝构建，因为 posting list row id 当前用 `UInt32` 表示。

临时文件写在 `text_index_tmp` 存储中，最后由 `MergeTextIndexesTask` 或 mutation 收尾逻辑写入正式 `Part` 文件，并加入 `checksums.txt`。

### 10.6 与复制和远端存储协同

因为 text index 文件列在 `checksums.txt` 中，所以 `ReplicatedMergeTree` fetch `Part` 时会把 `.idx`、`.dct.idx`、`.pst.idx` 及 marks 一起传输和校验。对象存储场景下，text index 也按 `Part` 文件处理；读取层通过 index cache、mark cache、postings cache 降低随机读成本。

## 11. 查询示例

表：

```sql
CREATE TABLE docs
(
    id UInt64,
    body String,
    INDEX body_text body TYPE text(
        tokenizer = splitByNonAlpha,
        preprocessor = lower(body),
        posting_list_codec = 'bitpacking'
    )
)
ENGINE = MergeTree
ORDER BY id;
```

数据：

```text
row 0: ClickHouse stores data in parts
row 1: MergeTree builds sparse primary indexes
row 2: Text index stores tokens and postings
row 3: ClickHouse text search reads postings
```

预处理和 tokenization 后：

```text
row 0: clickhouse, stores, data, in, parts
row 1: mergetree, builds, sparse, primary, indexes
row 2: text, index, stores, tokens, and, postings
row 3: clickhouse, text, search, reads, postings
```

倒排结构：

```text
clickhouse -> [0, 3]
stores     -> [0, 2]
text       -> [2, 3]
postings   -> [2, 3]
index      -> [2]
```

查询：

```sql
SELECT id
FROM docs
WHERE hasAllTokens(body, ['text', 'postings']);
```

执行逻辑：

```text
token "text"     -> postings [2, 3]
token "postings" -> postings [2, 3]

All mode:
  [2, 3] AND [2, 3] = [2, 3]

结果候选 rows:
  row 2, row 3
```

如果走 ordinary skip index 路径，它会用 `[2, 3]` 判断对应 mark range 是否可能命中；如果走 direct read，它会为当前读取窗口生成虚拟过滤列：

```text
row 0 -> 0
row 1 -> 0
row 2 -> 1
row 3 -> 1
```

`WHERE` 使用这个虚拟列即可过滤，不需要读取 `body` 并重新执行 `hasAllTokens`。

## 12. 和 Bloom Filter 文本索引的区别

`tokenbf_v1` / `ngrambf_v1` 是 Bloom filter skip index：它只能判断 token 是否一定不存在于某个 granule，存在 false positive，也不能告诉你哪些行命中。

`text` index 是真正的倒排索引：

| 能力 | `tokenbf_v1` / `ngrambf_v1` | `text` |
|------|-----------------------------|--------|
| 存储 token -> rows | 否 | 是 |
| false positive | 可能 | 精确 token postings 无 false positive |
| 可生成逐行虚拟过滤列 | 否 | 是 |
| 支持 direct read | 否 | 是 |
| 存储成本 | 较低 | 较高 |
| 查询能力 | 粗粒度跳 granule | 可做 row-level postings 组合 |

因此 `text` index 更适合正式全文检索；Bloom filter 文本索引更像轻量级粗筛。

## 13. 关键设计总结

1. Text index 是 `MergeTree` 的 `Part` 内倒排索引，不是全表全局索引。
2. 磁盘上分三路流：`.idx` sparse header、`.dct.idx` dictionary blocks、`.pst.idx` posting lists。
3. Dictionary block 保存 token 和 posting metadata，posting list 大多写在 `.pst.idx`，极小 postings 可以内嵌。
4. Posting list 存的是 `UInt32` row id，row id 相对当前 `Part`。
5. Merge 时必须用 `MergedPartOffsets` 把源 `Part` row id 映射到新 `Part` row id。
6. 查询时先从 SQL 谓词提取 token，再通过 sparse index 找 dictionary block，再读 postings，最后和当前 mark row range 相交。
7. Direct read 可以用 postings 直接生成虚拟 `UInt8` 过滤列，避免读取原始文本列。
8. 对无法完整证明的场景，代码会保守返回“可能匹配”或走 fallback，保证不会漏行。
