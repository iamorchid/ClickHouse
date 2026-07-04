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

### 1.1 全局组件架构

text index 模块横跨写入、merge、mutation、查询四条数据流。下图给出核心类的依赖关系：

```
                ┌─────────────────────────────────────────────────────────────┐
                │                       MergeTreeData                          │
                │  （part 集合管理、merge 触发、查询入口）                       │
                └────────────┬─────────────────────────┬─────────────────────┘
                             │ 写入 / merge / mutate    │ 查询
                             ▼                          ▼
   ┌──────────────────────────────────────┐   ┌────────────────────────────────┐
   │              写入与 merge            │   │           查询路径              │
   ├──────────────────────────────────────┤   ├────────────────────────────────┤
   │ MergeTreeIndexText                   │   │ MergeTreeIndexConditionText    │
   │  └ getSubstreams / createIndexAggreg │   │  ├ RPN + TextSearchQuery       │
   │  └ createIndexCondition              │   │  └ getDirectReadMode (Ex/Hint) │
   │                                      │   │                                 │
   │ MergeTreeIndexAggregatorText         │   │ ┌─ 普通 Skip 路径 ─────────────┐│
   │  └ MergeTreeIndexTextGranuleBuilder  │   │ │ MergeTreeSkipIndexReader     ││
   │       └ PostingListBuilder (SBO)     │   │ │  └ mayBeTrueOnGranule        ││
   │            └ Arena + StringHashMap   │   │ └──────────────────────────────┘│
   │                                      │   │ ┌─ Direct Read 路径 ───────────┐│
   │ MergeTreeIndexGranuleTextWritable    │   │ │ MergeTreeReaderTextIndex     ││
   │  └ serializeBinaryWithMultipleStreams│   │ │  ├ readGranule               ││
   │       ├ Regular     → .idx           │   │ │  ├ classifyVirtualColumns    ││
   │       ├ TextIndex   → .dct.idx       │   │ │  ├ buildPostingsForMark      ││
   │       │  Dictionary                  │   │ │  └ PostingListCursor         ││
   │       └ TextIndex   → .pst.idx       │   │ └──────────────────────────────┘│
   │          Postings                    │   └──┬─────────────────────────────┘
   │                                      │      │ TextSearchQuery + posting list
   │ BuildTextIndexTransform              │      ▼
   │  └ writeTemporarySegment             │   ┌────────────────────────────────┐
   │     → text_index_tmp/<seg>_<idx>     │   │       Cache 层                  │
   │                                      │   │ TextIndexHeaderCache            │
   │ MergeTextIndexesTask                 │   │ TextIndexTokensCache            │
   │  ├ k-way merge by token (SortQueue)  │   │ TextIndexPostingsCache          │
   │  ├ MergedPartOffsets (FoR + bitpack) │   └────────────────────────────────┘
   │  └ addToChecksums → checksums.txt    │
   └──────────────────────────────────────┘

   Codec：
   ┌────────────────────────────────────────────────────────────┐
   │ PostingsSerialization::Flags（统一字节标志）                 │
   │   RawPostings / EmbeddedPostings / SingleBlock /            │
   │   IsCompressed / HasBlockIndex                              │
   │ PostingListCodecBitpackingImpl                              │
   │   delta + per-block bitpacking + Index Section              │
   └────────────────────────────────────────────────────────────┘
```

三个子系统职责边界：

- **写入与 merge 子系统**：负责把行 → token → posting list → 字典 + 倒排文件。它的关键设计是"整 Part 一个 granule + 周期性临时 segment + k-way merge 重映射 row id"。
- **查询子系统**：把 SQL 谓词翻译成 `TextSearchQuery`，要么走传统 skip index 裁剪 mark range，要么走 direct read 把 posting list 拉成 `UInt8` 虚拟列。
- **缓存层**：三级专用缓存（header / token info / posting segment）让相同 part 的热 token 解码只发生一次，特别在对象存储场景下显著降低 random read 成本。

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

### 3.1 子流注册的源码

`MergeTreeIndexText::getSubstreams` 把 text index 的三个文件以 `IMergeTreeIndex` 多 substream 接口注册（`src/Storages/MergeTree/MergeTreeIndexText.cpp:1587-1595`）：

```cpp
MergeTreeIndexSubstreams MergeTreeIndexText::getSubstreams() const
{
    return
    {
        {MergeTreeIndexSubstream::Type::Regular,             "",     ".idx"},
        {MergeTreeIndexSubstream::Type::TextIndexDictionary, ".dct", ".idx"},
        {MergeTreeIndexSubstream::Type::TextIndexPostings,   ".pst", ".idx"}
    };
}
```

`MergeTreeIndexTextParams` 默认值（`MergeTreeIndexText.h:76-82` 与 `MergeTreeIndexText.cpp:78-81`）：

| 字段 | 默认值 |
|---|---|
| `dictionary_block_size` | `512`（`DEFAULT_DICTIONARY_BLOCK_SIZE`） |
| `dictionary_block_frontcoding_compression` | `1`（开启） |
| `posting_list_block_size` | `1024 * 1024` |
| `posting_list_codec` | `"none"`（`DEFAULT_POSTING_LIST_CODEC`） |

读取时 `makeTextIndexInputStream` 用 `MergeTreeReaderStreamSingleColumnWholePart`（`TextIndexUtils.cpp:512-539`，`marks_count = 1`）：把整文件视为单 mark，靠 `seekToMark({offset_in_file, 0})` 完成任意字节级 seek。这正好契合"text index 一个 Part 一个逻辑 granule"的语义。

### 3.2 三种 `.mrk2` 文件结构

text index 的三路子流各对应一个 mark 文件。和普通列一样，mark 文件用于把"逻辑 granule 序号"映射到"压缩文件偏移 + 解压块内偏移"，但 text index 整 part 一个 granule 这一特殊性让 mark 文件**退化为一条记录的占位符**。

#### `MarkInCompressedFile` 通用结构

`src/Formats/MarkInCompressedFile.h:17-28`：

```cpp
struct MarkInCompressedFile
{
    size_t offset_in_compressed_file;     // 8 字节：压缩块在原文件的起始
    size_t offset_in_decompressed_block;  // 8 字节：解压后块内偏移
};
```

mark 文件扩展名由 `(compressed, adaptive_granularity, part_type)` 三元组决定（`src/Storages/MergeTree/MergeTreeIndexGranularityInfo.cpp:82-101`）：

| 组合 | 扩展名 | 每 mark 大小 |
|---|---|---|
| `!compressed, !adaptive, Wide` | `.mrk` | 16 字节（2 × UInt64） |
| `!compressed, adaptive, Wide` | `.mrk2` | 24 字节（3 × UInt64） |
| `compressed, adaptive, Wide` | `.cmrk2` | 24 字节（再整体压缩） |
| `!compressed, adaptive, Compact` | `.mrk3` | 同 mrk2 |
| `compressed, adaptive, Compact` | `.cmrk3` | 同 cmrk2 |

`.mrk2` 与 `.mrk` 的差异是多出来的 `rows_in_granule` 字段（adaptive 模式专用）：

```
.mrk2 一条 mark（24 字节）：
┌──────────────────────────┬──────────────────────────┬────────────────────┐
│ offset_in_compressed_file│ offset_in_decompressed_  │  rows_in_granule   │
│        UInt64 LE         │    block (UInt64 LE)     │     UInt64 LE      │
│         8 bytes          │         8 bytes          │      8 bytes       │
└──────────────────────────┴──────────────────────────┴────────────────────┘
```

`.cmrk2` 内容与 `.mrk2` 相同，整文件再用 codec 压缩；读取时 `MergeTreeMarksLoader::loadMarksImpl` 用 `CompressedReadBufferFromFile` 解压（`MergeTreeMarksLoader.cpp:172-177`）。

#### 三路 mrk2 文件对应表

```
┌──────────────────────────┬──────────────────────────┬──────────────────┐
│ 数据文件                  │ mark 文件                 │ 数据是否压缩     │
├──────────────────────────┼──────────────────────────┼──────────────────┤
│ skp_idx_<n>.idx          │ skp_idx_<n>.mrk2         │ 是（LZ4/ZSTD）   │
│ skp_idx_<n>.dct.idx      │ skp_idx_<n>.dct.mrk2     │ 是（LZ4/ZSTD）   │
│ skp_idx_<n>.pst.idx      │ skp_idx_<n>.pst.mrk2     │ 否（见下文）     │
└──────────────────────────┴──────────────────────────┴──────────────────┘
```

`.pst.idx` 数据流不压缩（`src/Storages/MergeTree/MergeTreeIndicesSerialization.h:33-38`）：

```cpp
static bool isCompressed(Type type)
{
    // Text index postings are not compressed by write buffer,
    // because the compression is implicitly applied during building them.
    return type != Type::TextIndexPostings;
}
```

原因：posting list 本身已经走过 Roaring / bitpacking 等编码（实质压缩），再让写缓冲 LZ4 一遍既浪费 CPU、又会破坏字节级 seek 所需的偏移稳定性。

#### 写入时如何生成（每路只一条 mark）

`writeMarks` 函数在三路 stream 上各调用一次（`src/Storages/MergeTree/TextIndexUtils.cpp:80-92`）：

```cpp
void writeMarks(MergeTreeIndexOutputStreams & streams, bool can_use_adaptive_granularity)
{
    for (const auto & [_, stream] : streams)
    {
        auto & marks_out = stream->compress_marks
            ? stream->marks_compressed_hashing
            : stream->marks_hashing;

        writeBinaryLittleEndian(stream->plain_hashing.count(),        marks_out); // offset_in_compressed_file
        writeBinaryLittleEndian(stream->compressed_hashing.offset(),  marks_out); // offset_in_decompressed_block
        if (can_use_adaptive_granularity)
            writeBinaryLittleEndian(1UL,                              marks_out); // rows_in_granule = 1
    }
}
```

时序（`MergeTextIndexesTask::executeStep`，`TextIndexUtils.cpp:411-415`）：

```
1. executeStep() 起步时调用 writeMarks(output_streams, adaptive)
   ↓ 此时三路数据流都还未写入任何字节
   ↓ 三个 mark 全部为 {0, 0, 1}（指向文件起始）

2. 写入数据：
   - Regular   (.idx)     ← serializeHeader（sparse index）
   - Dictionary(.dct.idx) ← 字典块（token + TokenPostingsInfo）
   - Postings  (.pst.idx) ← posting list（Roaring 或 Bitpacking）

3. finalize() 关闭三路 stream
```

**结论**：text index 整 part 只一个逻辑 granule，三路 `.mrk2` 各只含 1 条 mark，内容通常是 `{0, 0, 1}`。它们不是用来定位每个 dictionary block 或 posting block 的二级索引，而是把通用 skip index 框架里的"第 0 个 index granule"定位到对应子流文件的起点。

#### 读取路径 A：常规 skip index 过滤

由 `MergeTreeIndexReader::makeIndexReaderStream`（`src/Storages/MergeTree/MergeTreeIndexReader.cpp:96-113`）构造，传入真实的 `marks_loader`，用普通 `MergeTreeReaderStreamSingleColumn` + `seekToMark(mark_idx)` 经过 mark 表查询字节偏移。这里 mark 文件**确实被加载**（虽然只有一条记录）。

对 text index 来说，常规 skip index 过滤路径会构造三路 stream：

- `skp_idx_body_text.idx` + `skp_idx_body_text.mrk2`
- `skp_idx_body_text.dct.idx` + `skp_idx_body_text.dct.mrk2`
- `skp_idx_body_text.pst.idx` + `skp_idx_body_text.pst.mrk2`

`MergeTreeIndexReader::read` 会对所有 stream 调用 `seekToMark(mark)`。因为 text index 只有一个逻辑 granule，这里的 `mark` 实际就是 `0`。所以：

- `skp_idx_body_text.mrk2` 的作用是把 Regular 子流定位到 `.idx` 起点，然后读取 header 和 sparse index；
- `skp_idx_body_text.dct.mrk2` 的作用是把 Dictionary 子流定位到 `.dct.idx` 起点，满足通用 reader 对每个子流都按 granule seek 的契约；
- `skp_idx_body_text.pst.mrk2` 同理，把 Postings 子流定位到 `.pst.idx` 起点。

随后进入 text index 自己的反序列化逻辑后，dictionary block 和 posting block 的随机 seek 就不再查这些 `.mrk2`。

#### 读取路径 B：merge / direct read 阶段

`makeTextIndexInputStream`（`TextIndexUtils.cpp:512-539`）显式传入 `marks_loader = nullptr`：

```cpp
static constexpr size_t marks_count = 1;
return std::make_unique<MergeTreeReaderStreamSingleColumnWholePart>(
    data_part_storage, *actual_stream_name, extension,
    marks_count,
    MarkRanges{{0, marks_count}},
    reader_settings,
    /*uncompressed_cache=*/ nullptr,
    data_part_storage->getFileSize(*actual_stream_name + extension),
    /*marks_loader=*/ nullptr,    // ← 整文件视图，不走 mark 表
    ...);
```

`MergeTreeReaderStreamSingleColumnWholePart::seekToMark(size_t)` 直接抛异常（`MergeTreeReaderStream.cpp:404-406`）——这条路径不支持「按 mark 序号跳」，只支持「按 `MarkInCompressedFile{byte_offset, 0}` 字节 seek」。

posting list 随机 seek 走的就是字节 seek（`TextIndexUtils.cpp:317-320`）：

```cpp
for (const auto offset_in_file : token_info.offsets)
{
    stream->seekToMark({offset_in_file, 0});      // 不经过 mrk2
    postings.emplace_back(postings_serialization.deserialize(...));
}
```

`token_info.offsets` 直接存 `.pst.idx` 内的绝对字节偏移；底层 `seekToMark(MarkInCompressedFile)`（`MergeTreeReaderStream.cpp:178-195`）：

```cpp
void MergeTreeReaderStream::seekToMark(const MarkInCompressedFile & mark)
{
    if (compressed_data_buffer)
        compressed_data_buffer->seek(mark.offset_in_compressed_file,
                                     mark.offset_in_decompressed_block);
    else
        plain_file_buffer->seek(mark.offset_in_compressed_file, SEEK_SET);
}
```

由于 pst 流 `is_compressed = false`，走 `plain_file_buffer->seek(offset, SEEK_SET)`——直接到文件偏移，`.pst.mrk2` 完全不参与。

#### 三种 mrk2 协作关系图

```
查询 token "clickhouse"
    │
    ▼
.idx (Regular，已压缩)
   ├─ MergeTreeIndexReader 读取（用 .mrk2 拿 mark{0,0,1} → seek 文件起点）
   └─ 反序列化 TextIndexHeader + sparse_index
        sparse_index 中存：first_token → dict_offset (压缩文件偏移)
                                            │
                                            ▼
.dct.idx (Dictionary，已压缩)
   ├─ 进入 reader 时先用 .dct.mrk2 seek 到 Dictionary 子流起点
   ├─ 查找具体 dictionary block 时，用 MarkInCompressedFile{dict_offset, 0} 字节 seek
   │  （注意：这个二级 seek **不查 .dct.mrk2**，dict_offset 来自 sparse_index）
   └─ 反序列化 token 列表 + TokenPostingsInfo
        TokenPostingsInfo.offsets 中存：每个 posting block 在 .pst 的字节偏移
                                            │
                                            ▼
.pst.idx (Postings，未压缩)
   ├─ 进入 reader 时先用 .pst.mrk2 seek 到 Postings 子流起点
   ├─ 查找具体 posting block 时，用 MarkInCompressedFile{block_offset, 0} 字节 seek
   │  （这个二级 seek 也不查 .pst.mrk2，block_offset 来自 TokenPostingsInfo）
   └─ 反序列化 Roaring / Bitpacking segment
```

#### 为什么仍要保留三个 mrk2 文件

虽然 dictionary block 和 posting block 的二级定位不靠 `.mrk2`，三路 `.mrk2` 仍是必需的：

1. **通用 skip index 框架要求**：`MergeTreeIndexReader` 调用路径会为每个 substream 加载 mark，并通过 `seekToMark(0)` 定位当前 index granule；
2. **checksum 校验**：`MergeTextIndexesTask::addToChecksums`（`TextIndexUtils.cpp:498-499`）把三个 mark 文件也登记进 `checksums.txt`，缺失会让 part 校验失败；
3. **跨副本一致性**：fetch 时 mark 文件随 data part 一起传输，跨副本字节级一致；
4. **`MergeTreeReaderStreamSingleColumnWholePart` 仍通过 `getFileSize` 限定读取范围**，mark 文件是这一约束的元数据外壳。

简言之：`.mrk2` 在 text index 里只负责"逻辑 granule → 子流起点"这一层；具体 token 所在的 dictionary block，以及具体 posting segment 所在的位置，由 `sparse_index → TokenPostingsInfo.offsets` 这条二级索引链承担。

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

### 4.1 Header 字节布局

`serializeHeader` / `deserializeHeader`（`MergeTreeIndexText.cpp:1065-1116`）。当前版本 `WithCodec` 的字节布局：

```
┌────────────────────────────────────────────────────────────┐
│  .idx (Regular substream) — TextIndexHeader                 │
├──────────────────┬─────────────────────────────────────────┤
│ version          │ VarUInt（0=Initial, 1=WithCodec）        │
│ codec_type       │ VarUInt（仅 WithCodec：0=None, 1=Bitpack）│
│ num_sparse_tokens│ VarUInt                                  │
│ sparse_tokens[]  │ SerializationString binary bulk          │
│ dict_offsets[]   │ ColumnUInt64 binary bulk（压缩文件偏移）  │
└──────────────────┴─────────────────────────────────────────┘
```

`Version` 枚举（`MergeTreeIndexText.h:254-260`）：

- `Initial = 0`：无 `codec_type` 字段；读取时若 `TokenPostingsInfo.header` 出现 `IsCompressed`，按惯例懒加载 Bitpacking codec；
- `WithCodec = 1`：显式记录 codec 类型，未来便于扩展（PFD、StreamVByte 等）。

`dict_offsets[i]` 是**压缩文件偏移**（`offset_in_compressed_file`，即每个 dictionary block 起始于新的压缩块边界），保证可独立解压一块而不必从头解压整个 `.dct.idx`。

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

### 5.1 Dictionary block 字节布局

源码 `serializeTokensAndPostings`（`MergeTreeIndexText.cpp:1276-1333`）。**每个 block 写入前调用 `dictionary_stream.compressed_hashing.next()`**，强制对齐到一个新压缩块边界，从而支持按 sparse index 偏移单块解压。

```
┌─────────────────────────────────────────────────────────────────┐
│  Dictionary Block（每 dictionary_block_size 个 token 一块）       │
├───────────────────┬─────────────────────────────────────────────┤
│ tokens_format     │ VarUInt（0 = RawStrings, 1 = FrontCoded）    │
│ num_tokens        │ VarUInt                                      │
│ tokens[]          │ 按 tokens_format 编码                         │
├───── 对每个 token 重复 ──────────────────────────────────────────┤
│ token_info.header     │ VarUInt（PostingsSerialization::Flags）  │
│ token_info.cardinality│ VarUInt                                  │
│ if EmbeddedPostings:  │ cardinality 个 VarUInt row_id            │
│ if !SingleBlock:      │ num_blocks VarUInt                       │
│ 每 block 重复:         │                                          │
│   offset_in_pst_file  │ VarUInt（绝对字节偏移到 .pst.idx）        │
│   rows_range.begin    │ VarUInt                                  │
│   rows_range.end      │ VarUInt                                  │
└───────────────────┴─────────────────────────────────────────────┘
```

### 5.2 Token 编码 RawStrings vs FrontCodedStrings

`MergeTreeIndexText.cpp:841-884`：

- **RawStrings**：每 token = `[VarUInt len][bytes]`，等同 `SerializationString::serializeBinaryBulk`；
- **FrontCodedStrings**：利用排序 token 公共前缀压缩：
  - 第一 token：`[VarUInt len][bytes]`
  - 后续 token：`[VarUInt lcp][VarUInt suffix_len][suffix_bytes]`

解码时只需对前一 token 前缀做一次 `memcpy`，再 append 新后缀。该编码对自然语言或代码 token 这类公共前缀较多的场景压缩效果显著（如 `click`/`clickhouse`/`clicking` 三 token，公共前缀复用 `click`）。

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

### 6.1 Flags 与各序列化路径的对应关系

`PostingsSerialization::Flags` 枚举（`MergeTreeIndexText.h:154-168`）：

```cpp
enum Flags : UInt64
{
    RawPostings      = 1ULL << 0,  // VarUInt 序列直接写
    EmbeddedPostings = 1ULL << 1,  // 写在字典块内，不进 .pst.idx
    SingleBlock      = 1ULL << 2,  // 只有 1 个 posting block，省 num_blocks
    IsCompressed     = 1ULL << 3,  // 使用 posting_list_codec（如 Bitpacking）
    HasBlockIndex    = 1ULL << 4,  // 每 segment 追加 Index Section（cursor 跳转）
};
```

不同 cardinality 与 codec 组合产生的 flag 组合（`MergeTreeIndexText.cpp:960-1030`）：

| 条件 | 实际 flags | 写入位置 |
|---|---|---|
| `cardinality ≤ 6` | `RawPostings \| EmbeddedPostings` | dictionary block 内 |
| `6 < cardinality ≤ 12` | `RawPostings \| SingleBlock` | `.pst.idx` 单 block |
| `12 < cardinality ≤ posting_list_block_size`（无 codec） | `SingleBlock` | `.pst.idx` Roaring portable 单 block |
| `cardinality > posting_list_block_size`（无 codec） | `0`（多 block Roaring） | `.pst.idx` 多 block |
| 任意 + `codec=bitpacking` | `IsCompressed \| HasBlockIndex \| [SingleBlock?]` | `.pst.idx` segment + Index Section |

常量定义：`MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS = 6`（`MergeTreeIndexText.cpp:73`），`MAX_CARDINALITY_FOR_RAW_POSTINGS = 12`（`MergeTreeIndexText.cpp:72`）。

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

### 7.4 Bitpacking segment 字节布局

`PostingListCodecBitpackingImpl::serializeTo`（`MergeTreeIndexTextPostingListCodec.cpp:113-141`），固定 `BLOCK_SIZE = 128`（`BitpackingBlockCodec.h:21`）。

```
┌──────────────────────────────────────────────────────┐
│  .pst (TextIndexPostings substream) — Segment         │
├──────────────────────────────────────────────────────┤
│ VarUInt codec_type     (= 1 表示 Bitpacking)          │
│ VarUInt payload_bytes                                │
│ VarUInt cardinality                                  │
│ VarUInt first_row_id                                 │
├──────────────────────────────────────────────────────┤
│ [Block 0] : 1 byte (bits) + packed payload           │
│ [Block 1] : 1 byte (bits) + packed payload           │
│  ...                                                 │
│ [Tail Block]                                         │
├──── Index Section（仅 HasBlockIndex 时存在）─────────┤
│ VarUInt num_blocks                                   │
│ VarUInt block_last_row_ids[0..n-1]                   │
│ VarUInt block_offsets[0..n-1]   ← Payload 内相对偏移 │
└──────────────────────────────────────────────────────┘
```

编码过程：

```
原始递增 row_ids: [r0, r1, r2, ..., rN]
          │
          ▼ delta
deltas:         [r0, r1-r0, r2-r1, ...]
          │
          ▼ 按 BLOCK_SIZE(128) 分块
Block0=[d0..d127] Block1=[d128..d255] ...
          │
          ▼ 每 block 内：
          │  max_bits = ceil_log2(max(block))
          │  写 [1B max_bits][bitpack 后字节]
          │
          ▼ payload 写完，追加 Index Section
Index Section 用 VarUInt 编码 last_row_id 数组与 relative_offset 数组
```

`payload_bytes`、`first_row_id`、`block_last_row_ids[]` 三者组合让 cursor 可以 O(log n) 跳到任意 row id 所在 block，再线性扫描该 block 内最多 128 个值。

### 7.5 三层文件偏移引用关系

```
.idx (Header + Sparse Index)
       sparse_index.dict_offsets[i]   ← 压缩文件偏移
              │
              ▼ 解压 dictionary block i
.dct.idx
       token_info[j].offsets[k]       ← .pst.idx 内绝对字节偏移
              │
              ▼ seek 到 segment
.pst.idx
       segment header → blocks → Index Section
                                  │
                                  ▼ block_offsets[m]
                                Block m（128 个 row_id）
```

`makeTextIndexInputStream`（`TextIndexUtils.cpp:512-539`）以 whole-file 模式打开每个子流，所有 seek 都通过 `seekToMark` 实现。这样在 S3 等对象存储上，seek 退化成 HTTP Range 请求；`offsets[]` 和 `block_offsets[]` 数组让随机访问最小化到 segment / block 粒度，避免全文件下载。

### 7.6 `PostingListCursor` 跳转语义

`MergeTreeIndexTextPostingListCursor.h:42-198` 接口（核心 5 个方法）：

```cpp
bool      valid()          const;     // 是否还有有效值
uint32_t  value()          const;     // 当前 row_id
void      next();                      // 移到下一个
void      advance(uint32_t target);   // 跳到 ≥ target 的第一个 row_id（leapfrog）

void linearOr (UInt8* data, size_t row_offset, size_t num_rows);  // 顺序 OR 填 UInt8 列
void linearAnd(UInt8* data, size_t row_offset, size_t num_rows);  // 顺序 AND 收紧 UInt8 列
```

`advance(target)` 两级二分跳跃（`MergeTreeIndexTextPostingListCursor.cpp:371-415`）：

```
1. Segment 级：在 token_info.ranges[] 上二分
                找到第一个 range.end >= target 的 segment
2. Block 级 ：在 segment.block_last_row_ids[] 上二分
                找到第一个 last_row_id >= target 的 packed block
3. 块内    ：decodeBlock(j) → inclusive_scan 复原绝对 row_id
                线性扫描至多 128 项
```

Cursor 支持两种构造模式：

- **压缩模式**：绑定 `.pst` stream + `TokenPostingsInfo`，按需 `prepareSegment`/`decodeBlock`；
- **内嵌模式**：绑定 `FlatPostingsPtr`（cardinality ≤ 12 已解码的 raw 数组），直接前进。

`PostingListSegment`（`PostingListSegment.h:1-44`）一次解析后是不可变的，可以放进 `TextIndexPostingsCache` 跨 query 共享。

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

这里的 index granule 不是普通 `MergeTree` 数据 granule。`text` index 把整个 `Part` 作为一个逻辑 index granule，所以普通 `INSERT` 写新 `Part` 时，`tokens_map`、`posting_lists` 和 `Arena` 会持续累积这个 `Part` 内已经处理过的 token。到 `fillSkipIndicesChecksums` 收尾阶段，writer 调用 `getGranuleAndReset()`，再一次性序列化成 `.idx`、`.dct.idx`、`.pst.idx` 三路文件。

因此普通 `INSERT` 路径可以理解为：

```text
写入 Part 的多个数据 granule
  -> text index aggregator 持续累积整个 Part 的 token -> postings
  -> Part 收尾时 build + serialize
  -> 写出最终 text index 文件
```

写入一个 text-index granule 时：

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

### 8.3 写入数据流细化

完整调用栈（INSERT 一个 `Block` 到磁盘）：

```
MergeTreeIndexAggregatorText::update(block, pos, limit)
   │  preprocessor.processColumn()      ← 应用 lower/upper 等 ExpressionActions
   │
   ▼  循环 limit 行
MergeTreeIndexTextGranuleBuilder::addDocument(string_view)
   │  forEachToken(*tokenizer, data, len, callback)
   │     ├─ SplitByNonAlpha：SSE2/SSE4.2 加速，按 16 字节批处理
   │     ├─ Ngrams：滑动窗口
   │     ├─ AsciiCJK：ASCII + CJK 混合
   │     └─ ...
   │  per token:
   │     ArenaKeyHolder key_holder{token, *arena}        ← 字符串入 Arena
   │     tokens_map.emplace(key_holder, it, inserted)     ← StringHashMap 命中/插入
   │     posting_list_builder.add(current_row, posting_lists)
   │
   ├─► incrementCurrentRow()    ← 行计数
   │
   ▼  Part 写完后
MergeTreeIndexTextGranuleBuilder::build()
   │  tokens_map.forEachValue → SortedTokensAndPostings
   │  std::ranges::sort(...)
   │
   ▼
MergeTreeIndexGranuleTextWritable::serializeBinaryWithMultipleStreams()
   │  serializeTokensAndPostings()
   │     ├─ 按 dictionary_block_size = 512 分块
   │     │   dictionary_stream.next()       ← 对齐压缩块边界
   │     │   写 token 列表（FrontCoded 可选）
   │     │   每 token 调 serializePostings()
   │     └─ 写 sparse index 到 .idx header
```

**`tokens_map` 数据结构**：`StringHashMap<PostingListBuilder>`（`MergeTreeIndexText.h:146`）。`StringHashMap` 是 ClickHouse 内部针对字符串优化的开放寻址哈希；token 字符串本身存到 `Arena`，节点只保存指针。`PostingListBuilder` 本身是 union（24+1 字节），整张 hashmap 在低频 token 大量出现时也几乎不发生堆分配。

### 8.4 `PostingListBuilder` 小对象优化（SBO）状态机

关键常量（`MergeTreeIndexText.h:97-98`）：

```cpp
static constexpr size_t max_small_size = 6;
using SmallContainer = std::array<UInt32, max_small_size>;
```

内存布局（`MergeTreeIndexText.h:136-142`）：

```cpp
union {
    SmallContainer        small{};   // 6 × UInt32 = 24 字节（栈上）
    PostingListWithContext large;    // PostingList* + roaring::BulkContext
};
UInt8 small_size;
```

状态机：

```
        ┌────────────────────────┐
        │  Small 模式             │  small_size ∈ [0, 6)
        │  (24 字节，无堆分配)     │
        └─────────┬──────────────┘
                  │ add(value):
                  │   if value != small[small_size-1]:
                  │       small[small_size++] = value
                  │
        small_size == max_small_size (6)
                  │
                  ▼ 升级
        ┌────────────────────────┐
        │  Large 模式             │
        │  large.postings = &posting_lists.emplace_back()
        │  large.context  = roaring::BulkContext{}
        │  addBulk(small[0..5]) 全部转入 Roaring
        └─────────┬──────────────┘
                  │ add(value):
                  │   large.postings->addBulk(large.context, value)
                  │   （BulkContext 缓存上次插入位置，顺序写线性）
                  ▼
              Roaring bitmap
```

**为什么用 `std::list<PostingList>` 而非 `vector`**：`PostingListBuilder::large.postings` 持有原始指针，要求容器扩容时迭代器不失效——`std::list::emplace_back` 满足该约束。

序列化层的存储路径在第 7 节决策树中已展开；运行时分配只在 Small → Large 升级那一刻发生（一次 Roaring 对象 + 内部 container），冷 token 永远停在栈上。

### 8.5 `BuildTextIndexTransform` 临时 segment 生命周期

`BuildTextIndexTransform` 继承 `ISimpleTransform`，在 materialization 和 merge 场景使用。触发阈值（`TextIndexUtils.cpp:143`）：

```cpp
static constexpr size_t max_processed_tokens = 100'000'000;
```

生命周期：

```
BuildTextIndexTransform 构造
   │  为每个 text index 创建 MergeTreeIndexAggregatorText
   │
   ▼  每个输入 Chunk
transform(chunk) → aggregate(block)
   │  aggregator.update(block, ...)
   │  if aggregator.getNumProcessedTokens() > 100M:
   │      writeTemporarySegment(i)
   │         ├─ granule = aggregator.getGranuleAndReset()
   │         ├─ aggregator.setCurrentRow(num_processed_rows)   ← 行号不复位
   │         ├─ index_file_name = "<prefix>_<seg_idx>_<index_name>"
   │         └─ 写到 temporary_text_index_storage（text_index_tmp/）
   │
   ▼  pipeline 结束
finalize() → 对每个非空 aggregator 再调 writeTemporarySegment
   │
   ▼
getSegments(index, part_idx) 返回所有 TextIndexSegment
   │
   ▼
MergeTextIndexesTask
   ├─ SortingQueue<SortCursor> 做 k-way merge by token
   ├─ 同 token postings 做 OR 合并（adjustPartOffsets 重映射 row id）
   ├─ 按 dictionary_block_size 批量写出 .dct/.pst/.idx
   ├─ executeStep() 分批，受 background_task_preferred_step_execution_time_ms 限制
   └─ addToChecksums() → 进 checksums.txt

最终
   temporary_text_index_storage->removeRecursive()  ← 清理 text_index_tmp/
```

`writeTemporarySegment` 之后 `setCurrentRow(num_processed_rows)`——**行号不归零**——保证后续 segment 中的 row id 仍是全 part 全局编号，便于后续 merge 时 OR 合并不需要重新平移。

### 8.6 整 Part 一个 granule 的内存峰值控制

text index granule 不对应 MergeTree 数据 granule，而是**整 Part 一个 granule**（`MergeTreeIndexText.h:28-29`：「skip index that is always calculated on the whole and has infinite granularity」）。这意味着 INSERT 同一个 Part 时，所有 token 都汇聚在同一 `tokens_map`。峰值内存组成：

| 来源 | 量级 |
|---|---|
| `Arena`（unique token 字符串池） | 与词汇表大小成正比 |
| `StringHashMap`（tokens_map） | 每 token 约 25 字节（指针 + builder） |
| `std::list<PostingList>` | 仅 cardinality > 6 的 token 才分配 Roaring |

控制手段差异：

- **materialization / merge 场景**：`BuildTextIndexTransform` 用 100M token 阈值滚动 segment，每次 `getGranuleAndReset()` 释放 tokens_map、posting_lists 与 Arena，峰值与单 segment 词汇量挂钩；
- **直接 INSERT 大 Part 场景**：没有 segment 切分保护，词汇量随 Part 行数线性增长。这是有意的权衡——INSERT 通常 Part 不会非常大；超大 Part 一般来自 merge，会走 transform 路径。

所以 text index 构建内存不是完全按行数或文件大小硬上限可控的。特别是普通 `INSERT` 生成超大 `Part`，或者文本列里有大量唯一 token、长 token、低重复度 token 时，`tokens_map` 和 `Arena` 的峰值可能很高。`BuildTextIndexTransform` 的 100M token 阈值也只是按处理 token 数滚动落盘，不是精确的内存阈值；如果单个 segment 内唯一 token 很多，内存仍可能明显增长。

`memoryUsageBytes()` 精确计量内存（`tokens_map.getBufferSizeInBytes() + Σ posting_lists[i].getSizeInBytes() + arena.allocatedBytes()`），可接入 memory tracker。

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
SELECT author, title
FROM docs
WHERE publish_date > '2000-12-31'
  AND hasToken(body, 'clickhouse');
```

假设 `docs` 表上有：

```sql
INDEX body_text body TYPE text(tokenizer = splitByNonAlpha)
```

这个查询里有两类过滤条件：

1. `publish_date > '2000-12-31'`：普通列过滤，需要读取 `publish_date`，或者由主键、minmax、其他 skip index 先裁剪 mark range。
2. `hasToken(body, 'clickhouse')`：text index 支持的精确 token 谓词，可以用 direct read 生成虚拟过滤列。

优化后的逻辑可以理解为：

```text
原始谓词:
publish_date > '2000-12-31'
AND hasToken(body, 'clickhouse')

改写后:
publish_date > '2000-12-31'
AND __text_index_body_text_hasToken_<hash>
```

因为 `hasToken` 对 text index 是 `Exact` direct read，虚拟列命中就等价于原始 `hasToken(body, 'clickhouse')` 命中。执行时不需要为了这个谓词读取 `body` 列，也不需要对每行重新 tokenize 和执行 `hasToken` 函数。

一次查询在单个 `Part` 内大致分成几步：

```text
1. 分析谓词
   hasToken(body, 'clickhouse')
      -> TextSearchQuery(function = hasToken, mode = All, direct = Exact, tokens = ['clickhouse'])
      -> 虚拟列名 __text_index_body_text_hasToken_<hash>

2. 读取 text index header
   skp_idx_body_text.idx
      -> sparse index
      -> 定位 'clickhouse' 所在 dictionary block

3. 读取 dictionary block
   skp_idx_body_text.dct.idx
      -> 找到 token 'clickhouse'
      -> 得到 TokenPostingsInfo
         cardinality = ...
         offsets = posting blocks 在 .pst.idx 中的位置
         ranges  = 每个 posting block 覆盖的 row id 范围

4. 读取需要的 posting blocks
   skp_idx_body_text.pst.idx
      -> 解出 postings('clickhouse')
      -> 这是 Part 内 row id 集合，例如 {0, 2, 100, 8193, ...}

5. 按 mark 生成虚拟 UInt8 列
   当前 mark rows [8192, 16383]
   postings('clickhouse') ∩ [8192, 16383] = {8193, 9000}
      -> 虚拟列在 mark 内对应行填 1
      -> 其他行填 0

6. 继续执行普通表达式过滤和投影
   读取 publish_date 判断 publish_date > '2000-12-31'
   对两个过滤条件做 AND
   对保留下来的行读取或输出 author、title
```

第 5 步对应 `MergeTreeReaderTextIndex::readRows`。它按 mark range 生成虚拟列：

```text
mark -> rows [mark_start, mark_end]
posting list for "clickhouse" -> [0, 2, 100, 8193, ...]

virtual column for this mark:
  row mark_start + 0 -> 0 or 1
  row mark_start + 1 -> 0 or 1
  ...
```

这里要注意两个边界：

- `body` 列不是投影列，也不再需要参与 `hasToken` 原谓词计算，所以这条路径可以避免读取 `body`；
- `publish_date` 仍然是普通过滤列，除非被主键或其他索引完全裁剪，否则还要读取并执行比较；
- `author`、`title` 是输出列，最终命中的行仍然需要读取它们。

如果当前 mark 的 postings 与 mark row range 没有交集，虚拟列全是 `0`，这一段数据会被过滤掉。若有交集，也只把命中的 row id 位置置为 `1`，后续再和 `publish_date` 条件一起过滤。

对精确 token 函数，例如 `hasToken`、`hasAnyTokens`、`hasAllTokens`，direct read 可以是 `Exact`。对 `like`、`startsWith`、`endsWith`、`hasPhrase`、某些 preprocessor 或 tokenizer 场景，direct read 可能只能作为 `Hint`：虚拟列先减少候选行，原始谓词仍要执行以保证正确性。

### 9.3 Pattern 查询

对于 `like`、`match` 等模式查询，代码会尝试从模式中提取必要 token 或扫描 dictionary blocks 找匹配 token。若匹配 token 太多，超过 `text_index_like_max_postings_to_read`，分析会进入 bypass：

```text
pattern scan too broad
  -> cannot build complete postings condition
  -> conservatively return true or use fallback expression
```

这保证不会因为不完整的倒排分析跳过真实匹配行。

### 9.4 Phrase 查询

当前 text index 不是 positional inverted index。posting list 只保存 token 出现在哪些 `Part` 内 row id 上，不保存 token 在原始字符串中的 position、offset 或第几次出现。因此索引本身不能判断短语里的 token 是否相邻、是否按顺序出现。

`hasPhrase` 的索引使用方式是必要条件过滤。`MergeTreeIndexConditionText::traverseAtomNode` 识别到 `hasPhrase` 后，会先检查 tokenizer 类型，只支持 `splitByNonAlpha`、`splitByString`、`ngrams`、`asciiCJK`。然后把 phrase 字符串按索引 tokenizer 切成 tokens，并构造 `TextSearchMode::All` 的 `TextSearchQuery`：

```text
hasPhrase(body, 'quick brown fox')
  -> tokenizer 切成 ['quick', 'brown', 'fox']
  -> FUNCTION_HAS_ALL_TOKENS
  -> postings('quick') ∩ postings('brown') ∩ postings('fox')
```

这只能证明候选行同时包含这些 token。例如一行包含 `quick ... fox ... brown`，索引层也可能认为它是候选，因为三个 token 都出现了。真正的 `hasPhrase` 语义仍要靠原始谓词在读取 `body` 后判断。

所以 `getDirectReadMode` 对 `hasPhrase` 只返回 `Hint` 或 `None`，不会返回 `Exact`。开启 hint 时，text index 可以生成一个候选过滤列来减少读取和后续计算；但原始 `hasPhrase` 表达式仍必须执行，保证不会把“只包含全部 token 但不是 phrase”的行误判为命中。

### 9.5 `TextSearchQuery` 数据结构

`MergeTreeIndexConditionText.h:43-54`：

| 字段 | 类型 | 含义 |
|---|---|---|
| `function_name` | `String` | 原始函数名（用于 hash 与虚拟列命名） |
| `search_mode` | `TextSearchMode` | `All` (AND) / `Any` (OR) |
| `direct_read_mode` | `TextIndexDirectReadMode` | `None` / `Exact` / `Hint` |
| `tokens` | `VectorWithMemoryTracking<String>` | 构造时已排序的 token 集合 |
| `patterns` | `vector<OptimizedRegularExpression>` | `like` / `ilike` 时的编译后正则 |

构造时 `tokens` 自动排序，便于后续按字典序合并多查询、共享 dictionary 读取。

`isSupportedFunction` 完整列表（`MergeTreeIndexConditionText.cpp:196-219`）：

```cpp
hasToken / hasTokenOrNull / hasAnyTokens / hasAllTokens / hasPhrase
equals  /  has  /  hasAll  /  hasAny
mapContainsKey / mapContainsKeyLike / mapContainsValue / mapContainsValueLike
like / ilike / startsWith / endsWith
match / multiSearchAny / multiSearchAnyUTF8 / multiMatchAny
```

### 9.6 Exact / Hint / None 直读模式判定表

`getDirectReadMode`（`MergeTreeIndexConditionText.cpp:227-268`）按函数类型 + tokenizer + preprocessor 决定模式：

| 函数 | ArrayTokenizer + 无 preprocessor | 其他 tokenizer / 有 preprocessor |
|---|---|---|
| `hasToken` / `hasAnyTokens` / `hasAllTokens` | **Exact** | **Exact** |
| `equals` / `has` / `mapContainsKey` / `mapContainsValue` / `hasAll` | **Exact** | Hint 或 None |
| `hasAny` | **Exact** | **None**（多 query 不可 Hint） |
| `like` / `hasPhrase` / `startsWith` / `endsWith` | Hint 或 None | Hint 或 None |
| `match` / `multiSearchAny` / `multiMatchAny` | **None** | **None** |
| `like '%foo%'`（dict scan 命中且超限保护通过） | **Exact** | — |

判定要点：

- **Exact**：虚拟列直接代替原谓词，posting list 命中即结果；
- **Hint**：虚拟列只是提示，必须再走原谓词过滤；
- **None**：完全不参与 direct read，只能走传统 skip index 路径。

设置 `query_plan_text_index_add_hint` 决定 Hint vs None 的退化（`MergeTreeIndexConditionText.cpp:221-225`）。

### 9.7 完整查询路径决策图

```
SQL 谓词
   │  RPNBuilder 遍历谓词 DAG
   │  traverseAtomNode → 识别 supportedFunction
   │  生成 TextSearchQuery：(function_name, search_mode, direct_read_mode, tokens, patterns)
   │  汇总 all_search_tokens、global_search_mode
   │
   ▼
┌──────────────────────────────┐
│  direct_read_mode == None?    │
└──────┬───────────────┬───────┘
     YES                NO
       │                │
       ▼                ▼
┌───────────────────┐  ┌────────────────────────────────────────┐
│  普通 Skip 路径    │  │  Direct Read 路径                      │
├───────────────────┤  │  replaceToVirtualColumn                │
│  reader.read(...) │  │  虚拟列名 __text_index_<idx>_<func>_<h>│
│  granule.setCurrent│  ├────────────────────────────────────────┤
│  Range(rows_range)│  │  MergeTreeReaderTextIndex::readRows()  │
│  condition->mayBe │  │   readGranule() 拉三流                  │
│  TrueOnGranule()  │  │     ├─ Regular   → .idx header          │
│   ├─hasAllQuery   │  │     ├─ Dictionary → .dct.idx           │
│   │  TokensOrEmpty│  │     └─ Postings   → .pst.idx (按需)    │
│   ├─hasAnyQuery   │  │   classifyVirtualColumns()             │
│   │  Tokens       │  │     ├─[is_always_true] → 全 1 填充     │
│   │ 求 posting    │  │     ├─[use_fallback]   → 物理列原谓词 │
│   │ ∩ mark_range  │  │     ├─[use_lazy_mode]  → cursor 解码  │
│   └─返回 true/false│  │     │     lazyIntersect/lazyUnion     │
│                   │  │     │     PostingLists                 │
│  → 裁剪 mark range │  │     └─[eager mode]                     │
└───────────────────┘  │           buildPostingsForMark         │
                       │           Roaring AND range → fill UInt8│
                       └────────────────────────────────────────┘
```

### 9.8 `mayBeTrueOnGranule` 实现细节

`MergeTreeIndexText.cpp:729-805` 中的 `hasAllQueryTokensOrEmpty` / `hasAnyQueryTokens` 逻辑：

```
1. query_builder.is_failed → return false（必要 token 缺失，granule 一定不命中）
2. bypass 且有 pattern    → return true（保守，避免漏行）
3. rows_range（token 出现的行范围）∩ current_range（当前 mark）
   ├─ 无交集    → return false（裁剪）
   └─ 有交集
        ├─ !needReadPostings → return true（无法精确判定）
        └─ 已加载 postings   → 精确 Roaring AND
```

模式差异：

- `FUNCTION_HAS_ALL_TOKENS`（All）：`QueryBuilder.rows_range` 是各 token 行范围的**交集**；
- `FUNCTION_HAS_ANY_TOKENS`（Any）：`rows_range` 是**并集**；
- `FUNCTION_LIKE`：走 `hasAnyTokensImpl`，但检查 `patterns` 字段；
- `global_search_mode = Any`（含 OR/NOT/hasAnyTokens）：`requiresReadingAllTokens` 返回 true，字典扫描不能提前退出。

`mark → row range` 转换由 `MergeTreeDataSelectExecutor.cpp:2056-2062` 完成：

```cpp
size_t row_begin = part->index_granularity->getMarkStartingRow(mark);
size_t row_end   = part->index_granularity->getMarkStartingRow(mark + 1);
granule_text.setCurrentRange(RowsRange(row_begin, row_end - 1));
```

### 9.9 Pattern 查询 bypass 决策流程

`like` 等模式查询的 token 提取与字典扫描流程：

```
like '%foo%'
   │
   ▼
stringLikeToPatterns()                     ← MergeTreeIndexConditionText.cpp:596-658
   ├─ 成功（单段字母数字 + 两侧 %，长度 ≥ text_index_like_min_pattern_length）
   │     → 生成 OptimizedRegularExpression，tokens 留空
   │     → direct_read_mode = Exact
   │     → 进入 Dict Scan 路径
   └─ 失败（复杂模式）
         → fallback 到 stringLikeToTokens：从 % 之间抽出 token 当 Hint

Dict Scan 路径（analyzeDictionaryForPatterns，MergeTreeIndexText.cpp:513-575）：
   │
   ├─ 遍历 sparse_index 每个 dictionary block
   │     反序列化 token 列表
   │     for token: analyzer->addTokenToPatterns(token)（正则匹配）
   │
   ├─ 累计大 posting token 数（non-embedded）→ postings_to_read
   │
   ├─ if postings_to_read > text_index_like_max_postings_to_read:
   │       analyzer->bypassPatternQueries()
   │       ProfileEvents::increment(TextIndexDiscardPatternScan)
   │       return
   │
   └─ 否则 → buildPostingsForQuery() 正常生成虚拟列

classifyVirtualColumns 检测到 is_bypassed：
   ├─ Hint  模式 → is_always_true[i] = true     ← 不过滤，依赖原谓词
   └─ Exact 模式 → use_fallback[i] = true       ← 回退读物理列 + 原谓词
```

关键设计：bypass 时不报错，而是回退到一种"必然正确但较慢"的路径，保证查询语义不破坏。

### 9.10 Lazy 模式与 cursor 复用

`MergeTreeReaderTextIndex` 在 direct read 模式下两种执行风格：

- **eager**：把 posting list 完全 OR 到一个 Roaring，再展平到 UInt8 列；适合 dense；
- **lazy**：每个 `(virtual_column, token)` 对应一个 `PostingListCursor`，按 mark 顺序前进；适合稀疏。

`lazyIntersectPostingLists` 自适应选择：

- density 低 → leapfrog（cursor 按升序 cardinality 排序，short-cursor 驱动 advance）；
- density 高 → bitmap counting；

反向跳 mark 时 `lazy_cursors.clear()`（cursor 单向前进，不支持回退）；`prebuilt_cursors` 缓存基于 analyzer 预折叠 postings 构建的 cursor，可跨 mark 复用。

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

#### `MergedPartOffsets` 行号映射数据结构

`MergedPartOffsets`（`src/Storages/MergeTree/MergedPartOffsets.h:182-264`）使用 **Frame-of-Reference + bit-packing** 压缩存储映射：

- 内部为 `std::vector<PackedPartOffsets> offset_maps`，每个源 part 一个；
- `PackedPartOffsets` 每 1024 行一个 Page：
  - 存 `min_val`（页内最小值）
  - 位宽 `bits = 64 - leading_zeros(max_val - min_val)`
  - 后续 1023 个值用 `bits` 位 packed 存差值；
- 支持 O(1) 随机读：`page = i >> 10`，`offset = i & 0x3FF`。

`insert` 时机：`ExecuteAndFinalizeHorizontalPart::execute()` 主循环里，每个 block 拿到的 `_part_index` 虚拟列被逐元素喂入（`MergedPartOffsets.h:199-207`）：

```cpp
void insert(const UInt64 * begin_part_index, const UInt64 * end_part_index)
{
    for (const UInt64 * it = begin_part_index; it != end_part_index; ++it)
    {
        offset_maps[*it].insert(num_rows);  // 新 part 的当前行号
        ++num_rows;
    }
}
```

内存量级：N 行、K 个 part，平均每 page 约占 `(bits_per_val * 1023 / 8)` 字节。两个 part 均匀交叉合并时 `bits_per_val ≈ 1`，1 亿行只占约 12 MB。

#### `MergeTextIndexesTask` k-way 归并

`TextIndexUtils.cpp:279-459`：

```
initializeQueue():
   for each segment:
       readDictionaryBlock(segment)
       push cursor_segment 入 priority_queue（按 token 字典序）

executeStep():
   while !queue.empty() && !timeout:
       current = queue.top()
       if isNewToken(current):
           if !output_postings.empty():
               flushPostingList()                  ← 写 .pst.idx
           if output_tokens.size() >= dictionary_block_size:
               flushDictionaryBlock()              ← 写 .dct.idx 块
           output_tokens.append(current.token)
       postings = readPostingLists(current.order)
       for p in postings:
           p = adjustPartOffsets(current.order, p) ← row id 重映射（若 merge_offsets 非空）
           output_postings |= p                     ← Roaring OR
       if cursor.isLast():
           queue.pop()
           readDictionaryBlock(current.order)       ← 补下一块
       else:
           queue.next()

finalize():
   flushPostingList(); flushDictionaryBlock()
   写 sparse index 到 .idx
```

`executeStep` 受 `background_task_preferred_step_execution_time_ms` 控制，按时间片协作让出，避免长 merge 阻塞调度。

#### row id 映射示意

```
源 Part A 行号:   row_A0 → off=0
                  row_A1 → off=1
                  row_A2 → off=2
源 Part B 行号:   row_B0 → off=0
                  row_B1 → off=1
                  row_B2 → off=2

merge 排序输出 _part_index 列： [0, 1, 0, 1, 0, 1]
新 Part 行号:                    0  1  2  3  4  5

offset_maps[0] (Part A):  [0, 2, 4]
offset_maps[1] (Part B):  [1, 3, 5]

adjustPartOffsets(0, [0, 1, 2]):
   → (*merged_part_offsets)[0, 0] = 0
   → (*merged_part_offsets)[0, 1] = 2
   → (*merged_part_offsets)[0, 2] = 4
   → 返回新 postings = [0, 2, 4]

token "x" 在源 A 出现于 [0, 1]、源 B 出现于 [2]
   → 调整后：A 的 [0,1] → [0,2]，B 的 [2] → [5]
   → OR 合并：[0, 2, 5]
```

### 10.5 与 mutation/materialize 协同

`ALTER TABLE ... MATERIALIZE INDEX` 或 mutation 路径会走 `BuildTextIndexTransform`。如果待 materialize 的 `Part` 行数超过 `UInt32` 上限，代码会拒绝构建，因为 posting list row id 当前用 `UInt32` 表示。

临时文件写在 `text_index_tmp` 存储中，最后由 `MergeTextIndexesTask` 或 mutation 收尾逻辑写入正式 `Part` 文件，并加入 `checksums.txt`。

### 10.6 与复制和远端存储协同

因为 text index 文件列在 `checksums.txt` 中，所以 `ReplicatedMergeTree` fetch `Part` 时会把 `.idx`、`.dct.idx`、`.pst.idx` 及 marks 一起传输和校验。对象存储场景下，text index 也按 `Part` 文件处理；读取层通过 index cache、mark cache、postings cache 降低随机读成本。

`MergeTextIndexesTask::addToChecksums()` 把所有输出流的哈希写进 `checksums.txt`（`TextIndexUtils.cpp:496-500`），`MergeTreeIndexText::getDeserializedFormat()` 通过检查 `.idx` 是否在 checksums 内来判断索引是否已物化（`MergeTreeIndexText.cpp:1597-1602`）。这让 part fetch / 校验 / 移动 / 删除全部走与普通列文件相同的代码路径。

### 10.7 与其他 skip index 的协同

`MergeTreeSkipIndexReader::read()`（`MergeTreeIndexReadResultPool.cpp:48-144`）顺序处理 `skip_indexes.useful_indices`，逐 index 调 `filterMarksUsingIndex()` 收紧 `ranges`。text index 通过 `MergeTreeIndexConditionText::mayBeTrueOnGranule()` 参与此过程。

若启用 `use_for_disjunctions`，每个 index 的 `update_partial_disjunction_result_fn` 回调（`MergeTreeIndexConditionText.cpp:415-417`）会更新 `partial_eval_results` 位图，最后 `mergePartialResultsForDisjunctions()` 合并 AND/OR 逻辑。

`MergeTreeIndexReadResultPool` 以原始指针 `const IMergeTreeDataPart *` 为 key 跨 task 共享同 part 的 `SkipIndexReadResult`（`MergeTreeIndexReadResultPool.h:215-217`），避免同一 part 被多线程重复过滤。

### 10.8 缓存体系

text index 使用三级专用缓存（`src/Storages/MergeTree/TextIndexCache.h`），全部基于 `CacheBase<UInt128, ...>`，key 为 SipHash-128：

```
┌──────────────────────────────────────────────────────────┐
│  TextIndexHeaderCache                                     │
│    key   = hash(disk_name + full_path + index_file_name)  │
│    value = TextIndexHeader（含 sparse index）              │
│    用途  = 避免每次查询重新读 .idx                          │
├──────────────────────────────────────────────────────────┤
│  TextIndexTokensCache                                     │
│    key   = hash(index_id, token_string)                   │
│    value = TokenPostingsInfoPtr（offsets + ranges）        │
│    用途  = 字典扫描命中时缓存 token → posting 元数据        │
├──────────────────────────────────────────────────────────┤
│  TextIndexPostingsCache                                   │
│    key   = hash(index_id, file_offset, kind)              │
│    kind  = Roaring(0) / Segment(1) / Flat(2)              │
│    value = PostingListPtr | PostingListSegmentPtr |       │
│            FlatPostingsPtr                                │
│    用途  = posting block 解码结果缓存                       │
└──────────────────────────────────────────────────────────┘
```

`index_id_for_caches`（`MergeTreeIndexText.cpp:391`）：

```cpp
index_id_for_caches = fmt::format("{}:{}:{}",
    part_storage.getDiskName(),
    part_storage.getFullPath(),
    state.index.getFileName());
```

设计要点：

- `kind` 字段防止不同格式 cache 条目碰撞；
- `use_text_index_postings_cache = 0` → 退化为 query 级 local cache（多线程共享但不跨 query）；`= 1` → 全局共享，热点 part 的热 token segment 只解码一次；
- text index 不使用 `MarkCache`（只有 1 个 mark）、也不使用通用 `UncompressedCache`（`TextIndexUtils.cpp:535` 传 `nullptr`）；
- 对象存储场景下，`PostingListSegment` 一次解析后是不可变的，可放进 `TextIndexPostingsCache` 跨 query 共享，将昂贵的 Range 请求成本摊薄。

### 10.9 全局组件协作

```
MergeTreeData
  └─► (merge 触发)
       MergeTask
         ├─ ExecuteAndFinalizeHorizontalPart
         │    ├─ 每 source part 加 addBuildTextIndexesStep
         │    │    └─ BuildTextIndexTransform (Processor)
         │    │         ├─ MergeTreeIndexAggregatorText.update()
         │    │         └─ writeTemporarySegment → text_index_tmp/
         │    └─ pull loop: MergedPartOffsets.insert(_part_index)
         └─ MergeTextIndexStage
              └─ MergeTextIndexesTask (每个 index 一个)
                   ├─ SortingQueue<SortCursor> k-way merge
                   ├─ readDictionaryBlock() → .dct.idx
                   ├─ readPostingLists()    → .pst.idx
                   ├─ adjustPartOffsets()   ← MergedPartOffsets
                   ├─ flushPostingList()    → 输出 .pst.idx
                   ├─ flushDictionaryBlock()→ 输出 .dct.idx
                   └─ addToChecksums()      → checksums.txt

MutateTask
  └─► PartMergerWriter
        ├─ createBuildTextIndexesTask → BuildTextIndexTransform
        ├─ aggregate(block) per row
        └─ finalizeTempProjectionsAndIndexes
             └─ MergeTextIndexesTask（merged_part_offsets = nullptr）
                  ↑ mutation 中 row 顺序不变，无需重映射

查询路径
  MergeTreeSelectProcessor
    └─ MergeTreeIndexReadResultPool
         └─ MergeTreeSkipIndexReader.read()
              ├─ MergeTreeIndexConditionText.mayBeTrueOnGranule()  ← skip 路径
              └─ (direct read) MergeTreeReaderTextIndex
                   ├─ TextIndexHeaderCache.getOrSet(index_id)
                   ├─ TextIndexTokensCache.getOrSet(index_id, token)
                   └─ TextIndexPostingsCache.getOrSet(index_id, offset, kind)
```

### 10.10 关键设计要点汇总

| 方面 | 设计决策 |
|---|---|
| row id 映射 | FoR + bit-packing，O(1) 随机读，构建在 horizontal merge 主循环内无额外 I/O |
| merge 算法 | k-way merge by token，按时间片协作（`executeStep` 受 step_time_ms 限制） |
| 已有 index 复用 | `getDeserializedFormat()` 检查 checksums，源 part 已有 index 直接 segment 复用，无需重建 |
| 缓存键 | `disk:full_path:index_file_name` → SipHash-128，跨 part / 磁盘 / 索引名隔离 |
| mutation 无映射 | `merged_part_offsets = nullptr`，row 顺序不变 |
| 临时目录 | `text_index_tmp/`，`commitTransaction()` 后供 merge task 读，最终 `removeRecursive()` |
| 对象存储适配 | whole-file open + block offset 精确 seek，缓存解码结果摊薄 Range 请求成本 |

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
