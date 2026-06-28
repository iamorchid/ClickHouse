# `MergeTree` 列存储与序列化分析

本文基于当前仓库源码整理 `MergeTree` 对各种 ClickHouse SQL 类型的磁盘存储方式。重点是列数据在 data part 中如何拆成 substream、如何处理 `NULL`、文件 layout、mark layout 和压缩链路。

## 1. 总体模型

`MergeTree` 的列存储不是直接由 `ColumnVector`、`ColumnString` 这类内存列决定，而是由 `IDataType` 对应的 `ISerialization` 决定。

核心分工：

- `DataTypeFactory` 注册 SQL 类型族：数值、时间、字符串、`Array`、`Tuple`、`Map`、`Nullable`、`LowCardinality`、`Variant`、`Dynamic`、`JSON`、`AggregateFunction`、`QBit`、Geo domain 等。
- 每个 `IDataType` 返回一个 `ISerialization`。`ISerialization::enumerateStreams` 枚举该类型需要写到磁盘的 substream。
- `MergeTreeDataPartWriterWide` / `MergeTreeDataPartWriterCompact` 把 substream 映射为 part 内文件或共享数据段。
- `Serialization*::serializeBinaryBulk*` 负责把 column 的行范围写到对应 substream。
- `CompressedWriteBuffer` 按块压缩 substream 内容；每个压缩块有 checksum、codec method、压缩/未压缩大小等内部 header。

源码入口：

- `src/DataTypes/Serializations/ISerialization.h`
- `src/DataTypes/Serializations/ISerialization.cpp`
- `src/Storages/MergeTree/MergeTreeDataPartWriterWide.cpp`
- `src/Storages/MergeTree/MergeTreeDataPartWriterCompact.cpp`
- `src/Storages/MergeTree/MergeTreeWriterStream.cpp`
- `src/Compression/CompressedWriteBuffer.cpp`
- `src/Compression/CompressionFactory.cpp`

## 2. Wide part 与 Compact part 的文件 layout

### 2.1 Wide part

`Wide` part 中，每个实际 substream 通常生成一组独立文件：

- `<stream>.bin`：压缩后的数据流。
- `<stream>.mrk2` 或压缩 mark 文件：每个 granule 的定位信息。

`MergeTreeDataPartWriterWide::addStreams` 对每个 substream 创建 `MergeTreeWriterStream`。`MergeTreeWriterStream` 内部有：

- `plain_file`：最终落盘文件。
- `plain_hashing`：计算落盘文件 hash。
- `compressor`：`CompressedWriteBuffer`。
- `compressed_hashing`：统计未压缩数据流 hash 和未压缩字节数。
- mark 文件对应的 `marks_file`、`marks_hashing`、`marks_compressor`、`marks_compressed_hashing`。

写 mark 时记录：

```text
offset_in_compressed_file
offset_in_decompressed_block
[rows_in_mark, only when adaptive granularity is enabled]
```

其中：

- `offset_in_compressed_file` 是 `.bin` 中压缩块起始偏移。
- `offset_in_decompressed_block` 是该压缩块解压后内部偏移。
- 自适应 granularity 下额外写 granule 行数。

### 2.2 Compact part

`Compact` part 中，多列、多 substream 共享：

- `data.bin`
- `data.mrk*`

`MergeTreeDataPartWriterCompact` 按 codec 维护多个 `CompressedStream`，但最终都写入同一个 `data.bin`。每个 granule 中，writer 依次写所有 columns/substreams，并在共享 marks 文件中按 substream 顺序写 mark。

`Compact` 写入有两个重要特征：

- `position_independent_encoding = true`，例如 `Array` 的 offsets 写成每行 size，而不是全局累计 offset。
- `low_cardinality_max_dictionary_size = 0`，使 `LowCardinality` 更偏向按 granule 的局部形式写入，避免跨 granule 大状态。

### 2.3 文件名与 substream 命名

`ISerialization::getFileNameForStream` 由列名和 substream path 生成文件名前缀。典型后缀：

| Substream | 文件名/子列后缀 |
|---|---|
| `Regular` | 无额外后缀 |
| `NullMap` | `.null` |
| `ArraySizes` | `.size0`、`.size1` |
| `StringSizes` | `.size` |
| `DictionaryKeys` | `.dict` |
| `DictionaryKeysPrefix` | `.dict_prefix` |
| `DictionaryIndexes` | 通常是主路径下 index stream |
| `SparseElements` | `.sparse` |
| `SparseOffsets` | `.sparse.idx` |
| `VariantDiscriminators` | `.variant_discr` |
| `VariantDiscriminatorsPrefix` | `.variant_discr_prefix` |
| `VariantElement` | `.<variant-name>` |
| `DynamicStructure` | `.dynamic_structure` |
| `ObjectStructure` | `.object_structure` |
| `ObjectSharedData` | `.object_shared_data` |

`Nested` 是命名约定，不是单独物理类型。默认 `share_nested_offsets = true` 时，`n.a Array(T)` 和 `n.b Array(U)` 会共享 `n.size0` offsets stream。

## 3. 压缩模型

### 3.1 压缩选择顺序

列数据 codec 选择顺序来自 `MergeTreeSettings`：

1. 列定义中的 `CODEC(...)`。
2. 表级 `default_compression_codec`。
3. 全局 `compression` 配置经 `Context::chooseCompressionCodec` 选择。

TTL recompression 可覆盖 part merge 输出 codec：`MergeTreeData::getCompressionCodecForPart` 会优先检查 recompression TTL。

marks 和 primary key 独立配置：

- `compress_marks = true`
- `marks_compression_codec = ZSTD(3)`
- `compress_primary_key = true`
- `primary_key_compression_codec = ZSTD(3)`
- 默认 block size 65536。

### 3.2 codec 类型

当前工厂注册的主要 codec：

- Generic compression：`LZ4`、`LZ4HC`、`ZSTD`、`NONE`。
- Pre-transform / specialized codec：`Delta`、`DoubleDelta`、`GCD`、`T64`、`Gorilla`、`FPC`、`ALP`。
- Wrapper / post-processing：`Multiple`、`Encrypted`。

`CompressionCodecFactory::get` 会把 `CODEC(Delta, ZSTD)` 组合成 `CompressionCodecMultiple`。当 `only_generic = true` 时，会过滤掉非 generic codec。

`MergeTreeDataPartWriterWide::addStreams` 对某些 substream 禁止使用 special codec，只保留 generic codec。`ISerialization::isSpecialCompressionAllowed` 禁止 special codec 的流包括：

- `NullMap`
- `ArraySizes`
- `StringSizes`
- `DictionaryIndexes`
- `SparseOffsets`

原因是这些流本身是辅助元数据或 index，按业务类型套 `Delta` / `Gorilla` 等通常没有语义，甚至可能不满足类型约束。

### 3.3 压缩块格式

`CompressedWriteBuffer::nextImpl` 对当前未压缩 buffer 做：

1. 调用 codec 压缩。
2. 计算压缩后 payload 的 `CityHash128`。
3. 先写 checksum 的 low64 / high64。
4. 再写 codec 产生的压缩块内容。

codec 内容本身带 ClickHouse 内部 header，包含 method byte、compressed size、decompressed size。读时 `CompressedReadBufferBase` 先校验 checksum，再通过 method byte 找 codec。

## 4. 各类型族的磁盘布局

下面按底层 `Serialization*` 归类。很多 SQL 类型共享同一套二进制 layout。

### 4.1 固定宽度数值类型

覆盖：

- `UInt8/16/32/64/128/256`
- `Int8/16/32/64/128/256`
- `Float32/64`
- `BFloat16`
- `Date`、`Date32`
- `DateTime`、`DateTime32`
- `Time`、`Time64`
- `Decimal32/64/128/256`
- `Enum8/16`
- `IPv4`
- `UUID`
- `Bool`
- `Interval*`

底层形态：

- 多数是 `ColumnVector<T>`。
- bulk 写入为连续 little-endian 原始字节。
- `Enum8/16` 存储为 `Int8/Int16` 值，名字只参与 SQL 文本格式和类型元信息。
- `Decimal*` 存储 scaled integer 原始值，scale/precision 在类型元信息中，不逐值存储。
- `Date` 是 `UInt16` 天数，`Date32` 是 `Int32` 天数。
- `DateTime` / `DateTime32` 是秒级整数，`DateTime64` 是带 scale 的 `Decimal64` 风格整数。
- `Bool` 是 `UInt8` 的 custom domain，磁盘上仍按 `UInt8` 存。
- `UUID` 是 16 字节 `UInt128` 风格存储。
- `IPv4` 是 4 字节值，`IPv6` 是 16 字节值；`IPv6` bulk 直接写底层 16 字节表示。

layout：

```text
<column>.bin:
  row0 raw bytes
  row1 raw bytes
  ...
```

无额外 substream。`Nullable(T)` 包装后见 4.6。

压缩：

- 默认对整个 `<column>.bin` 走列 codec。
- 固定宽度类型允许 `Delta`、`DoubleDelta`、`GCD`、`T64` 等 special codec，取决于 codec 对类型大小和类型族的检查。

### 4.2 `String`

当前仓库支持两种 MergeTree serialization version：

- `single_stream`
- `with_size_stream`，当前默认。

#### `single_stream`

每个字符串写成：

```text
VarUInt(length)
raw bytes
```

layout：

```text
<column>.bin:
  len0 bytes0 len1 bytes1 ...
```

读取跳过行时必须逐个读 `VarUInt(length)` 再跳过 payload。

#### `with_size_stream`

`SerializationString::enumerateStreamsWithSize` 生成两个流：

```text
<column>.size.bin:
  UInt64 length for each row

<column>.bin:
  concatenated raw bytes
```

`StringSizes` 流写 `UInt64` 长度数组，`Regular` 流只写拼接后的 bytes。优点：

- `.size` 子列是真实可读流。
- 长度流和内容流可以独立压缩。
- 读取部分行时可以先从 size stream 算出 bytes_to_skip / bytes_to_read。

注意：

- `StringSizes` 禁止 special codec，只走 generic codec。
- `string_serialization_version` 默认只影响 top-level `String`，但 `propagate_types_serialization_versions_to_nested_types = true` 时会传播到嵌套类型。

### 4.3 `FixedString(N)`

`FixedString(N)` 每行固定写 N 字节，不存长度：

```text
<column>.bin:
  N bytes row0
  N bytes row1
  ...
```

不足 N 的值在单值 binary 序列化时补 `\0`。bulk layout 固定宽度，支持按 mark 偏移快速 seek。

### 4.4 `Array(T)`

`Array` 拆成两个逻辑部分：

- `ArraySizes`：每一行 array 的大小或 offset。
- `ArrayElements`：所有元素扁平拼接后按 nested type 序列化。

Wide part 默认 `position_independent_encoding = false`，`ArraySizes` 实际写 `ColumnArray::Offsets` 的累计 offset：

```text
arr.size0.bin:
  offset0, offset1, offset2, ...

arr.bin / arr.<nested-substream>.bin:
  all elements of all rows, flattened
```

Compact part 设置 `position_independent_encoding = true`，`ArraySizes` 写每行 size：

```text
data.bin:
  size0, elements(row0), size1, elements(row1), ...
```

多维数组按嵌套层级生成多个 `.sizeN`：

```text
Array(Array(UInt32)):
  col.size0.bin          -- 外层 array sizes/offsets
  col.size1.bin          -- 内层 array sizes/offsets
  col.bin                -- UInt32 flattened values
```

`Nested` dotted columns 默认共享第一层 offsets，例如：

```sql
n.a Array(UInt32), n.b Array(String)
```

共享：

```text
n.size0.bin
```

各元素各自写数据 stream。

### 4.5 `Tuple(...)` / named tuple / Geo domain

`Tuple` 没有整体 payload，而是每个元素各自递归序列化：

```text
tuple_col.1.bin
tuple_col.2.bin
...
```

如果 tuple 有显式名字，文件名使用元素名后缀：

```text
tuple_col.x.bin
tuple_col.y.bin
```

Geo domain 是 custom data type，底层映射到 `Tuple` / `Array`：

- `Point` -> `Tuple(Float64, Float64)`
- `LineString` -> `Array(Point)`
- `Ring` -> `Array(Point)`
- `Polygon` -> `Array(Ring)`
- `MultiPolygon` -> `Array(Polygon)`
- `Geometry` -> 多种几何类型组合

因此 Geo 类型没有额外专用 MergeTree layout，继承 `Tuple` / `Array` / `Variant` 等底层布局。

### 4.6 `Nullable(T)`

`Nullable` 拆成：

- `NullMap`：每行一个 `UInt8`，1 表示 `NULL`，0 表示非 `NULL`。
- `NullableElements`：nested value stream。

layout：

```text
col.null.bin:
  UInt8 null flag per row

col.bin / col.<nested-substream>.bin:
  nested values for every row
```

关键点：

- nested stream 对 `NULL` 行也有占位值。这个值未必有业务意义，只用于保持行数对齐。
- 读完后会校验 null map 和 nested column size 一致。
- 单值 binary 格式是先写一个 bool `is_null`，非 null 再写 nested value；但 MergeTree bulk 格式使用独立 null map stream。
- `NullMap` 禁止 special codec，只使用 generic codec。
- `Nullable(Nullable(T))` 这类嵌套通常不允许，具体由类型系统约束。

### 4.7 `LowCardinality(T)`

`LowCardinality` 存储 dictionary + indexes，主要 substream：

- `DictionaryKeysPrefix` 或 `DictionaryKeys`
- `DictionaryKeys`
- `DictionaryIndexes`

格式版本：

```text
DictionaryKeysPrefix / DictionaryKeys:
  UInt64 key_version = SharedDictionariesWithAdditionalKeys
```

字典流写：

```text
UInt64 num_keys
keys serialized by nested non-null serialization
```

indexes 流每个 granule 写：

```text
UInt64 index_serialization_type_and_flags
[UInt64 num_dictionary_keys + dictionary keys]       if dictionary update is needed
[UInt64 num_additional_keys + additional keys]       if additional keys exist
UInt64 num_rows
indexes serialized as UInt8/UInt16/UInt32/UInt64
```

flags 表示：

- 是否需要全局 dictionary。
- 是否有 additional keys。
- 是否需要更新 dictionary。
- index 类型宽度。

`LowCardinality(Nullable(T))` 的 `NULL` 处理：

- dictionary type 可以是 nullable。
- 实际 keys 序列化常使用 remove-nullable 后的 nested serialization。
- 当不使用全局 dictionary 且 dictionary type nullable 时，代码会为 additional keys 构造 null map，并约定第 0 个 key 可代表 null。

Wide part 可跨 granule 共享 dictionary，并可检测“整个 part 单 dictionary”。Compact part 设置 `low_cardinality_max_dictionary_size = 0`，倾向写局部 keys/indexes。

### 4.8 `Map(K, V)`

基础 `Map` 本质是：

```text
Array(Tuple(K, V))
```

因此 basic layout 是：

```text
map.size0.bin       -- 每行 map entry 数
map.keys...         -- key column streams
map.values...       -- value column streams
```

单值 binary 格式是：

```text
VarUInt map_size
key0 value0 key1 value1 ...
```

MergeTree bulk 格式则复用 `Array` + `Tuple` 的 substream。`Map` 的 null 语义由 key/value 类型决定；通常 key 不应是 nullable，value 可以是 `Nullable(V)`。

当前设置支持：

- `map_serialization_version = basic`
- `map_serialization_version = with_buckets`

`with_buckets` 会按 key hash / bucket 策略拆成多个 bucket substream，用于优化读取单个 map key。bucket 数由 `max_buckets_in_map`、`map_buckets_strategy`、`map_buckets_coefficient`、`map_buckets_min_avg_size` 控制。

### 4.9 `Variant(...)`

`Variant` 存储 discriminator + 每个备选类型的元素流。

主要 substream：

```text
col.variant_discr[_prefix].bin
col.<variant-name>...bin
```

`VariantDiscriminators`：

- prefix 里写 discriminator serialization mode。
- basic 模式按行写 global discriminator。
- compact 模式对 granule 做压缩表达：如果全是同一种 variant 或全是 null，可只写格式标记和 discriminator；否则写 plain discriminator 序列。

每个 variant element 子流只写属于该 variant 的值集合，不是每行都写。恢复完整列时通过 discriminator 把各 variant 子列拼回逻辑行序。

`NULL`：

- `Variant` 有 `NULL_DISCRIMINATOR`。
- 对可抽取为 nullable 的 variant subcolumn，代码提供 `VariantElementNullMap` 作为虚拟/ephemeral subcolumn；它不一定单独落真实文件。

### 4.10 `Dynamic`

`Dynamic` 是运行时类型集合，核心 substream：

```text
col.dynamic_structure.bin
col.dynamic_data...
```

`DynamicStructure` 写结构版本和本 part / granule 中出现的动态类型集合。当前支持版本 `V1`、`V2`、`V3`，Native format 还可使用 flattened serialization。

典型 `V2/V3` 思路：

1. 在 structure stream 写 serialization version。
2. 写动态 variant 类型信息。
3. `DynamicData` 使用一个内部 `Variant` serialization 写实际数据。

Flattened 模式：

- structure stream 写所有 flattened types。
- data stream 中写 indexes 和每种 type 的 column。

`NULL` 由内部 `Variant` / nested nullable 逻辑表达。

### 4.11 `JSON` / `Object`

当前 SQL 类型名是 `JSON`，底层使用 `DataTypeObject` / `SerializationObject*`。版本设置：

- `object_serialization_version = v1/v2/v3`，当前默认 `v3`。
- `object_shared_data_serialization_version = map/map_with_buckets/advanced`，当前默认 `advanced`。
- compact / wide part 的 shared data bucket 数可分别配置。

逻辑上，JSON 被拆成：

- typed paths：已知固定路径，按对应类型单独写 substream。
- dynamic paths：动态出现的路径，通常用 `Dynamic` 类似机制。
- shared data：未独立提升为 typed/dynamic path 的剩余 key-value。

主要 substream：

```text
col.object_structure.bin
col.object_data.<typed-path>...
col.object_data.<dynamic-path>...
col.object_shared_data...
```

`advanced` shared data 还会写：

- `.structure_prefix` / `.structure_suffix`
- `.data`
- `.paths_marks`
- `.substreams`
- `.substreams_marks`
- `.paths_substreams_metadata`
- `.copy.sizes`
- `.copy.paths_indexes`
- `.copy.values`

这些额外元数据用于从 shared data 中更高效地读取单个 path，代价是更多磁盘元数据。

JSON 的 `NULL` 需要区分两层：

- SQL `Nullable(JSON)`：外层 `NullMap`。
- JSON 文档内部的 `null`：由 JSON/Object 自身的动态值、shared data 或 nested nullable 表达。

### 4.12 `AggregateFunction` 与 `SimpleAggregateFunction`

`AggregateFunction(...)` 存储 aggregate state 二进制，由对应 aggregate function 自己实现：

```text
function->serialize(state, out, version)
```

bulk 写入使用：

```text
function->serializeBatch(...)
```

读取时在 `Arena` 中为每行 state 分配内存，再调用：

```text
function->createAndDeserializeBatch(...)
```

因此 layout 不由类型系统固定，而由具体聚合函数版本和 state variant 决定。

`SimpleAggregateFunction` 是 custom domain，通常底层存储为其 result/storage type，不是 `ColumnAggregateFunction` state layout。

### 4.13 `Nothing`

`Nothing` 没有真实业务值。当前 `SerializationNothing::serializeBinaryBulk` 对每行写一个字节 `'0'`，读取时按读到的字节数增加 `ColumnNothing` size。

实际表列中 `Nothing` 主要出现在特殊推导或 `Nullable(Nothing)` 等场景。

### 4.14 `QBit(T, N)`

`QBit` 支持 `BFloat16`、`Float32`、`Float64` 元素。内存上是 `ColumnQBit`，底层嵌套为一个 `Tuple`，tuple 每个元素是 `FixedString(bitsToBytes(N))`。

直观理解：

```text
QBit(Float32, N)
  -> Tuple(FixedString(bitsToBytes(N)) x 32)
```

每个 tuple element 是一个 bit-plane。磁盘 bulk layout 继承 `Tuple` + `FixedString`：

```text
qbit.1.bin
qbit.2.bin
...
qbit.32.bin
```

单值 binary/text 输入输出时，`SerializationQBit` 会在普通 float vector 和 bit-plane tuple 之间做 transpose / untranspose。

### 4.15 Sparse / Detached / Replicated serialization kind

`ISerialization::KindStack` 支持 wrapper：

- `DEFAULT`
- `SPARSE`
- `DETACHED`
- `REPLICATED`

`SPARSE` 用于默认值占比高的列：

```text
<path>.sparse.idx    -- non-default value offsets, VarUInt run-length style
<path>.sparse...     -- non-default values
```

offset stream 使用 run-length 表示默认值组长度，并用高位 flag 标记 granule 结束。`SparseNullMap` 是 ephemeral subcolumn，不直接落真实文件。

`DETACHED` 用于 blob-like detached column wrapper；`REPLICATED` 用于重复值索引 + 元素集合的 wrapper。这些是 serialization wrapper，不是 SQL 顶层类型族。

## 5. `NULL` 处理总览

| 场景 | 磁盘表达 |
|---|---|
| `Nullable(T)` | 独立 `.null` stream，1 byte/row；nested stream 仍有占位值 |
| `LowCardinality(Nullable(T))` | `NULL` 可进入 dictionary；局部 additional keys 场景可构造 null map |
| `Array(Nullable(T))` | array size stream + nested nullable 的 `.null` stream，null map 行数等于 flattened element 数 |
| `Nullable(Array(T))` | 外层 `.null` 行数等于表行数；array sizes 和 elements 仍按 nested column 对齐 |
| `Map(K, Nullable(V))` | map size stream + value nested `.null`，null map 行数等于 flattened entry 数 |
| `Variant` | discriminator 可为 `NULL_DISCRIMINATOR`；variant subcolumn null map 可虚拟生成 |
| `JSON` 内部 null | 由 Object/Dynamic/shared data 表达，不等价于 SQL 外层 `Nullable(JSON)` |

一个容易出错的点：`Nullable(T)` 的 nested stream 对 `NULL` 行不省略数据。它写的是占位值，保证 nested column 与 null map 行数一致。

## 6. 读路径与 mark 定位

`MergeTreeReaderStream` 读取时：

1. 加载 marks。
2. 对目标 mark 取 `MarkInCompressedFile`。
3. 如果是压缩流，调用 `CompressedReadBuffer*::seek(offset_in_compressed_file, offset_in_decompressed_block)`。
4. 调用对应 `Serialization*::deserializeBinaryBulkWithMultipleStreams`。

对于多 substream 类型，reader 会为每个 substream 准备 getter。`Array`、`Nullable`、`String(with_size_stream)`、`LowCardinality` 等 serialization 会按自己的 path 读取多个 stream。

substream cache 用于避免重复读：

- `String` 的 `.size` 可被 `.size` 子列和完整 `String` 读取复用。
- `Nullable` 的 `.null` 可被 null 子列和完整列复用。
- `Sparse` offsets、`LowCardinality` dictionary 等也有状态缓存。

## 7. 类型族总结表

| SQL 类型族 | 主要内存列 | MergeTree substream | NULL 处理 | 典型压缩 |
|---|---|---|---|---|
| `Int*` / `UInt*` / `Float*` / `BFloat16` | `ColumnVector<T>` | `Regular` | 需外层 `Nullable` | 可用 default / `Delta` / `DoubleDelta` / `GCD` / `T64` 等 |
| `Decimal*` | `ColumnDecimal<T>` / vector-like | `Regular` | 需外层 `Nullable` | 可用 numeric special codec |
| `Date` / `Date32` / `DateTime*` / `Time*` | vector-like | `Regular` | 需外层 `Nullable` | 常见 `Delta,ZSTD` |
| `Enum8/16` | `ColumnVector<Int8/Int16>` | `Regular` | 需外层 `Nullable` | numeric codec |
| `Bool` | `ColumnUInt8` | `Regular` | 需外层 `Nullable` | generic 或 byte-oriented codec |
| `UUID` | `ColumnVector<UUID>` | `Regular` | 需外层 `Nullable` | generic |
| `IPv4/IPv6` | `ColumnVector<IPv*>` | `Regular` | 需外层 `Nullable` | generic / fixed-size codec |
| `String` | `ColumnString` | `Regular` 或 `StringSizes + Regular` | 需外层 `Nullable` | size stream generic，data stream generic/special if allowed |
| `FixedString` | `ColumnFixedString` | `Regular` | 需外层 `Nullable` | generic |
| `Array(T)` | `ColumnArray` | `ArraySizes + nested` | nested 或外层 nullable | sizes generic，elements 按 nested |
| `Tuple` | `ColumnTuple` | 每元素递归 | 每元素或外层 nullable | 按元素 |
| `Map` | `ColumnMap` | `ArraySizes + key/value`；可 buckets | value 可 nullable | 按 key/value，buckets 独立 |
| `Nullable(T)` | `ColumnNullable` | `NullMap + nested` | 原生 null map | null map generic |
| `LowCardinality(T)` | `ColumnLowCardinality` | `DictionaryKeys + DictionaryIndexes` | dictionary 可含 null | keys 按 T，indexes generic |
| `Variant` | `ColumnVariant` | `Discriminators + variant elements` | discriminator 表达 null | discr generic，elements 按类型 |
| `Dynamic` | `ColumnDynamic` | `DynamicStructure + DynamicData` | 内部 variant/dynamic 表达 | structure generic，data 按动态类型 |
| `JSON` | `ColumnObject` | object structure/data/shared data | SQL null 需外层 nullable；JSON null 内部表达 | shared data 多流，通常 generic |
| `AggregateFunction` | `ColumnAggregateFunction` | `Regular` | 需外层 nullable | 取决于 state bytes |
| `SimpleAggregateFunction` | storage/result column | 按底层类型 | 按底层类型 | 按底层类型 |
| `QBit` | `ColumnQBit` | tuple of fixed-string bit planes | 需外层 nullable | generic |
| `Nothing` | `ColumnNothing` | dummy byte stream | 常见于 `Nullable(Nothing)` | generic |

## 8. 关键结论

1. `MergeTree` 的“支持所有 column 类型”本质上是支持所有可注册 `IDataType` 的 `ISerialization`；复杂类型通过递归 substream 组合实现。
2. Wide part 更接近“一个 substream 一组文件”；Compact part 把 substream 串进共享 `data.bin`，marks 负责定位。
3. `NULL` 在 MergeTree bulk 格式中主要由独立 null map 表达；nested value stream 仍保持行数对齐。
4. `String` 当前默认是独立 `.size` stream + bytes stream，这和老的 inline `VarUInt length + bytes` 格式不同。
5. `Array` / `Map` 的核心是 offsets/sizes + flattened elements；`Nested` 只是共享 offsets 的命名约定。
6. `LowCardinality`、`Variant`、`Dynamic`、`JSON` 的 layout 都带有“结构/字典/判别符”元数据流，读路径依赖 deserialize state。
7. special codec 不是对所有 substream 都允许；null map、array sizes、string sizes、dictionary indexes、sparse offsets 只走 generic codec。
8. marks、primary key 和 column data 使用独立压缩配置；marks 默认 `ZSTD(3)`，列数据默认由全局 compression selector 或列/表 codec 决定。
