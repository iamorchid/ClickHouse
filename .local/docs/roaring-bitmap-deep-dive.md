# Roaring Bitmap 原理详解

## 1. Roaring bitmap 解决什么问题

Bitmap 常用来表示一个整数集合：

```text
集合 S = {1, 2, 5, 100}

bitmap:
bit 1   = 1
bit 2   = 1
bit 5   = 1
bit 100 = 1
其他 bit = 0
```

如果整数范围很小，普通 bitmap 很高效。比如只表示 `[0, 1023]`，只需要 `1024 bit = 128 bytes`。

但如果整数范围很大，普通 bitmap 会浪费大量空间。比如只保存：

```text
{1, 1_000_000_000}
```

普通 bitmap 至少要覆盖到 `1_000_000_000` 这一位，大约需要：

```text
1_000_000_001 bit / 8 ≈ 119 MB
```

这显然不划算。另一种方案是有序数组：

```text
[1, 1_000_000_000]
```

它对稀疏集合很省空间，但做交集、并集、差集时不如 bitmap 直接。

Roaring bitmap 的核心目标是折中：

1. 稀疏时像有序数组一样省空间。
2. 稠密时像 bitmap 一样运算快。
3. 对不同局部区间选择不同表示。

## 2. 核心思想：按高 16 位分桶

Roaring bitmap 把 `UInt32` 空间拆成两层：

```text
32-bit value = high 16 bits + low 16 bits
```

也就是：

```text
value = (high << 16) + low
```

其中：

- `high` 决定这个值属于哪个桶。
- `low` 是桶内的偏移，范围固定是 `[0, 65535]`。

例子：

```text
value = 1
high = 0
low  = 1

value = 65_536
high = 1
low  = 0

value = 65_537
high = 1
low  = 1

value = 131_077
high = 2
low  = 5
```

集合：

```text
S = {1, 2, 65_536, 65_537, 131_077}
```

会被拆成：

```text
high = 0 -> lows {1, 2}
high = 1 -> lows {0, 1}
high = 2 -> lows {5}
```

Roaring bitmap 顶层保存一个有序的 `high -> container` 映射。每个 container 只负责一个 `high` 桶内的低 16 位集合。

结构上可以理解为：

```text
RoaringBitmap
  high 0 -> container for lows {1, 2}
  high 1 -> container for lows {0, 1}
  high 2 -> container for lows {5}
```

这样做有两个直接好处：

1. 每个 container 的 universe 固定是 `65536` 个值，局部结构很容易优化。
2. 集合运算可以按 `high` 桶对齐，只处理两边都相关的局部 container。

## 3. 三种 container

Roaring bitmap 的关键不只是分桶，而是每个桶可以选择不同的 container 表示。

常见实现中有三类 container：

| Container | 保存方式 | 适合场景 |
|---|---|---|
| `ArrayContainer` | 有序 `UInt16` 数组 | 稀疏集合 |
| `BitmapContainer` | 固定 `65536 bit` bitmap | 稠密集合 |
| `RunContainer` | 连续区间列表 | 长连续区间 |

### 3.1 `ArrayContainer`

如果一个桶里只有少量值，直接保存低 16 位数组最省空间。

例子：

```text
high = 10
lows = {3, 8, 1000}
```

`ArrayContainer` 保存：

```text
[3, 8, 1000]
```

空间大约是：

```text
3 * 2 bytes = 6 bytes
```

如果用普通 bitmap，一个桶固定需要：

```text
65536 bit = 8192 bytes
```

所以稀疏时数组明显更省。

### 3.2 `BitmapContainer`

如果一个桶里有很多值，数组就不划算了。

例子：

```text
high = 20
lows = {0, 1, 2, ..., 9999}
```

`ArrayContainer` 需要：

```text
10000 * 2 bytes = 20000 bytes
```

`BitmapContainer` 固定只需要：

```text
8192 bytes
```

而且做交集、并集时可以按机器字批量运算：

```text
AND: word_a & word_b
OR : word_a | word_b
AND NOT: word_a & ~word_b
```

一个 `BitmapContainer` 通常是 `1024` 个 `UInt64`：

```text
65536 bits / 64 = 1024 words
```

所以两个稠密 container 做交集就是循环 `1024` 次按位与。

### 3.3 `RunContainer`

如果桶内值形成长连续区间，用区间表示更省。

例子：

```text
lows = {100, 101, 102, ..., 5000}
```

`RunContainer` 可以保存成：

```text
[(start = 100, length = 4901)]
```

如果有多个连续段：

```text
lows = {1, 2, 3, 100, 101, 102, 103, 1000}
```

可以保存为：

```text
[(1, 3), (100, 4), (1000, 1)]
```

`RunContainer` 对连续数据非常省空间，也适合做 range 相关操作。

## 4. 为什么数组和 bitmap 的切换阈值常见是 `4096`

一个桶最多有 `65536` 个低位值。

`BitmapContainer` 固定大小：

```text
65536 bit = 8192 bytes
```

`ArrayContainer` 每个值用 `UInt16` 保存：

```text
cardinality * 2 bytes
```

当 cardinality 是 `4096` 时：

```text
4096 * 2 bytes = 8192 bytes
```

这正好和 bitmap 一样大。

所以很多 Roaring 实现会用类似规则：

```text
cardinality <= 4096 -> ArrayContainer
cardinality >  4096 -> BitmapContainer
```

这不是语义要求，而是空间和性能上的经验阈值。具体实现可能还会结合 `RunContainer`、延迟转换、序列化格式等因素做调整。

## 5. 插入过程例子

假设依次插入这些 row id：

```text
1, 2, 3, 65_536, 65_537, 65_538, 65_539, 131_072
```

先拆高低位：

| value | high | low |
|---:|---:|---:|
| `1` | `0` | `1` |
| `2` | `0` | `2` |
| `3` | `0` | `3` |
| `65_536` | `1` | `0` |
| `65_537` | `1` | `1` |
| `65_538` | `1` | `2` |
| `65_539` | `1` | `3` |
| `131_072` | `2` | `0` |

Roaring bitmap 变成：

```text
high = 0 -> ArrayContainer [1, 2, 3]
high = 1 -> ArrayContainer [0, 1, 2, 3]
high = 2 -> ArrayContainer [0]
```

如果继续往 `high = 1` 的桶里插入大量值：

```text
65_540, 65_541, ..., 70_000
```

`high = 1` 的 container cardinality 超过阈值后，可能从：

```text
ArrayContainer [0, 1, 2, ...]
```

转换成：

```text
BitmapContainer
bit 0 = 1
bit 1 = 1
bit 2 = 1
...
```

其他桶仍然可以保持 `ArrayContainer`。这就是“局部自适应”。

## 6. 交集运算例子

假设有两个集合：

```text
A = {1, 2, 3, 65_536, 65_537, 200_000}
B = {2, 3, 4, 65_537, 65_538, 300_000}
```

拆桶：

```text
A:
high = 0 -> {1, 2, 3}
high = 1 -> {0, 1}
high = 3 -> {3392}   // 200_000 = 3 * 65536 + 3392

B:
high = 0 -> {2, 3, 4}
high = 1 -> {1, 2}
high = 4 -> {37856}  // 300_000 = 4 * 65536 + 37856
```

计算 `A ∩ B` 时，先对齐顶层 `high`：

```text
high = 0 两边都有 -> container 交集
high = 1 两边都有 -> container 交集
high = 3 只有 A 有 -> 跳过
high = 4 只有 B 有 -> 跳过
```

桶内计算：

```text
high = 0:
{1, 2, 3} ∩ {2, 3, 4} = {2, 3}

high = 1:
{0, 1} ∩ {1, 2} = {1}
```

合回 32 位整数：

```text
high = 0, low = 2 -> 2
high = 0, low = 3 -> 3
high = 1, low = 1 -> 65_537
```

最终：

```text
A ∩ B = {2, 3, 65_537}
```

## 7. 并集运算例子

继续用上面的 `A` 和 `B`：

```text
A:
high = 0 -> {1, 2, 3}
high = 1 -> {0, 1}
high = 3 -> {3392}

B:
high = 0 -> {2, 3, 4}
high = 1 -> {1, 2}
high = 4 -> {37856}
```

计算 `A ∪ B`：

```text
high = 0:
{1, 2, 3} ∪ {2, 3, 4} = {1, 2, 3, 4}

high = 1:
{0, 1} ∪ {1, 2} = {0, 1, 2}

high = 3:
只有 A 有 -> 直接拷贝 {3392}

high = 4:
只有 B 有 -> 直接拷贝 {37856}
```

还原：

```text
A ∪ B = {
  1, 2, 3, 4,
  65_536, 65_537, 65_538,
  200_000,
  300_000
}
```

## 8. 差集运算例子

计算：

```text
A - B
```

仍然按桶处理：

```text
high = 0:
{1, 2, 3} - {2, 3, 4} = {1}

high = 1:
{0, 1} - {1, 2} = {0}

high = 3:
只有 A 有 -> 保留 {3392}

high = 4:
只有 B 有 -> 和 A - B 无关
```

还原：

```text
A - B = {1, 65_536, 200_000}
```

## 9. 不同 container 之间如何运算

Roaring bitmap 的运算本质是“同 high 桶的 container 两两运算”。container 类型可能不同：

```text
ArrayContainer  ∩ ArrayContainer
ArrayContainer  ∩ BitmapContainer
BitmapContainer ∩ BitmapContainer
RunContainer    ∩ ArrayContainer
RunContainer    ∩ BitmapContainer
...
```

不同组合使用不同算法。

### 9.1 `ArrayContainer ∩ ArrayContainer`

两个有序数组求交集，类似 merge join：

```text
A = [1, 3, 5, 7]
B = [3, 4, 5, 8]

i = 0, j = 0
A[i] = 1, B[j] = 3 -> 1 < 3, i++
A[i] = 3, B[j] = 3 -> 命中 3, i++, j++
A[i] = 5, B[j] = 4 -> 5 > 4, j++
A[i] = 5, B[j] = 5 -> 命中 5, i++, j++
...

result = [3, 5]
```

复杂度大约是：

```text
O(len(A) + len(B))
```

### 9.2 `ArrayContainer ∩ BitmapContainer`

遍历数组，对每个 low 去 bitmap 里查 bit：

```text
array = [1, 3, 5, 7]
bitmap has bit 3 and bit 7

check 1 -> 0
check 3 -> 1
check 5 -> 0
check 7 -> 1

result = [3, 7]
```

复杂度大约是：

```text
O(len(array))
```

这比扫描整个 `8192 bytes` bitmap 更适合稀疏数组。

### 9.3 `BitmapContainer ∩ BitmapContainer`

两个 bitmap container 逐 word 做 `AND`：

```text
for i in 0..1023:
    result.words[i] = a.words[i] & b.words[i]
```

同时可以用 CPU 指令快速统计 cardinality：

```text
cardinality += popcount(result.words[i])
```

这个路径对稠密集合非常快。

## 10. Cardinality 为什么便宜

Roaring bitmap 通常会维护每个 container 的 cardinality，也就是元素个数。

例如：

```text
high = 0 -> cardinality 3
high = 1 -> cardinality 5000
high = 2 -> cardinality 1
```

整个 bitmap 的 cardinality 是：

```text
3 + 5000 + 1 = 5004
```

因此 `count` 不需要遍历所有元素，只需要累加 container 元数据。集合运算后也会更新结果 container 的 cardinality。

在查询系统里这很重要，因为优化器或执行器经常需要知道一个 posting list 大概有多大，来决定是否继续读取、是否 bypass、是否选择 lazy 计算。

## 11. `contains` 查询例子

判断 `x` 是否在 Roaring bitmap 中：

```text
x = 65_537
high = 1
low  = 1
```

步骤：

1. 在顶层有序 key 中找 `high = 1`。
2. 如果没有这个 container，返回 false。
3. 如果有，根据 container 类型判断 low：
   - `ArrayContainer`：二分查找 `1`。
   - `BitmapContainer`：检查 bit `1`。
   - `RunContainer`：检查 `1` 是否落在某个区间。

所以查询不是从 `0` 扫到 `65_537`，而是直接定位桶，再在桶内判断。

## 12. 遍历例子

Roaring bitmap 遍历时按 `high` 从小到大，再遍历 container 里的 `low`。

```text
high = 0 -> lows [1, 2, 3]
high = 2 -> lows [5]
```

输出：

```text
(0 << 16) + 1 = 1
(0 << 16) + 2 = 2
(0 << 16) + 3 = 3
(2 << 16) + 5 = 131_077
```

由于 container 内部也是有序或可按序扫描，所以整体遍历天然有序。

## 13. Roaring bitmap 和普通压缩 bitmap 的区别

传统压缩 bitmap 经常按连续 `0` 或连续 `1` 做 run-length encoding。例如：

```text
000000000000111111111111000000000000
```

可以压缩成：

```text
12 zeros, 12 ones, 12 zeros
```

这种方式对长连续段很好，但随机稀疏数据或集合运算可能不够理想。

Roaring bitmap 的不同点是：

1. 先按高 16 位分桶。
2. 每个桶独立选择数组、bitmap 或 run。
3. 运算时按 container 类型走专门算法。

所以它不是单一压缩格式，而是一个混合数据结构。

## 14. 为什么 Roaring bitmap 适合倒排索引 posting list

倒排索引的 posting list 通常是：

```text
token -> row ids
```

例如：

```text
clickhouse -> [0, 2, 10, 10000, 10001, 10002]
storage    -> [0, 10000, 10003]
```

查询：

```text
hasAllTokens(body, ['clickhouse', 'storage'])
```

需要计算：

```text
postings('clickhouse') ∩ postings('storage')
```

Roaring bitmap 适合这个场景：

1. row id 是递增整数，天然适合 bitmap 表示。
2. 低频 token 的 posting list 很短，`ArrayContainer` 省空间。
3. 高频 token 的 posting list 很长，`BitmapContainer` 运算快。
4. 连续 row id 常见时，`RunContainer` 能压缩连续区间。
5. `AND`、`OR`、`AND NOT` 是倒排查询的核心操作，Roaring 对这些操作有专门优化。

## 15. 结合 text index 的例子

假设一个 `Part` 内有 10 行：

```text
row 0: clickhouse storage engine
row 1: merge tree index
row 2: clickhouse text index
row 3: storage index
row 4: clickhouse storage
row 5: query engine
row 6: text search
row 7: clickhouse query
row 8: storage search
row 9: clickhouse engine
```

token 对应的 posting list：

```text
clickhouse -> {0, 2, 4, 7, 9}
storage    -> {0, 3, 4, 8}
engine     -> {0, 5, 9}
text       -> {2, 6}
```

查询：

```text
clickhouse AND storage
```

计算：

```text
{0, 2, 4, 7, 9} ∩ {0, 3, 4, 8}
= {0, 4}
```

表示只有 row `0` 和 row `4` 同时包含两个 token。

查询：

```text
clickhouse OR text
```

计算：

```text
{0, 2, 4, 7, 9} ∪ {2, 6}
= {0, 2, 4, 6, 7, 9}
```

这些集合如果用 Roaring bitmap 保存，低频 token 会非常紧凑；当 `Part` 很大、高频 token 覆盖大量 row 时，container 会自动选择更适合的 bitmap 表示。

## 16. 和 phrase search 的关系

Roaring bitmap 只表达“某个 token 出现在哪些 row id”。它不表达 token 在文本里的位置。

例如：

```text
row 0: quick brown fox
row 1: quick fox brown
```

posting list：

```text
quick -> {0, 1}
brown -> {0, 1}
fox   -> {0, 1}
```

如果查询短语：

```text
hasPhrase(body, 'quick brown fox')
```

只用 Roaring bitmap 做交集：

```text
quick ∩ brown ∩ fox = {0, 1}
```

它无法区分 row `0` 是真正短语命中，而 row `1` 只是包含同样 token 但顺序不同。

要让倒排索引精确支持 phrase search，需要 positional posting，例如：

```text
quick -> row 0 positions [0], row 1 positions [0]
brown -> row 0 positions [1], row 1 positions [2]
fox   -> row 0 positions [2], row 1 positions [1]
```

然后判断是否存在：

```text
quick position = p
brown position = p + 1
fox position = p + 2
```

当前这种 row-id Roaring bitmap posting list 不保存这类 position 信息，所以只能做候选过滤，不能单独证明 phrase 精确命中。

## 17. 优点和代价

优点：

1. 稀疏和稠密场景都比较高效。
2. 集合运算快，尤其是 bitmap container 之间的按位运算。
3. cardinality 统计便宜。
4. 序列化后适合作为 posting list 存储。
5. 按桶组织，局部更新和局部运算都比较自然。

代价：

1. 结构比普通数组或普通 bitmap 复杂。
2. 小集合上有一定元数据开销。
3. 不保存 position，只能表达整数集合。
4. 对极端随机、高基数、低重复数据，仍可能占用不少内存。
5. 不同 container 类型之间的转换和运算需要额外实现复杂度。

## 18. 总结

Roaring bitmap 可以理解为：

```text
UInt32 set
  -> 按 high 16 bits 分桶
  -> 每个桶保存 low 16 bits
  -> 桶内根据数据形态选择 Array / Bitmap / Run container
```

它的关键价值不是单纯“压缩 bitmap”，而是“分桶 + 自适应 container + 高效集合运算”。这让它非常适合倒排索引里的 posting list：低频 token 节省空间，高频 token 运算快，多个 token 的 `AND` / `OR` 可以高效完成。
