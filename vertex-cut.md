# 点分割存储

&emsp;&emsp;在第一章分布式图系统中，我们介绍了图存储的两种方式：点分割存储和边分割存储。`GraphX`借鉴`powerGraph`，使用的是点分割方式存储图。这种存储方式特点是任何一条边只会出现在一台机器上，每个点有可能分布到不同的机器上。
当点被分割到不同机器上时，是相同的镜像，但是有一个点作为主点,其他的点作为虚点，当点的数据发生变化时,先更新主点的数据，然后将所有更新好的数据发送到虚点所在的所有机器，更新虚点。
这样做的好处是在边的存储上是没有冗余的，而且对于某个点与它的邻居的交互操作，只要满足交换律和结合律，就可以在不同的机器上面执行，网络开销较小。但是这种分割方式会存储多份点数据，更新点时，
会发生网络传输，并且有可能出现同步问题。

&emsp;&emsp;`GraphX`在进行图分割时，有几种不同的分区(`partition`)策略，它通过`PartitionStrategy`专门定义这些策略。在`PartitionStrategy`中，总共定义了`EdgePartition2D`、`EdgePartition1D`、`RandomVertexCut`以及
`CanonicalRandomVertexCut`这四种不同的分区策略。下面分别介绍这几种策略。

## 1 RandomVertexCut

```scala
case object RandomVertexCut extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      math.abs((src, dst).hashCode()) % numParts
    }
  }
```
&emsp;&emsp;这个方法比较简单，通过取源顶点和目标顶点`id`的哈希值来将边分配到不同的分区。这个方法会产生一个随机的边分割，两个顶点之间相同方向的边会分配到同一个分区。

## 2 CanonicalRandomVertexCut

```scala
case object CanonicalRandomVertexCut extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      if (src < dst) {
        math.abs((src, dst).hashCode()) % numParts
      } else {
        math.abs((dst, src).hashCode()) % numParts
      }
    }
  }
```
&emsp;&emsp;这种分割方法和前一种方法没有本质的不同。不同的是，哈希值的产生带有确定的方向（即两个顶点中较小`id`的顶点在前）。两个顶点之间所有的边都会分配到同一个分区，而不管方向如何。

## 3 EdgePartition1D

```scala
case object EdgePartition1D extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val mixingPrime: VertexId = 1125899906842597L
      (math.abs(src * mixingPrime) % numParts).toInt
    }
  }
```
&emsp;&emsp;这种方法仅仅根据源顶点`id`来将边分配到不同的分区。有相同源顶点的边会分配到同一分区。

## 4 EdgePartition2D

```scala
case object EdgePartition2D extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt
      val mixingPrime: VertexId = 1125899906842597L
      if (numParts == ceilSqrtNumParts * ceilSqrtNumParts) {
        // Use old method for perfect squared to ensure we get same results
        val col: PartitionID = (math.abs(src * mixingPrime) % ceilSqrtNumParts).toInt
        val row: PartitionID = (math.abs(dst * mixingPrime) % ceilSqrtNumParts).toInt
        (col * ceilSqrtNumParts + row) % numParts
      } else {
        // Otherwise use new method
        val cols = ceilSqrtNumParts
        val rows = (numParts + cols - 1) / cols
        val lastColRows = numParts - rows * (cols - 1)
        val col = (math.abs(src * mixingPrime) % numParts / rows).toInt
        val row = (math.abs(dst * mixingPrime) % (if (col < cols - 1) rows else lastColRows)).toInt
        col * rows + row
      }
    }
  }
```
&emsp;&emsp;这种分割方法同时使用到了源顶点`id`和目的顶点`id`。它使用稀疏边连接矩阵的2维区分来将边分配到不同的分区，从而保证顶点的备份数不大于`2 * sqrt(numParts)`的限制。这里`numParts`表示分区数。
这个方法的实现分两种情况，即分区数能完全开方和不能完全开方两种情况。当分区数能完全开方时，采用下面的方法：

```scala
 val col: PartitionID = (math.abs(src * mixingPrime) % ceilSqrtNumParts).toInt
 val row: PartitionID = (math.abs(dst * mixingPrime) % ceilSqrtNumParts).toInt
 (col * ceilSqrtNumParts + row) % numParts
```

&emsp;&emsp;当分区数不能完全开方时，采用下面的方法。这个方法的最后一列允许拥有不同的行数。

```scala
val cols = ceilSqrtNumParts
val rows = (numParts + cols - 1) / cols
//最后一列允许不同的行数
val lastColRows = numParts - rows * (cols - 1)
val col = (math.abs(src * mixingPrime) % numParts / rows).toInt
val row = (math.abs(dst * mixingPrime) % (if (col < cols - 1) rows else lastColRows)).toInt
col * rows + row
```
&emsp;&emsp;下面举个例子来说明该方法。假设我们有一个拥有12个顶点的图，要把它切分到9台机器。我们可以用下面的稀疏矩阵来表示:

```
          __________________________________
     v0   | P0 *     | P1       | P2    *  |
     v1   |  ****    |  *       |          |
     v2   |  ******* |      **  |  ****    |
     v3   |  *****   |  *  *    |       *  |
          ----------------------------------
     v4   | P3 *     | P4 ***   | P5 **  * |
     v5   |  *  *    |  *       |          |
     v6   |       *  |      **  |  ****    |
     v7   |  * * *   |  *  *    |       *  |
          ----------------------------------
     v8   | P6   *   | P7    *  | P8  *   *|
     v9   |     *    |  *    *  |          |
     v10  |       *  |      **  |  *  *    |
     v11  | * <-E    |  ***     |       ** |
          ----------------------------------
```

&emsp;&emsp;上面的例子中`*`表示分配到处理器上的边。`E`表示连接顶点`v11`和`v1`的边，它被分配到了处理器`P6`上。为了获得边所在的处理器，我们将矩阵切分为`sqrt(numParts) * sqrt(numParts)`块。
注意，上图中与顶点`v11`相连接的边只出现在第一列的块`(P0,P3,P6)`或者最后一行的块`(P6,P7,P8)`中，这保证了`V11`的副本数不会超过`2 * sqrt(numParts)`份，在上例中即副本不能超过6份。

&emsp;&emsp;在上面的例子中，`P0`里面存在很多边，这会造成工作的不均衡。为了提高均衡，我们首先用顶点`id`乘以一个大的素数，然后再`shuffle`顶点的位置。乘以一个大的素数本质上不能解决不平衡的问题，只是减少了不平衡的情况发生。

# 5 参考文献

【1】[spark源码](https://github.com/apache/spark)