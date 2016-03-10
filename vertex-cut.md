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
&emsp;&emsp;这个方法比较简单，通过取源顶点和目标顶点`id`的哈希值来将边分配到不同的分区。