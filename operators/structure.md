# 结构操作

&emsp;&emsp;当前的`GraphX`仅仅支持一组简单的常用结构性操作。下面是基本的结构性操作列表。

```scala
class Graph[VD, ED] {
  def reverse: Graph[VD, ED]
  def subgraph(epred: EdgeTriplet[VD,ED] => Boolean,
               vpred: (VertexId, VD) => Boolean): Graph[VD, ED]
  def mask[VD2, ED2](other: Graph[VD2, ED2]): Graph[VD, ED]
  def groupEdges(merge: (ED, ED) => ED): Graph[VD,ED]
}
```
&emsp;&emsp;下面分别介绍这四种函数的原理。

# 1 reverse

&emsp;&emsp;`reverse`操作返回一个新的图，这个图的边的方向都是反转的。例如，这个操作可以用来计算反转的PageRank。因为反转操作没有修改顶点或者边的属性或者改变边的数量，所以我们可以
在不移动或者复制数据的情况下有效地实现它。

```scala
override def reverse: Graph[VD, ED] = {
    new GraphImpl(vertices.reverseRoutingTables(), replicatedVertexView.reverse())
}
def reverse(): ReplicatedVertexView[VD, ED] = {
    val newEdges = edges.mapEdgePartitions((pid, part) => part.reverse)
    new ReplicatedVertexView(newEdges, hasDstId, hasSrcId)
}
//EdgePartition中的reverse
def reverse: EdgePartition[ED, VD] = {
    val builder = new ExistingEdgePartitionBuilder[ED, VD](
      global2local, local2global, vertexAttrs, activeSet, size)
    var i = 0
    while (i < size) {
      val localSrcId = localSrcIds(i)
      val localDstId = localDstIds(i)
      val srcId = local2global(localSrcId)
      val dstId = local2global(localDstId)
      val attr = data(i)
      //将源顶点和目标顶点换位置
      builder.add(dstId, srcId, localDstId, localSrcId, attr)
      i += 1
    }
    builder.toEdgePartition
  }
```

## 2 subgraph

&emsp;&emsp;`subgraph`操作利用顶点和边的判断式（`predicates`），返回的图仅仅包含满足顶点判断式的顶点、满足边判断式的边以及满足顶点判断式的`triple`。`subgraph`操作可以用于很多场景，如获取
感兴趣的顶点和边组成的图或者获取清除断开链接后的图。

## 3 mask

&emsp;&emsp;`mask`操作构造一个子图，这个子图包含输入图中包含的顶点和边。这个操作可以和`subgraph`操作相结合，基于另外一个相关图的特征去约束一个图。

## 4 groupEdges

&emsp;&emsp;`groupEdges`操作合并多重图中的并行边(如顶点对之间重复的边)。在大量的应用程序中，并行的边可以合并（它们的权重合并）为一条边从而降低图的大小。