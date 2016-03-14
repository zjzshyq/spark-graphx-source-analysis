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

```scala
override def subgraph(
      epred: EdgeTriplet[VD, ED] => Boolean = x => true,
      vpred: (VertexId, VD) => Boolean = (a, b) => true): Graph[VD, ED] = {
    vertices.cache()
    // 过滤vertices, 重用partitioner和索引
    val newVerts = vertices.mapVertexPartitions(_.filter(vpred))
    // 过滤 triplets
    replicatedVertexView.upgrade(vertices, true, true)
    val newEdges = replicatedVertexView.edges.filter(epred, vpred)
    new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }
```
&emsp;&emsp;该代码显示，`subgraph`方法的实现分两步：先过滤`VertexRDD`，然后再过滤`EdgeRDD`。如上，过滤`VertexRDD`比较简单，我们重点看过滤`EdgeRDD`的过程。

```scala
def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgeRDDImpl[ED, VD] = {
    mapEdgePartitions((pid, part) => part.filter(epred, vpred))
  }
//EdgePartition中的filter方法
def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgePartition[ED, VD] = {
    val builder = new ExistingEdgePartitionBuilder[ED, VD](
      global2local, local2global, vertexAttrs, activeSet)
    var i = 0
    while (i < size) {
      // The user sees the EdgeTriplet, so we can't reuse it and must create one per edge.
      val localSrcId = localSrcIds(i)
      val localDstId = localDstIds(i)
      val et = new EdgeTriplet[VD, ED]
      et.srcId = local2global(localSrcId)
      et.dstId = local2global(localDstId)
      et.srcAttr = vertexAttrs(localSrcId)
      et.dstAttr = vertexAttrs(localDstId)
      et.attr = data(i)
      if (vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et)) {
        builder.add(et.srcId, et.dstId, localSrcId, localDstId, et.attr)
      }
      i += 1
    }
    builder.toEdgePartition
  }  
```
&emsp;&emsp;因为用户可以看到`EdgeTriplet`的信息，所以我们不能重用`EdgeTriplet`，需要重新创建一个，然后在用`epred`函数处理。这里`localSrcIds,localDstIds,local2global`等前文均有介绍，在此不再赘述。

## 3 mask

&emsp;&emsp;`mask`操作构造一个子图，这个子图包含输入图中包含的顶点和边。它的实现很简单，顶点和边均做`inner join`操作即可。这个操作可以和`subgraph`操作相结合，基于另外一个相关图的特征去约束一个图。

```scala
override def mask[VD2: ClassTag, ED2: ClassTag] (
      other: Graph[VD2, ED2]): Graph[VD, ED] = {
    val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v }
    val newEdges = replicatedVertexView.edges.innerJoin(other.edges) { (src, dst, v, w) => v }
    new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }
```

## 4 groupEdges

&emsp;&emsp;`groupEdges`操作合并多重图中的并行边(如顶点对之间重复的边)。在大量的应用程序中，并行的边可以合并（它们的权重合并）为一条边从而降低图的大小。

```scala
 override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    val newEdges = replicatedVertexView.edges.mapEdgePartitions(
      (pid, part) => part.groupEdges(merge))
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }
 def groupEdges(merge: (ED, ED) => ED): EdgePartition[ED, VD] = {
     val builder = new ExistingEdgePartitionBuilder[ED, VD](
       global2local, local2global, vertexAttrs, activeSet)
     var currSrcId: VertexId = null.asInstanceOf[VertexId]
     var currDstId: VertexId = null.asInstanceOf[VertexId]
     var currLocalSrcId = -1
     var currLocalDstId = -1
     var currAttr: ED = null.asInstanceOf[ED]
     // 迭代处理所有的边
     var i = 0
     while (i < size) {
       //如果源顶点和目的顶点都相同
       if (i > 0 && currSrcId == srcIds(i) && currDstId == dstIds(i)) {
         // 合并属性
         currAttr = merge(currAttr, data(i))
       } else {
         // This edge starts a new run of edges
         if (i > 0) {
           // 添加到builder中
           builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
         }
         // Then start accumulating for a new run
         currSrcId = srcIds(i)
         currDstId = dstIds(i)
         currLocalSrcId = localSrcIds(i)
         currLocalDstId = localDstIds(i)
         currAttr = data(i)
       }
       i += 1
     }
     if (size > 0) {
       builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
     }
     builder.toEdgePartition
   }
```
&emsp;&emsp;在[图构建](build-graph.md)那章我们说明过，存储的边按照源顶点`id`排过序，所以上面的代码可以通过一次迭代完成对所有相同边的处理。

## 5 参考文献

【1】[spark源码](https://github.com/apache/spark)