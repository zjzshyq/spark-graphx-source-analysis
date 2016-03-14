# 转换操作

&emsp;&emsp;`GraphX`中的转换操作主要有`mapVertices`,`mapEdges`和`mapTriplets`三个，它们在`Graph`文件中定义，在`GraphImpl`文件中实现。下面分别介绍这三个方法。

## 1 `mapVertices`

&emsp;&emsp;`mapVertices`用来更新顶点属性。从图的构建那章我们知道，顶点属性保存在边分区中，所以我们需要改变的是边分区中的属性。

```scala
override def mapVertices[VD2: ClassTag]
    (f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    if (eq != null) {
      vertices.cache()
      // 使用方法f处理vertices
      val newVerts = vertices.mapVertexPartitions(_.map(f)).cache()
      //获得两个不同vertexRDD的不同
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      //更新ReplicatedVertexView
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      GraphImpl(vertices.mapVertexPartitions(_.map(f)), replicatedVertexView.edges)
    }
  }
```
&emsp;&emsp;上面的代码中，当`VD`和`VD2`类型相同时，我们可以重用没有发生变化的点，否则需要重新创建所有的点。我们分析`VD`和`VD2`相同的情况，分四步处理。

- 1 使用方法`f`处理`vertices`,获得新的`VertexRDD`

- 2 使用在`VertexRDD`中定义的`diff`方法求出新`VertexRDD`和源`VertexRDD`的不同

```scala
override def diff(other: VertexRDD[VD]): VertexRDD[VD] = {
    val otherPartition = other match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        other.partitionsRDD
      case _ =>
        VertexRDD(other.partitionBy(this.partitioner.get)).partitionsRDD
    }
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      otherPartition, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.diff(otherPart))
    }
    this.withPartitionsRDD(newPartitionsRDD)
  }
```
&emsp;&emsp;这个方法首先处理新生成的`VertexRDD`的分区，如果它的分区和源`VertexRDD`的分区一致，那么直接取出它的`partitionsRDD`,否则重新分区后取出它的`partitionsRDD`。
针对新旧两个`VertexRDD`的所有分区，调用`VertexPartitionBaseOps`中的`diff`方法求得分区的不同。

```scala
def diff(other: Self[VD]): Self[VD] = {
    //首先判断
    if (self.index != other.index) {
      diff(createUsingIndex(other.iterator))
    } else {
      val newMask = self.mask & other.mask
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        if (self.values(i) == other.values(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      this.withValues(other.values).withMask(newMask)
    }
  }
```
&emsp;&emsp;该方法隐藏两个`VertexRDD`中相同的顶点信息，得到一个新的`VertexRDD`。

- 3 更新`ReplicatedVertexView`

```scala
def updateVertices(updates: VertexRDD[VD]): ReplicatedVertexView[VD, ED] = {
    //生成一个VertexAttributeBlock
    val shippedVerts = updates.shipVertexAttributes(hasSrcId, hasDstId)
      .setName("ReplicatedVertexView.updateVertices - shippedVerts %s %s (broadcast)".format(
        hasSrcId, hasDstId))
      .partitionBy(edges.partitioner.get)
    //生成新的边RDD
    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
      (ePartIter, shippedVertsIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
      }
    })
    new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }
```
&emsp;&emsp;`updateVertices`方法返回一个新的`ReplicatedVertexView`,它更新了边分区中包含的顶点属性。我们看看它的实现过程。首先看`shipVertexAttributes`方法的调用。
调用`shipVertexAttributes`方法会生成一个`VertexAttributeBlock`，`VertexAttributeBlock`包含当前分区的顶点属性，这些属性可以在特定的边分区使用。

```scala
def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): Iterator[(PartitionID, VertexAttributeBlock[VD])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val initialSize = if (shipSrc && shipDst) routingTable.partitionSize(pid) else 64
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, shipSrc, shipDst) { vid =>
        if (isDefined(vid)) {
          vids += vid
          attrs += this(vid)
        }
        i += 1
      }
      //（边分区id，VertexAttributeBlock（顶点id，属性））
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }
```
&emsp;&emsp;获得新的顶点属性之后，我们就可以调用`updateVertices`更新边中顶点的属性了，如下面代码所示：

```scala
edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator))
//更新EdgePartition的属性
def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[ED, VD] = {
    val newVertexAttrs = new Array[VD](vertexAttrs.length)
    System.arraycopy(vertexAttrs, 0, newVertexAttrs, 0, vertexAttrs.length)
    while (iter.hasNext) {
      val kv = iter.next()
      //global2local获得顶点的本地index
      newVertexAttrs(global2local(kv._1)) = kv._2
    }
    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, newVertexAttrs,
      activeSet)
  }
```

## 2 `mapEdges`

&emsp;&emsp;`mapEdges`用来更新边属性。

```scala
 override def mapEdges[ED2: ClassTag](
      f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val newEdges = replicatedVertexView.edges
      .mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }
```
&emsp;&emsp;相比于`mapVertices`，`mapEdges`显然要简单得多，它只需要根据方法`f`生成新的`EdgeRDD`,然后再初始化即可。

## 3 `mapTriplets`：用来更新边属性

&emsp;&emsp;`mapTriplets`用来更新边属性。

```scala
override def mapTriplets[ED2: ClassTag](
      f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2],
      tripletFields: TripletFields): Graph[VD, ED2] = {
    vertices.cache()
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val newEdges = replicatedVertexView.edges.mapEdgePartitions { (pid, part) =>
      part.map(f(pid, part.tripletIterator(tripletFields.useSrc, tripletFields.useDst)))
    }
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }
```
&emsp;&emsp;这段代码中，`replicatedVertexView`调用`upgrade`方法修改当前的`ReplicatedVertexView`，使调用者可以访问到指定级别的边信息（如仅仅可以读源顶点的属性）。

```scala
def upgrade(vertices: VertexRDD[VD], includeSrc: Boolean, includeDst: Boolean) {
    //判断传递级别
    val shipSrc = includeSrc && !hasSrcId
    val shipDst = includeDst && !hasDstId
    if (shipSrc || shipDst) {
      val shippedVerts: RDD[(Int, VertexAttributeBlock[VD])] =
        vertices.shipVertexAttributes(shipSrc, shipDst)
          .setName("ReplicatedVertexView.upgrade(%s, %s) - shippedVerts %s %s (broadcast)".format(
            includeSrc, includeDst, shipSrc, shipDst))
          .partitionBy(edges.partitioner.get)
      val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
        (ePartIter, shippedVertsIter) => ePartIter.map {
          case (pid, edgePartition) =>
            (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
        }
      })
      edges = newEdges
      hasSrcId = includeSrc
      hasDstId = includeDst
    }
  }
```
&emsp;&emsp;最后，用`f`处理边，生成新的`RDD`，最后用新的数据初始化图。

## 4 总结

&emsp;&emsp;调用`mapVertices`,`mapEdges`和`mapTriplets`时，其内部的结构化索引（`Structural indices`）并不会发生变化，它们都重用路由表中的数据。
