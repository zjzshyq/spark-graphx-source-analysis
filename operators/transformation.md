# 转换操作

&emsp;&emsp;`GraphX`中的转换操作主要有`mapVertices`,`mapEdges`和`mapTriplets`三个，它们在`Graph`类中定义，在`GraphImpl`中实现。下面分别介绍这三个方法。

## 1 `mapVertices`

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
      // The map does not preserve type, so we must re-replicate all vertices
      GraphImpl(vertices.mapVertexPartitions(_.map(f)), replicatedVertexView.edges)
    }
  }
```
&emsp;&emsp;上面的代码中，隐式变量`eq`表示当`VD`和`VD2`相同时其为`true`，否则为空。如果`VD`和`VD2`不同，分四步处理。

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
&emsp;&emsp;调用`shipVertexAttributes`生成一个`VertexAttributeBlock`，`VertexAttributeBlock`包含当前分区的顶点属性，这些属性可以在特定的边分区使用。

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

## 2 `mapEdges`

```scala
 override def mapEdges[ED2: ClassTag](
      f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val newEdges = replicatedVertexView.edges
      .mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }
```
&emsp;&emsp;相比于`mapVertices`，`mapEdges`显然要简单得多，它只需要根据方法`f`生成新的`EdgeRDD`,然后再初始化即可。

## 3 `mapTriplets`

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
&emsp;&emsp;这段代码中，`replicatedVertexView`调用`upgrade`方法修改当前的`ReplicatedVertexView`，使调用者可以访问到边信息。用`f`处理可以访问到的边，生成新的RDD，最后用新的数据初始化图。

## 4 总结

&emsp;&emsp;调用`mapVertices`,`mapEdges`和`mapTriplets`时，其内部的结构化索引（`Structural indices`）并不会发生变化，它们使用路由表重用数据。
