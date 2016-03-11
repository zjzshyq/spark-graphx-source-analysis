# 图的构建

&emsp;&emsp;`GraphX`的`Graph`对象是用户操作图的入口, 它包含了边(`edges`)、顶点(`vertices`)以及`triplets`三部分，当然这三部分都包含相应的属性，可以携带额外的信息。

# 1 构建图的方法

&emsp;&emsp;构建图的入口方法有两种，分别是根据边构建和根据边的两个顶点构建。

- **根据边构建图(Graph.fromEdges)**

```scala
def fromEdges[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultValue: VD,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] = {
    GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
  }
```
- **根据边的两个顶点数据构建(Graph.fromEdgeTuples)**

```scala
 def fromEdgeTuples[VD: ClassTag](
      rawEdges: RDD[(VertexId, VertexId)],
      defaultValue: VD,
      uniqueEdges: Option[PartitionStrategy] = None,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, Int] =
  {
    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    val graph = GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
    uniqueEdges match {
      case Some(p) => graph.partitionBy(p).groupEdges((a, b) => a + b)
      case None => graph
    }
  }
```
&emsp;&emsp;从上面的代码我们知道，不管是根据边构建图还是根据边的两个顶点数据构建，最终都是使用`GraphImpl`来构建的，即调用了`GraphImpl`的`apply`方法。

# 2 构建图的过程

&emsp;&emsp;构建图的过程很简单，分为三步，它们分别是构建边`EdgeRDD`、构建顶点`VertexRDD`、生成`Graph`对象。下面分别介绍这三个步骤。

## 2.1 构建边`EdgeRDD`

&emsp;&emsp;从源代码看构建边`EdgeRDD`也分为三步，下图的例子详细说明了这些步骤。

<div  align="center"><img src="imgs/4.1.png" width = "900" height = "450" alt="4.1" align="center" /></div><br />

- **1** 从文件中加载信息，转换成`tuple`的形式,即`(srcId, dstId)`

```scala
val rawEdgesRdd: RDD[(Long, Long)] = 
    sc.textFile(input).filter(s => s != "0,0").repartition(partitionNum).map {
      case line =>
        val ss = line.split(",")
        val src = ss(0).toLong
        val dst = ss(1).toLong
        if (src < dst)
          (src, dst)
        else
          (dst, src)
    }.distinct()
```

- **2** 入口，调用`Graph.fromEdgeTuples(rawEdgesRdd)`

&emsp;&emsp;源数据为分割的两个点`ID`，把源数据映射成`Edge(srcId, dstId, attr)`对象, attr默认为1。这样元数据就构建成了`RDD[Edge[ED]]`,如下面的代码

```scala
val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
```

- **3** 将`RDD[Edge[ED]]`进一步转化成`EdgeRDDImpl[ED, VD]`
 
&emsp;&emsp;第二步构建`RDD[Edge[ED]]`之后，`GraphX`做了如下操作用来构建`Graph`。

```scala
val graph = GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
```
&emsp;&emsp;该代码实际上是调用了`GraphImpl`对象的`apply`方法

```scala
def apply[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    fromEdgeRDD(EdgeRDD.fromEdges(edges), defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }
```
&emsp;&emsp;在`apply`调用`fromEdgeRDD`之前，代码调用`EdgeRDD.fromEdges(edges)`将`RDD[Edge[ED]]`进一步转化成`EdgeRDDImpl[ED, VD]`。

```scala
def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): EdgeRDDImpl[ED, VD] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED, VD]
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.toEdgePartition))
    }
    EdgeRDD.fromEdgePartitions(edgePartitions)
  }
```
&emsp;&emsp;程序遍历`RDD[Edge[ED]]`的每个分区，并调用`builder.toEdgePartition`对分区内的边作相应的处理。

```scala
def toEdgePartition: EdgePartition[ED, VD] = {
    val edgeArray = edges.trim().array
    new Sorter(Edge.edgeArraySortDataFormat[ED])
      .sort(edgeArray, 0, edgeArray.length, Edge.lexicographicOrdering)
    val localSrcIds = new Array[Int](edgeArray.size)
    val localDstIds = new Array[Int](edgeArray.size)
    val data = new Array[ED](edgeArray.size)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val local2global = new PrimitiveVector[VertexId]
    var vertexAttrs = Array.empty[VD]
    if (edgeArray.length > 0) {
      index.update(edgeArray(0).srcId, 0)
      var currSrcId: VertexId = edgeArray(0).srcId
      var currLocalId = -1
      var i = 0
      while (i < edgeArray.size) {
        val srcId = edgeArray(i).srcId
        val dstId = edgeArray(i).dstId
        localSrcIds(i) = global2local.changeValue(srcId,
          { currLocalId += 1; local2global += srcId; currLocalId }, identity)
        localDstIds(i) = global2local.changeValue(dstId,
          { currLocalId += 1; local2global += dstId; currLocalId }, identity)
        data(i) = edgeArray(i).attr
        if (srcId != currSrcId) {
          currSrcId = srcId
          index.update(currSrcId, i)
        }
        i += 1
      }
      vertexAttrs = new Array[VD](currLocalId + 1)
    }
    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global.trim().array, vertexAttrs,
      None)
  }
```
&emsp;&emsp;`toEdgePartition`的第一步就是对边进行排序，即按照`srcId`从小到大排序。排序是为了遍历时顺序访问，加快访问速度。采用数组而不是`Map`，是因为数组是连续的内存单元，具有原子性，避免了`Map`的`hash`问题，访问速度快

&emsp;&emsp;`toEdgePartition`的第二步就是填充`localSrcIds,localDstIds, data, index, global2local, local2global, vertexAttrs`。

&emsp;&emsp;数组`localSrcIds,localDstIds`中保存的是通过`global2local.changeValue(srcId/dstId)`转换而成的本地索引。`localSrcIds、localDstIds`数组中保存的索引位可以通过`local2global`查到具体的`VertexId`。

&emsp;&emsp;`global2local`是一个简单的，`key`值非负的快速`hash map`：`GraphXPrimitiveKeyOpenHashMap`, 保存`vertextId`和本地索引的映射关系。`global2local`中包含当前`partition`所有`srcId`、`dstId`与本地索引的映射关系。

&emsp;&emsp;`data`就是当前分区的`attr`属性数组。

&emsp;&emsp;我们知道相同的`srcId`可能对应不同的`dstId`。按照`srcId`排序之后，相同的`srcId`会出现多行，如上图中的`index desc`部分。`index`中记录的是相同`srcId`中第一个出现的`srcId`与其下标。

&emsp;&emsp;`local2global`记录的是所有的`VertexId`信息的数组。形如：`srcId,dstId,srcId,dstId,srcId,dstId,srcId,dstId`。其中会包含相同的`srcId`。即：当前分区所有`vertextId`的顺序实际值。

&emsp;&emsp;我们可以通过根据本地下标取`VertexId`，也可以根据`VertexId`取本地下标，取相应的属性。

```scala
＃ 根据本地下标取VertexId
localSrcIds/localDstIds -> index -> local2global -> VertexId
＃ 根据VertexId取本地下标，取属性
VertexId -> global2local -> index -> data -> attr object
```
