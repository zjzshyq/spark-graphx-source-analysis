# 聚合操作

&emsp;&emsp;`GraphX`中提供的聚合操作有`aggregateMessages`、`collectNeighborIds`和`collectNeighbors`三个，其中`aggregateMessages`在`GraphImpl`中实现，`collectNeighborIds`和`collectNeighbors`在
`GraphOps`中实现。下面分别介绍这几个方法。

# 1 `aggregateMessages`

## 1.1 `aggregateMessages`接口
&emsp;&emsp;`aggregateMessages`是`GraphX`最重要的`API`，用于替换`mapReduceTriplets`。目前`mapReduceTriplets`最终也是通过`aggregateMessages`来实现的。它主要功能是向邻边发消息，合并邻边收到的消息，返回`messageRDD`。
`aggregateMessages`的接口如下：

```scala
def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.All)
    : VertexRDD[A] = {
    aggregateMessagesWithActiveSet(sendMsg, mergeMsg, tripletFields, None)
  }
```
&emsp;&emsp;该接口有三个参数，分别为发消息函数，合并消息函数以及发消息的方向。

- `sendMsg`： 发消息函数

```scala
private def sendMsg(ctx: EdgeContext[KCoreVertex, Int, Map[Int, Int]]): Unit = {
    ctx.sendToDst(Map(ctx.srcAttr.preKCore -> -1, ctx.srcAttr.curKCore -> 1))
    ctx.sendToSrc(Map(ctx.dstAttr.preKCore -> -1, ctx.dstAttr.curKCore -> 1))
}
```

- `mergeMsg`：合并消息函数

&emsp;&emsp;该函数用于在`Map`阶段每个`edge`分区中每个点收到的消息合并，并且它还用于`reduce`阶段，合并不同分区的消息。合并`vertexId`相同的消息。

- `tripletFields`：定义发消息的方向

## 1.2 `aggregateMessages`处理流程

&emsp;&emsp;`aggregateMessages`方法分为`Map`和`Reduce`两个阶段，下面我们分别就这两个阶段说明。

### 1.2.1 Map阶段

&emsp;&emsp;从入口函数进入`aggregateMessagesWithActiveSet`函数，该函数首先使用`VertexRDD[VD]`更新`replicatedVertexView`, 只更新其中`vertexRDD`中`attr`对象。如[构建图](../build-graph.md)中介绍的，
`replicatedVertexView`是点和边的视图，点的属性有变化，要更新边中包含的点的`attr`。

```scala
replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
val view = activeSetOpt match {
    case Some((activeSet, _)) =>
      //返回只包含活跃顶点的replicatedVertexView
      replicatedVertexView.withActiveSet(activeSet)
    case None =>
      replicatedVertexView
}
```
&emsp;&emsp;程序然后会对`replicatedVertexView`的`edgeRDD`做`mapPartitions`操作，所有的操作都在每个边分区的迭代中完成，如下面的代码：

```scala
 val preAgg = view.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, edgePartition) =>
        // 选择 scan 方法
        val activeFraction = edgePartition.numActives.getOrElse(0) / edgePartition.indexSize.toFloat
        activeDirectionOpt match {
          case Some(EdgeDirection.Both) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            }
          case Some(EdgeDirection.Either) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Either)
          case Some(EdgeDirection.Out) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            }
          case Some(EdgeDirection.In) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.DstOnly)
          case _ => // None
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Neither)
        }
    })
```
&emsp;&emsp;在分区内，根据`activeFraction`的大小选择是进入`aggregateMessagesEdgeScan`还是`aggregateMessagesIndexScan`处理。`aggregateMessagesEdgeScan`会顺序地扫描所有的边，
而`aggregateMessagesIndexScan`会先过滤源顶点索引，然后在扫描。我们重点去分析`aggregateMessagesEdgeScan`。

```scala
def aggregateMessagesEdgeScan[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeness: EdgeActiveness): Iterator[(VertexId, A)] = {
    var ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    var i = 0
    while (i < size) {
      val localSrcId = localSrcIds(i)
      val srcId = local2global(localSrcId)
      val localDstId = localDstIds(i)
      val dstId = local2global(localDstId)
      val srcAttr = if (tripletFields.useSrc) vertexAttrs(localSrcId) else null.asInstanceOf[VD]
      val dstAttr = if (tripletFields.useDst) vertexAttrs(localDstId) else null.asInstanceOf[VD]
      ctx.set(srcId, dstId, localSrcId, localDstId, srcAttr, dstAttr, data(i))
      sendMsg(ctx)
      i += 1
    }
```
&emsp;&emsp;该方法由两步组成，分别是获得顶点相关信息，以及发送消息。

- 获取顶点相关信息

&emsp;&emsp;在前文介绍`edge partition`时，我们知道它包含`localSrcIds,localDstIds, data, index, global2local, local2global, vertexAttrs`这几个重要的数据结构。其中`localSrcIds,localDstIds`分别表示源顶点、目的顶点在当前分区中的索引。
所以我们可以遍历`localSrcIds`,根据其下标去`localSrcIds`中拿到`srcId`在全局`local2global`中的索引，最后拿到`srcId`。通过`vertexAttrs`拿到顶点属性。通过`data`拿到边属性。

- 发送消息

&emsp;&emsp;发消息前会根据接口中定义的`tripletFields`，拿到发消息的方向。发消息的过程就是遍历到一条边，向`localSrcIds/localDstIds`中添加数据，如果`localSrcIds/localDstIds`中已经存在该数据，则执行合并函数`mergeMsg`。

```scala
 override def sendToSrc(msg: A) {
    send(_localSrcId, msg)
  }
  override def sendToDst(msg: A) {
    send(_localDstId, msg)
  }
  @inline private def send(localId: Int, msg: A) {
    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
  }
```
&emsp;&emsp;每个点之间在发消息的时候是独立的，即：点单纯根据方向，向以相邻点的以`localId`为下标的数组中插数据，互相独立，可以并行运行。

&emsp;&emsp;`Map`阶段的执行如下例所示：

<div  align="center"><img src="imgs/graphx_aggmsg_map.png" width = "900" height = "300" alt="graphx_aggmsg_map" align="center" /></div><br />

