# Pregel API

&emsp;&emsp;图本身是递归数据结构，顶点的属性依赖于它们邻居的属性，这些邻居的属性又依赖于自己邻居的属性。所以许多重要的图算法都是迭代的重新计算每个顶点的属性，直到满足某个确定的条件。
一系列的图并发(`graph-parallel`)抽象已经被提出来用来表达这些迭代算法。`GraphX`公开了一个类似`Pregel`的操作，它是广泛使用的`Pregel`和`GraphLab`抽象的一个融合。

&emsp;&emsp;`GraphX`中实现的这个更高级的`Pregel`操作是一个约束到图拓扑的批量同步（`bulk-synchronous`）并行消息抽象。`Pregel`操作者执行一系列的超步（`super steps`），在这些步骤中，顶点从
之前的超步中接收进入(`inbound`)消息的总和，为顶点属性计算一个新的值，然后在以后的超步中发送消息到邻居顶点。不像`Pregel`而更像`GraphLab`，消息通过边`triplet`的一个函数被并行计算，
消息的计算既会访问源顶点特征也会访问目的顶点特征。在超步中，没有收到消息的顶点会被跳过。当没有消息遗留时，`Pregel`操作停止迭代并返回最终的图。

&emsp;&emsp;注意，与标准的`Pregel`实现不同的是，`GraphX`中的顶点仅仅能发送信息给邻居顶点，并且可以利用用户自定义的消息函数并行地构造消息。这些限制允许对`GraphX`进行额外的优化。

&emsp;&emsp;下面的代码是`pregel`的具体实现。

```scala
def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] =
  {
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
    // 计算消息
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()
    // 迭代
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // 接收消息并更新顶点
      prevG = g
      g = g.joinVertices(messages)(vprog).cache()
      val oldMessages = messages
      // 发送新消息
      messages = g.mapReduceTriplets(
        sendMsg, mergeMsg, Some((oldMessages, activeDirection))).cache()
      activeMessages = messages.count()
      i += 1
    }
    g
  } 
```
## 1 pregel计算模型

&emsp;&emsp;`Pregel`计算模型中有三个重要的函数，分别是`vertexProgram`、`sendMessage`和`messageCombiner`。

- `vertexProgram`：用户定义的顶点运行程序。它作用于每一个顶点，负责接收进来的信息，并计算新的顶点值。

- `sendMsg`：发送消息

- `mergeMsg`：合并消息

&emsp;&emsp;我们具体分析它的实现。根据代码可以知道，这个实现是一个迭代的过程。在开始迭代之前，先完成一些初始化操作：

```scala
var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
// 计算消息
var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
var activeMessages = messages.count()
```
&emsp;&emsp;程序首先用`vprog`函数处理图中所有的顶点，生成新的图。然后用生成的图调用聚合操作（`mapReduceTriplets`，实际的实现是我们前面章节讲到的`aggregateMessagesWithActiveSet`函数）获取聚合后的消息。
`activeMessages`指`messages`这个`VertexRDD`中的顶点数。

&emsp;&emsp;下面就开始迭代操作了。在迭代内部，分为二步。

- 1 接收消息，并更新顶点

```scala
 g = g.joinVertices(messages)(vprog).cache()
 //joinVertices的定义
 def joinVertices[U: ClassTag](table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD)
     : Graph[VD, ED] = {
     val uf = (id: VertexId, data: VD, o: Option[U]) => {
       o match {
         case Some(u) => mapFunc(id, data, u)
         case None => data
       }
     }
     graph.outerJoinVertices(table)(uf)
   }
```
&emsp;&emsp;这一步实际上是使用`outerJoinVertices`来更新顶点属性。`outerJoinVertices`在[关联操作](operators/join.md)中有详细介绍。

- 2 发送新消息

```scala
 messages = g.mapReduceTriplets(
        sendMsg, mergeMsg, Some((oldMessages, activeDirection))).cache()
```
&emsp;&emsp;注意，在上面的代码中，`mapReduceTriplets`多了一个参数`Some((oldMessages, activeDirection))`。这个参数的作用是：它使我们在发送新的消息时，会忽略掉那些两端都没有接收到消息的边，减少计算量。

## 2 pregel实现最短路径

```scala
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
val graph: Graph[Long, Double] =
  GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)
val sourceId: VertexId = 42 // The ultimate source
// 初始化图
val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
val sssp = initialGraph.pregel(Double.PositiveInfinity)(
  (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
  triplet => {  // Send Message
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (a,b) => math.min(a,b) // Merge Message
  )
println(sssp.vertices.collect.mkString("\n"))
```
&emsp;&emsp;上面的例子中，`Vertex Program`函数定义如下：

```scala
(id, dist, newDist) => math.min(dist, newDist)
```
&emsp;&emsp;这个函数的定义显而易见，当两个消息来的时候，取它们当中路径的最小值。同理`Merge Message`函数也是同样的含义。

&emsp;&emsp;`Send Message`函数中，会首先比较`triplet.srcAttr + triplet.attr`和`triplet.dstAttr`，即比较加上边的属性后，这个值是否小于目的节点的属性，如果小于，则发送消息到目的顶点。

## 3 参考文献

【1】[spark源码](https://github.com/apache/spark)