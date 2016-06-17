# GraphX介绍

## 1 GraphX的优势

&emsp;&emsp;`GraphX`是一个新的`Spark API`，它用于图和分布式图(`graph-parallel`)的计算。`GraphX`通过引入弹性分布式属性图（[Resilient Distributed Property Graph](property-graph.md)）：
顶点和边均有属性的有向多重图，来扩展`Spark RDD`。为了支持图计算，`GraphX`开发了一组基本的功能操作以及一个优化过的`Pregel API`。另外，`GraphX`包含了一个快速增长的图算法和图`builders`的
集合，用以简化图分析任务。

&emsp;&emsp;从社交网络到语言建模，不断增长的规模以及图形数据的重要性已经推动了许多新的分布式图系统（如[Giraph](http://giraph.apache.org/)和[GraphLab](http://graphlab.org/)）的发展。
通过限制计算类型以及引入新的技术来切分和分配图，这些系统可以高效地执行复杂的图形算法，比一般的分布式数据计算（`data-parallel`，如`spark`、`MapReduce`）快很多。

<div  align="center"><img src="imgs/2.1.png" width = "600" height = "370" alt="2.1" align="center" /></div><br />

&emsp;&emsp;分布式图（`graph-parallel`）计算和分布式数据（`data-parallel`）计算类似，分布式数据计算采用了一种`record-centric`的集合视图，而分布式图计算采用了一种`vertex-centric`的图视图。
分布式数据计算通过同时处理独立的数据来获得并发的目的，分布式图计算则是通过对图数据进行分区（即切分）来获得并发的目的。更准确的说，分布式图计算递归地定义特征的转换函数（这种转换函数作用于邻居特征），通过并发地执行这些转换函数来获得并发的目的。

&emsp;&emsp;分布式图计算比分布式数据计算更适合图的处理，但是在典型的图处理流水线中，它并不能很好地处理所有操作。例如，虽然分布式图系统可以很好的计算`PageRank`以及`label diffusion`，但是它们不适合从不同的数据源构建图或者跨过多个图计算特征。
更准确的说，分布式图系统提供的更窄的计算视图无法处理那些构建和转换图结构以及跨越多个图的需求。分布式图系统中无法提供的这些操作需要数据在图本体之上移动并且需要一个图层面而不是单独的顶点或边层面的计算视图。例如，我们可能想限制我们的分析到几个子图上，然后比较结果。
这不仅需要改变图结构，还需要跨多个图计算。

<div  align="center"><img src="imgs/2.2.png" width = "600" height = "370" alt="2.2" align="center" /></div><br />

&emsp;&emsp;我们如何处理数据取决于我们的目标，有时同一原始数据可能会处理成许多不同表和图的视图，并且图和表之间经常需要能够相互移动。如下图所示：

<div  align="center"><img src="imgs/2.3.png" width = "600" height = "370" alt="2.3" align="center" /></div><br />

&emsp;&emsp;所以我们的图流水线必须通过组合`graph-parallel`和`data- parallel`来实现。但是这种组合必然会导致大量的数据移动以及数据复制，同时这样的系统也非常复杂。
例如，在传统的图计算流水线中，在`Table View`视图下，可能需要`Spark`或者`Hadoop`的支持，在`Graph View`这种视图下，可能需要`Prege`或者`GraphLab`的支持。也就是把图和表分在不同的系统中分别处理。
不同系统之间数据的移动和通信会成为很大的负担。

&emsp;&emsp;`GraphX`项目将`graph-parallel`和`data-parallel`统一到一个系统中，并提供了一个唯一的组合`API`。`GraphX`允许用户把数据当做一个图和一个集合（`RDD`），而不需要数据移动或者复制。也就是说`GraphX`统一了`Graph View`和`Table View`，
可以非常轻松的做`pipeline`操作。

## 2 弹性分布式属性图

&emsp;&emsp;`GraphX`的核心抽象是[弹性分布式属性图](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.Graph)，它是一个有向多重图，带有连接到每个顶点和边的用户定义的对象。
有向多重图中多个并行的边共享相同的源和目的顶点。支持并行边的能力简化了建模场景，相同的顶点可能存在多种关系(例如`co-worker`和`friend`)。
每个顶点用一个唯一的64位长的标识符（`VertexID`）作为`key`。`GraphX`并没有对顶点标识强加任何排序。同样，边拥有相应的源和目的顶点标识符。

&emsp;&emsp;属性图扩展了`Spark RDD`的抽象，有`Table`和`Graph`两种视图，但是只需要一份物理存储。两种视图都有自己独有的操作符，从而使我们同时获得了操作的灵活性和执行的高效率。
属性图以`vertex(VD)`和`edge(ED)`类型作为参数类型，这些类型分别是顶点和边相关联的对象的类型。

&emsp;&emsp;在某些情况下，在同样的图中，我们可能希望拥有不同属性类型的顶点。这可以通过继承完成。例如，将用户和产品建模成一个二分图，我们可以用如下方式：

```scala
class VertexProperty()
case class UserProperty(val name: String) extends VertexProperty
case class ProductProperty(val name: String, val price: Double) extends VertexProperty
// The graph might then have the type:
var graph: Graph[VertexProperty, String] = null
```

&emsp;&emsp;和`RDD`一样，属性图是不可变的、分布式的、容错的。图的值或者结构的改变需要生成一个新的图来实现。注意，原始图中不受影响的部分都可以在新图中重用，用来减少存储的成本。
执行者使用一系列顶点分区方法来对图进行分区。如`RDD`一样，图的每个分区可以在发生故障的情况下被重新创建在不同的机器上。

&emsp;&emsp;逻辑上,属性图对应于一对类型化的集合(`RDD`),这个集合包含每一个顶点和边的属性。因此，图的类中包含访问图中顶点和边的成员变量。

```scala
class Graph[VD, ED] {
  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
}
```

&emsp;&emsp;`VertexRDD[VD]`和`EdgeRDD[ED]`类是`RDD[(VertexID, VD)]`和`RDD[Edge[ED]]`的继承和优化版本。`VertexRDD[VD]`和`EdgeRDD[ED]`都提供了额外的图计算功能并提供内部优化功能。

```scala
abstract class VertexRDD[VD](
    sc: SparkContext,
    deps: Seq[Dependency[_]]) extends RDD[(VertexId, VD)](sc, deps) 
    
abstract class EdgeRDD[ED](
    sc: SparkContext,
    deps: Seq[Dependency[_]]) extends RDD[Edge[ED]](sc, deps)
```

## 3 GraphX的图存储模式

&emsp;&emsp;`Graphx`借鉴`PowerGraph`，使用的是`Vertex-Cut`( 点分割 ) 方式存储图，用三个`RDD`存储图数据信息：

- `VertexTable(id, data)`：`id`为顶点`id`， `data`为顶点属性

- `EdgeTable(pid, src, dst, data)`：`pid` 为分区`id` ，`src`为源顶点`id` ，`dst`为目的顶点`id`，`data`为边属性

- `RoutingTable(id, pid)`：`id` 为顶点`id` ，`pid` 为分区`id`

&emsp;&emsp;点分割存储实现如下图所示：

<div  align="center"><img src="imgs/2.4.png" width = "600" height = "370" alt="2.3" align="center" /></div><br />

&emsp;&emsp;在后文的[图构建](build-graph.md)部分，我们会详细介绍这三个部分。

## 4 GraphX底层设计的核心点

- 1 对`Graph`视图的所有操作，最终都会转换成其关联的`Table`视图的`RDD`操作来完成。一个图的计算在逻辑上等价于一系列`RDD`的转换过程。因此，`Graph`最终具备了`RDD`的3个关键特性：不变性、分布性和容错性。其中最关键的是不变性。逻辑上，所有图的转换和操作都产生了一个新图；物理上，`GraphX`会有一定程度的不变顶点和边的复用优化，对用户透明。

- 2 两种视图底层共用的物理数据，由`RDD[VertexPartition]`和`RDD[EdgePartition]`这两个`RDD`组成。点和边实际都不是以表`Collection[tuple]`的形式存储的，而是由`VertexPartition/EdgePartition`在内部存储一个带索引结构的分片数据块，以加速不同视图下的遍历速度。不变的索引结构在`RDD`转换过程中是共用的，降低了计算和存储开销。

- 3 图的分布式存储采用点分割模式，而且使用`partitionBy`方法，由用户指定不同的划分策略。下一章会具体讲到划分策略。

## 5 参考文献

【1】[spark graphx参考文献](https://github.com/endymecy/spark-programming-guide-zh-cn/tree/master/graphx-programming-guide)

【2】[快刀初试：Spark GraphX在淘宝的实践](http://www.csdn.net/article/2014-08-07/2821097)

【3】[GraphX: Unifying Data-Parallel and Graph-Parallel](docs/graphx.pdf)