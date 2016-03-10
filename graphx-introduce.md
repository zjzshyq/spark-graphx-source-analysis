# GraphX的实现原理

&emsp;&emsp;`GraphX`是一个新的`Spark API`，它用于图和分布式图(`graph-parallel`)的计算。`GraphX`通过引入弹性分布式属性图（[Resilient Distributed Property Graph](property-graph.md)）：
顶点和边均有属性的有向多重图，来扩展`Spark RDD`。为了支持图计算，`GraphX`开发了一组基本的功能操作以及一个优化过的`Pregel API`。另外，`GraphX`包含了一个快速增长的图算法和图`builders`的
集合，用以简化图分析任务。

&emsp;&emsp;从社交网络到语言建模，不断增长的规模以及图形数据的重要性已经推动了许多新的分布式图系统（如[Giraph](http://giraph.apache.org/)和[GraphLab](http://graphlab.org/)）的发展。
通过限制可表达的计算类型和引入新的技术来划分和分配图，这些系统可以高效地执行复杂的图形算法，比一般的分布式数据计算（`data-parallel`，如`spark`、`mapreduce`）快很多。

<div  align="center"><img src="imgs/2.1.png" width = "600" height = "370" alt="2.1" align="center" /></div><br />

&emsp;&emsp;分布式图（`graph-parallel`）计算和分布式数据（`data-parallel`）计算类似，分布式数据计算采用了一种`record-centric`的集合视图，而分布式图计算采用了一种`vertex-centric`的图视图。
分布式数据计算通过同时处理独立的数据来获得并发的目的，分布式图计算则是通过对图数据进行分区来获得并发的目的。更准确的说，分布式图计算递归的定义特征的转换函数（这种转换函数作用于邻居特征），通过并发地执行这些转换函数来获得并发的目的。

&emsp;&emsp;分布式图计算比分布式数据计算更适合图的处理，但是在典型的图处理流水线中，它并不能处理所有操作。例如，虽然分布式图系统可以很好的计算`PageRank`以及`label diffusion`，但是它们不适合从不同的数据源构建图或者跨过多个图计算特征。
更准确的说，分布式图系统提供的更窄的计算视图无法处理那些构建和转换图结构以及跨越多个图的需求。分布式图系统中无法提供的这些操作需要数据在图本体之上移动并且需要一个图层面而不是单独的顶点或边层面的计算视图。例如，我们可能想限制我们的分析到几个子图上，然后比较结果。
这不仅需要改变图结构，还需要跨多个图计算。

<div  align="center"><img src="imgs/2.2.png" width = "600" height = "370" alt="2.2" align="center" /></div><br />

&emsp;&emsp;我们如何处理数据取决于我们的目标，同一原始数据可能有许多不同表和图的视图，并且图和表之间经常需要能够相互移动。如下图所示：

<div  align="center"><img src="imgs/2.3.png" width = "600" height = "370" alt="2.3" align="center" /></div><br />

&emsp;&emsp;所以我们的图流水线必须通过组成`graph-parallel`和`data- parallel`相结合的系统来实现。但是这种组合必然会导致大量的数据移动以及数据复制。同时这样的系统也非常复杂。
例如，在传统的图计算流水线中，在`Table View`视图下，可能需要`Spark`或者`Hadoop`的支持，在`Graph View`这种视图下，可能需要`Prege`或者`GraphLab`的支持。也就是把图和表分在不同的系统中分别处理。

&emsp;&emsp;`GraphX`项目将`graph-parallel`和`data-parallel`统一到一个系统中，并提供了一个唯一的组合`API`。`GraphX`允许用户将数据当做一个图和一个集合（RDD），而不需要数据移动或者复制。也就是说`GraphX`统一了`Graph View`和`Table View`，
可以非常轻松的做`pipeline`操作。

