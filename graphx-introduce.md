# GraphX的实现原理

&emsp;&emsp;`GraphX`是一个新的`Spark API`，它用于图和分布式图(`graph-parallel`)的计算。`GraphX`通过引入[Resilient Distributed Property Graph](property-graph.md)：带有
顶点和边均有属性的有向多重图，来扩展`Spark RDD`。为了支持图计算，`GraphX`公开一组基本的功能操作以及一个优化过的`Pregel API`。另外，`GraphX`包含了一个日益增长的图算法和图`builders`的
集合，用以简化图分析任务。

