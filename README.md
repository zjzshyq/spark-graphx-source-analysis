# `Spark GraphX`源码分析

&emsp;&emsp;`Spark GraphX`是一个新的`Spark API`，它用于图和分布式图(`graph-parallel`)的计算。`GraphX` 综合了 `Pregel` 和 `GraphLab` 两者的优点，即接口相对简单，又保证性能，可以应对点分割的图存储模式，胜任符合幂律分布的自然图的大型计算。
本专题会详细介绍`GraphX`的实现原理，并对`GraphX`的存储结构以及部分操作作详细分析。

&emsp;&emsp;本专题介绍的内容如下：

## 目录

* [分布式图计算](parallel-graph-system.md)
* [GraphX简介](graphx-introduce.md)
* [GraphX点切分存储](vertex-cut.md)
* [vertices、edges和triplets](vertex-edge-triple.md)
* [图的构建](build-graph.md)
* [GraphX的图运算操作](operators/readme.md)
    * [转换操作](operators/transformation.md)
    * [结构操作](operators/structure.md)
    * [关联操作](operators/join.md)
    * [聚合操作](operators/aggregate.md)
    * [缓存操作](operators/cache.md)
* [GraphX Pregel API](pregel-api.md)

