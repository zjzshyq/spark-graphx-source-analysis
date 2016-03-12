# 缓存操作

&emsp;&emsp;在`Spark`中，`RDD`默认是不缓存的。为了避免重复计算，当需要多次利用它们时，我们必须显示地缓存它们。`GraphX`中的图也有相同的方式。当利用到图多次时，确保首先访问`Graph.cache()`方法。

&emsp;&emsp;在迭代计算中，为了获得最佳的性能，不缓存可能是必须的。默认情况下，缓存的`RDD`和图会一直保留在内存中直到因为内存压力迫使它们以`LRU`的顺序删除。对于迭代计算，先前的迭代的中间结果将填充到缓存
中。虽然它们最终会被删除，但是保存在内存中的不需要的数据将会减慢垃圾回收。只有中间结果不需要，不缓存它们是更高效的。然而，因为图是由多个`RDD`组成的，正确的不持久化它们是困难的。对于迭代计算，我们建议使用`Pregel API`，它可以正确的不持久化中间结果。

&emsp;&emsp;`GraphX`中的缓存操作有`cache`,`persist`,`unpersist`和`unpersistVertices`。它们的接口分别是：

```scala
def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED]
def cache(): Graph[VD, ED]
def unpersist(blocking: Boolean = true): Graph[VD, ED]
def unpersistVertices(blocking: Boolean = true): Graph[VD, ED]
```