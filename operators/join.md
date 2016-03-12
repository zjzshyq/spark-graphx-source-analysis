# 关联操作

&emsp;&emsp;在许多情况下，有必要将外部数据加入到图中。例如，我们可能有额外的用户属性需要合并到已有的图中或者我们可能想从一个图中取出顶点特征加入到另外一个图中。这些任务可以用`join`操作完成。
主要的`join`操作如下所示。

```scala
class Graph[VD, ED] {
  def joinVertices[U](table: RDD[(VertexId, U)])(map: (VertexId, VD, U) => VD)
    : Graph[VD, ED]
  def outerJoinVertices[U, VD2](table: RDD[(VertexId, U)])(map: (VertexId, VD, Option[U]) => VD2)
    : Graph[VD2, ED]
}
```

&emsp;&emsp;`joinVertices`操作`join`输入`RDD`和顶点，返回一个新的带有顶点特征的图。这些特征是通过在连接顶点的结果上使用用户定义的`map`函数获得的。没有匹配的顶点保留其原始值。
下面详细地来分析这两个函数。

## 1 joinVertices

```scala
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
&emsp;&emsp;我们可以看到，`joinVertices`的实现是通过`outerJoinVertices`来实现的。这是因为`join`本来就是`outer join`的一种特例。

## 2 outerJoinVertices

```scala
override def outerJoinVertices[U: ClassTag, VD2: ClassTag]
      (other: RDD[(VertexId, U)])
      (updateF: (VertexId, VD, Option[U]) => VD2)
      (implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    if (eq != null) {
      vertices.cache()
      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(other)(updateF).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(other)(updateF)
      GraphImpl(newVerts, replicatedVertexView.edges)
    }
  }
```
&emsp;&emsp;通过以上的代码我们可以看到，如果`updateF`不改变类型，我们只需要创建改变的顶点即可，否则我们要重新创建所有的顶点。我们讨论不改变类型的情况。
这种情况分三步。

- 1 修改顶点属性值

```scala
val newVerts = vertices.leftJoin(other)(updateF).cache()
```
&emsp;&emsp;这一步会用顶点`RDD` `join` 传入的`RDD`，然后用`updateF`作用`joinRDD`中的所有顶点，改变它们的值。

- 2 找到发生改变的顶点

```scala
  val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
```

- 3 更新newReplicatedVertexView中边分区中的顶点属性

```scala
val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
```

&emsp;&emsp;第2、3两步的源码已经在[转换操作](transformation.md)中详细介绍。


