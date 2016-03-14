# `GraphX`中`vertices`、`edges`以及`triplets`

&emsp;&emsp;`vertices`、`edges`以及`triplets`是`GraphX`中三个非常重要的概念。我们在前文[GraphX介绍](graphx-introduce.md)中对这三个概念有初步的了解。

## 1 vertices

&emsp;&emsp;在`GraphX`中，`vertices`对应着名称为`VertexRDD`的`RDD`。这个`RDD`有顶点`id`和顶点属性两个成员变量。它的源码如下所示：

```scala
abstract class VertexRDD[VD](
    sc: SparkContext,
    deps: Seq[Dependency[_]]) extends RDD[(VertexId, VD)](sc, deps) 
```
&emsp;&emsp;从源码中我们可以看到，`VertexRDD`继承自`RDD[(VertexId, VD)]`，这里`VertexId`表示顶点`id`，`VD`表示顶点所带的属性的类别。这从另一个角度也说明`VertexRDD`拥有顶点`id`和顶点属性。

## 2 edges

&emsp;&emsp;在`GraphX`中，`edges`对应着`EdgeRDD`。这个`RDD`拥有三个成员变量，分别是源顶点`id`、目标顶点`id`以及边属性。它的源码如下所示：

```scala
abstract class EdgeRDD[ED](
    sc: SparkContext,
    deps: Seq[Dependency[_]]) extends RDD[Edge[ED]](sc, deps) 
```
&emsp;&emsp;从源码中我们可以看到，`EdgeRDD`继承自`RDD[Edge[ED]]`，即类型为`Edge[ED]`的`RDD`。`Edge[ED]`在后文会讲到。

## 3 triplets

&emsp;&emsp;在`GraphX`中，`triplets`对应着`EdgeTriplet`。它是一个三元组视图，这个视图逻辑上将顶点和边的属性保存为一个`RDD[EdgeTriplet[VD, ED]]`。可以通过下面的`Sql`表达式表示这个三元视图的含义:

```sql
SELECT src.id, dst.id, src.attr, e.attr, dst.attr
FROM edges AS e LEFT JOIN vertices AS src, vertices AS dst
ON e.srcId = src.Id AND e.dstId = dst.Id
```
&emsp;&emsp;同样，也可以通过下面图解的形式来表示它的含义：

<div  align="center"><img src="imgs/3.1.png" width = "550" height = "50" alt="3.1" align="center" /></div><br />

&emsp;&emsp;`EdgeTriplet`的源代码如下所示：

```scala
class EdgeTriplet[VD, ED] extends Edge[ED] {
  //源顶点属性
  var srcAttr: VD = _ // nullValue[VD]
  //目标顶点属性
  var dstAttr: VD = _ // nullValue[VD]
  protected[spark] def set(other: Edge[ED]): EdgeTriplet[VD, ED] = {
    srcId = other.srcId
    dstId = other.dstId
    attr = other.attr
    this
  }
```
&emsp;&emsp;`EdgeTriplet`类继承自`Edge`类，我们来看看这个父类：

```scala
case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
    var srcId: VertexId = 0,
    var dstId: VertexId = 0,
    var attr: ED = null.asInstanceOf[ED])
  extends Serializable
```
&emsp;&emsp;`Edge`类中包含源顶点`id`，目标顶点`id`以及边的属性。所以从源代码中我们可以知道，`triplets`既包含了边属性也包含了源顶点的`id`和属性、目标顶点的`id`和属性。

## 4 参考文献

【1】[spark源码](https://github.com/apache/spark)



