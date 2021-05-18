# 流处理基础概念
对于什么是流处理，从不同的角度有不同的定义。其实流处理与批处理这两个概念是对立统一的，它们的关系有点类似于对于 Java 中的 List ，是直接看作一个有限数据集并用下标去访问，还是用迭代器去访问。

```java
List<Integer> batchData = new ArrayList<Integer>();
batchData.add(...);
​
for (int i = 0; i < batchData.size(); i++) {
    batchData.get(i);      
}
​
batchData.forEach(data -> {
    ...
});
```

流处理系统本身有很多自己的特点。一般来说，由于需要支持无限数据集的处理，流处理系统一般采用一种数据驱动的处理方式。它会提前设置一些算子，然后等到数据到达后对数据进行处理。为了表达复杂的计算逻辑，包括Flink在内的分布式流处理引擎一般采用DAG图来表示整个计算逻辑，其中DAG图中的每一个点就代表一个基本的逻辑单元，也就是前面说的算子。由于计算逻辑被组织成有向图，数据会按照边的方向，从一些特殊的 Source 节点流入系统，然后通过网络传输、本地传输等不同的数据传输方式在算子之间进行发送和处理，最后会通过另外一些特殊的 Sink 节点将计算结果发送到某个外部系统或数据库中。

# Flink DataStream API 概览

```java
//1、设置运行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//2、配置数据源读取数据
DataStream<String> text = env.readTextFile ("input");
//3、进行一系列转换
DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).keyBy(0).sum(1);
//4、配置数据汇写出数据
counts.writeAsText("output");
//5、提交执行
env.execute("Streaming WordCount");
```

为了实现流式 Word Count，我们首先要先获得一个 StreamExecutionEnvironment 对象。它是我们构建图过程中的上下文对象。基于这个对象，我们可以添加一些算子。对于流处理程度，我们一般需要首先创建一个数据源去接入数据。在这个例子中，我们使用了 Environment 对象中内置的读取文件的数据源。这一步之后，我们拿到的是一个 DataStream 对象，它可以看作一个无限的数据集，可以在该集合上进行一序列的操作。例如，在 Word Count 例子中，我们首先将每一条记录（即文件中的一行）分隔为单词，这是通过 FlatMap 操作来实现的。调用 FlatMap 将会在底层的 DAG 图中添加一个 FlatMap 算子。然后，我们得到了一个记录是单词的流。我们将流中的单词进行分组（keyBy），然后累积计算每一个单词的数据（sum(1)）。计算出的单词的数据组成了一个新的流，我们将它写入到输出文件中。

最后，我们需要调用 **env.execute** 方法来开始程序的执行。需要强调的是，前面我们调用的所有方法，都不是在实际处理数据，而是在构通表达计算逻辑的 DAG 图。只有当我们将整个图构建完成并显式的调用 Execute 方法后，框架才会把计算图提供到集群中，接入数据并执行实际的逻辑。

基于流式 Word Count 的例子可以看出，基于 Flink 的 DataStream API 来编写流处理程序一般需要三步：通过 Source 接入数据、进行一系统列的处理以及将数据写出。最后，不要忘记显式调用 Execute 方式，否则前面编写的逻辑并不会真正执行。

从上面的例子中还可以看出，Flink DataStream API 的核心，就是代表流数据的 DataStream 对象。整个计算逻辑图的构建就是围绕调用 DataStream 对象上的不同操作产生新的 DataStream 对象展开的。整体来说，DataStream 上的操作可以分为四类。第一类是对于单条记录的操作，比如筛除掉不符合要求的记录（Filter 操作），或者将每条记录都做一个转换（Map 操作）。第二类是对多条记录的操作。比如说统计一个小时内的订单总成交量，就需要将一个小时内的所有订单记录的成交量加到一起。为了支持这种类型的操作，就得通过 Window 将需要的记录关联到一起进行处理。第三类是对多个流进行操作并转换为单个流。例如，多个流可以通过 Union、Join 或 Connect 等操作合到一起。这些操作合并的逻辑不同，但是它们最终都会产生了一个新的统一的流，从而可以进行一些跨流的操作。最后， DataStream 还支持与合并对称的操作，即把一个流按一定规则拆分为多个流（Split 操作），每个流是之前流的一个子集，这样我们就可以对不同的流作不同的处理。
![image-20210317212539690](https://github.com/BeanCookie/note-images/blob/main/flink004.png)

为了支持这些不同的流操作，Flink 引入了一组不同的流类型，用来表示某些操作的中间流数据集类型。完整的类型转换关系如图4所示。首先，对于一些针对单条记录的操作，如 Map 等，操作的结果仍然是是基本的 DataStream 类型。然后，对于 Split 操作，它会首先产生一个 SplitStream，基于 SplitStream 可以使用 Select 方法来筛选出符合要求的记录并再将得到一个基本的流。

类似的对于 Connect 操作，在调用 streamA.connect(streamB)后可以得到一个专门的 ConnectedStream。ConnectedStream 支持的操作与普通的 DataStream 有所区别，由于它代表两个不同的流混合的结果，因此它允许用户对两个流中的记录分别指定不同的处理逻辑，然后它们的处理结果形成一个新的 DataStream 流。由于不同记录的处理是在同一个算子中进行的，因此它们在处理时可以方便的共享一些状态信息。上层的一些 Join 操作，在底层也是需要依赖于 Connect 操作来实现的。

基本 DataStream 对象上的 allWindow 与 KeyedStream 上的 Window 操作的对比如图5所示。为了能够在多个并发实例上并行的对数据进行处理，我们需要通过 KeyBy 将数据进行分组。KeyBy 和 Window 操作都是对数据进行分组，但是 KeyBy 是在水平分向对流进行切分，而 Window 是在垂直方式对流进行切分。
使用 KeyBy 进行数据切分之后，后续算子的每一个实例可以只处理特定 Key 集合对应的数据。除了处理本身外，Flink 中允许算子维护一部分状态（State），在KeyedStream 算子的状态也是可以分布式存储的。由于 KeyBy 是一种确定的数据分配方式（下文将介绍其它分配方式），因此即使发生 Failover 作业重启，甚至发生了并发度的改变，Flink 都可以重新分配 Key 分组并保证处理某个 Key 的分组一定包含该 Key 的状态，从而保证一致性。
![image-20210317212539690](https://github.com/BeanCookie/note-images/blob/main/flink005.png)