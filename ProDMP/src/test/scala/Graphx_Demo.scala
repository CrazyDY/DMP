import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Graphx_Demo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AcctfileProcess")
      //参数设置
      .setMaster("local")
      .set("spark.worker.timeout", "500")
      .set("spark.cores.max", "10")
      .set("spark.rpc.askTimeout", "600s")
      .set("spark.network.timeout", "600s")
      .set("spark.task.maxFailures", "1")
      .set("spark.speculationfalse", "false")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sqlContext =
      SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    //TODO 构建点
    val vertices=Array((1L, ("北京")),(2L, ("上海")),(3L,("深圳")))
    val verticesRDD: RDD[(VertexId, String)] =
      sparkContext.parallelize(vertices)
    //TODO 构建边
    val edges = Array(Edge(1L,2L,1226),Edge(2L,3L,1435),Edge(3L,1L,2160))
    val edgesRDD: RDD[Edge[Int]] = sparkContext.parallelize(edges)
    //TODO 构建图
    val graph: Graph[String, Int] = Graph(verticesRDD, edgesRDD)

    //1. 有多少个机场
    val num_Airports = graph.numVertices
    //2. 有多少条航线
    val num_Routes = graph.numEdges
    //3.那条航线大于1400km
    val num_distance = graph.edges.filter{
      case Edge(srcid , destid , distance) => distance >= 1400
    }
    //4. 排序并打印最长线路
    val maxDistance = graph.triplets.sortBy(_.attr , ascending =
      false).map{
      line =>
        "from " + line.srcAttr + " to " + line.dstAttr +" distance is :" +
          line.attr.toString
    }
    //打印结果
    println("飞机场个数 " , num_Airports)
    println("航线条数" , num_Routes)
    println("大于1400km的航线" , num_distance.collect().toBuffer)
    println("最长距离" , maxDistance.take(1).toBuffer)
  }
}
