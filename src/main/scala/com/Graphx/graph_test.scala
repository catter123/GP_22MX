package com.Graphx

import com.utils.TagUtils
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 图计算案例
  */
object graph_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 构造点的集合
    val vertexRDD = sc.makeRDD(Seq(
      (1L, List("","")),
      (2L, List("","")),
      (6L, List("","")),
      (9L, List("","")),
      (133L, List("哈登", 30)),
      (138L, List("席尔瓦", 36)),
      (16L, List("","")),
      (44L, List("","")),
      (21L, List("","")),
      (5L, List("","")),
      (7L, List("","")),
      (158L, List("码云", 55))
    ))
    // 构造边的集合
    val egde: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))
    // 构建图
    val graph = Graph(vertexRDD,egde)
    // 取出每个边上的最大顶点
    val vertices = graph.connectedComponents().vertices
    vertices.foreach(println)
    println("***********************")
    vertices.join(vertexRDD)
        .map{
      case(userId,(conId,List(name,age)))=>{
        (conId,List(name,age))
      }
    }.filter(_._2(0)!="").reduceByKey(_++_).foreach(println)
  }
}
