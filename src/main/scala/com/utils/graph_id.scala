package com.utils

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row

/**
  * @author catter
  * @date 2019/8/26 21:31
  */
object graph_id {

//  def getGraph(args:Any*):(String,List[String])={
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val row = args.asInstanceOf[Row]
//    val list1 = args(0).asInstanceOf[(List[String],List[(String,Int)])]
//    val list1_key = list1._1
//    val list2 = args(0).asInstanceOf[(List[String],List[(String,Int)])]
//    val list2_key = list2._1
//    val list1_vextor = sc.makeRDD(Seq(
//      (list1_key(0).toInt, list1),
//      (list1_key(1).toInt, list1),
//      (list1_key(2).toInt, list1),
//      (list1_key(3).toInt, list1),
//      (list1_key(4).toInt, list1),
//      (list1_key(5).toInt, list1),
//      (list1_key(6).toInt, list1),
//      (list1_key(7).toInt, list1),
//      (list1_key(8).toInt, list1),
//      (list1_key(9).toInt, list1),
//      (list1_key(10).toInt, list1),
//      (list1_key(11).toInt, list1),
//      (list1_key(12).toInt, list1),
//      (list1_key(13).toInt, list1),
//      (list1_key(14).toInt, list1),
//      (0, list1),
//      (list2_key(0).toInt, list2),
//      (list2_key(1).toInt, list2),
//      (list2_key(2).toInt, list2),
//      (list2_key(3).toInt, list2),
//      (list2_key(4).toInt, list2),
//      (list2_key(5).toInt, list2),
//      (list2_key(6).toInt, list2),
//      (list2_key(7).toInt, list2),
//      (list2_key(8).toInt, list2),
//      (list2_key(9).toInt, list2),
//      (list2_key(10).toInt, list2),
//      (list2_key(11).toInt, list2),
//      (list2_key(12).toInt, list2),
//      (list2_key(13).toInt, list2),
//      (list2_key(14).toInt, list2),
//      (1, list2),
//    ))
//
//    val list1_adger = sc.makeRDD(Seq(
//      Edge(list1_key(0).toInt, 0,0),
//      Edge(list1_key(1).toInt, 0,0),
//      Edge(list1_key(2).toInt, 0,0),
//      Edge(list1_key(3).toInt, 0,0),
//      Edge(list1_key(4).toInt, 0,0),
//      Edge(list1_key(5).toInt, 0,0),
//      Edge(list1_key(6).toInt, 0,0),
//      Edge(list1_key(7).toInt, 0,0),
//      Edge(list1_key(8).toInt, 0,0),
//      Edge(list1_key(9).toInt, 0,0),
//      Edge(list1_key(10).toInt, 0,0),
//      Edge(list1_key(11).toInt, 0,0),
//      Edge(list1_key(12).toInt, 0,0),
//      Edge(list1_key(13).toInt, 0,0),
//      Edge(list1_key(14).toInt, 0,0),
//      Edge(list2_key(0).toInt, 1,0),
//      Edge(list2_key(1).toInt, 1,0),
//      Edge(list2_key(2).toInt, 1,0),
//      Edge(list2_key(3).toInt, 1,0),
//      Edge(list2_key(4).toInt, 1,0),
//      Edge(list2_key(5).toInt, 1,0),
//      Edge(list2_key(6).toInt, 1,0),
//      Edge(list2_key(7).toInt, 1,0),
//      Edge(list2_key(8).toInt, 1,0),
//      Edge(list2_key(9).toInt, 1,0),
//      Edge(list2_key(10).toInt, 1,0),
//      Edge(list2_key(11).toInt, 1,0),
//      Edge(list2_key(12).toInt, 1,0),
//      Edge(list2_key(13).toInt, 1,0),
//      Edge(list2_key(14).toInt, 1,0),
//    ))
//    Graph(list1_vextor,list1_adger)
//
//  }
}
