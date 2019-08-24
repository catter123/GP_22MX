package exam01

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSONObject

/**
  * @author catter
  * @date 2019/8/24 15:15
  */
object pois {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Tages").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val list: RDD[List[(String, String)]] = sc.textFile("D:\\Git\\GP_22MX\\src\\main\\scala\\exam01\\dir\\json.txt").map(str => {
      TagUtill.getbuccessTags(str)
    })
    //俺找buiiness
    val rdd: RDD[(String, String)] = sc.makeRDD(list.reduce((a, b)=>a++b))
    rdd.map(t=>(t._1,1)).reduceByKey(_+_).collect().foreach(println)

    val arr: RDD[List[Array[(String, Int)]]] = list.map(_.map(_._2.split(";").map(t=>(t,1))))
    val flatten: List[(String, Int)] = arr.reduce((a, b)=>a++b).flatten
    sc.makeRDD(flatten).reduceByKey(_+_).foreach(println)






  }

}
