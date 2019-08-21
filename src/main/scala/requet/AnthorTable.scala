package requet

//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession
//
///**
//  * @author catter
//  * @date 2019/8/21 19:35
//  */
//object AnthorTable {
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setAppName("anthortable").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//    sc.textFile("C:/Users/catter/Desktop/app_dict.txt").map(line=>{
//      val str: Array[String] = line.split("\t")
//      val Appname = str(1)
//      val idd = str(4)
//      (Appname,idd)
//    }).saveAsTextFile("D:/feQ/share/Spark用户画像分析/app.txt")
//
//
//  }
//
//}
