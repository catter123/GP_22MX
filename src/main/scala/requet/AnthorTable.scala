package requet

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author catter
  * @date 2019/8/21 19:35
  */
object AnthorTable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("anthortable").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","catter")
    val url="jdbc:mysql://localhost:3306/adactive"

    sc.textFile("D:/feQ/share/Spark用户画像分析/app_dict.txt").map(_.split("\t")).filter(_.length>=5).map(line=>{
//      val str: Array[String] = line.split("\t")


//      var appname = ""
//      var idd =""
//      if(str.length>=5){
       val appname = line(1)
       val idd = line(4)
//      }
//
      (appname,idd)
    }).filter(t=> !t._1.isEmpty && !t._2.isEmpty).toDF("appname","idd").write.jdbc(url,"dataWithAnthor",prop)




  }

}
