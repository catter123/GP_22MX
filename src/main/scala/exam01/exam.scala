package exam01

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object exam {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)


   val json: RDD[String] = sc.textFile("D:\\Git\\GP_22MX\\src\\main\\scala\\exam01\\dir\\json.txt")


    //取值
    val all: RDD[ListBuffer[(String, String, String)]] = json.map(kk => {

      var list = ListBuffer[(String, String, String)]()

      val jsonParase: JSONObject = JSON.parseObject(kk)
      val status: Int = jsonParase.getIntValue("status")
      if (status == 0)  ("","","")

      else {
        val regeocode: JSONObject = jsonParase.getJSONObject("regeocode")

        if (regeocode == null || regeocode.keySet().isEmpty)  ("","","")

        else {
          val pois: JSONArray = regeocode.getJSONArray("pois")
          if (pois == null || pois.isEmpty)  ("","","")

          else {
            var i = 0
            for (i <- 0 until pois.size()) {

              //直接一起获取
              //同意计算

              val poi: JSONObject = pois.getJSONObject(i)
              //获取businessarea
              val businessarea = poi.get("businessarea").toString
              //获取id
              val id = poi.get("id").toString
              //获取type
              val types = poi.get("type").toString
              list.append((id, businessarea, types))

            }
          }
        }
      }
      list
    })


    //type计算
    val tuples: ListBuffer[(String, Int)] = all.map(_.map(tup => (
      tup._3.split(";").map((_, 1))
      )


    ).flatten).reduce((a, b) => a ++ b)
    sc.makeRDD(tuples).reduceByKey(_+_).foreach(println)



    //business计算
    all.map(_.map(tup => (
      tup._2.split(";").map((_,1))
      )


    ).flatten.groupBy(_._1).mapValues(_.size)

    ).foreach(println)


  }
}
