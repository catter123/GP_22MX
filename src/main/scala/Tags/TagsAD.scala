package Tags

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


/**
  * @author catter
  * @date 2019/8/23 11:19
  *
  *       g广告标签
  */
object TagsAD extends Tag {
  override def makeAdTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    //取到类型和名称
    val adtype: Int = row.getAs[Int]("adspacetype")
    adtype match {
      case v if v > 9 => list :+= ("LC" + v, 1)
      case v if v <= 9 && v > 0 => list :+= ("LC0" + v, 1)
    }
    val adname: String = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adname)) {
      list :+= ("LN" + adname, 1)
    }
    list
  }


  override def makeAPTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

    list :+= ("CN" + adplatformproviderid, 1)

    list

  }

  override def makeAndroidTags(args: Any*): List[(String, Int)] = {
    //
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val client = row.getAs[Int]("client")

    if (client == 1) {
      list :+= ("D00010001", 1)
    } else if (client == 2) {
      list :+= ("D00010002", 1)
    } else if (client == 3) {
      list :+= ("D00010003", 1)
    } else {
      list :+= ("D00010004", 1)
    }
    list
  }

  override def makenetworkTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]

    val networkmannername = row.getAs[String]("networkmannername")
    if (networkmannername.equals("WIFI")) {
      list :+= ("D00020001", 1)
    } else if (networkmannername.equals("4G")) {
      list :+= ("D00020002", 1)
    } else if (networkmannername.equals("3G")) {
      list :+= ("D00020003", 1)
    } else if (networkmannername.equals("2G")) {
      list :+= ("D00020004", 1)
    } else {
      list :+= ("D00020005", 1)
    }
    list
  }


  override def makeispidTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val ispname = row.getAs[String]("ispname")
    if (ispname.equals("移动")) {
      list :+= ("D00030001", 1)
    } else if (ispname.equals("联通")) {
      list :+= ("D00030002", 1)
    } else if (ispname.equals("电信")) {
      list :+= ("D00030003", 1)
    } else {
      list :+= ("D00030004", 1)
    }
    list
  }


  override def makeAppTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    //    val broad = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    val jedis = args(1).asInstanceOf[Jedis]

    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")

    if (appname.equals("") || appname.isEmpty) {
      list :+= ("APP" + jedis.hget("app", appid), 1)
    } else {
      list :+= ("APP" + appname, 1)
    }

    list

  }

  override def makeKeyTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    //    val stopword = args(1).asInstanceOf[Broadcast[Map[String, Int]]]
    val jedis = args(1).asInstanceOf[Jedis]
    val keywords = row.getAs[String]("keywords")
    val str = keywords.split("|", -1)
    //    str.filter(word => word.length>=3 && word.length<=8 && stopword.value.contains(word))
    //        .foreach(t=>list:+=("k"+t,1))

    str.filter(word => word.length >= 3 && word.length <= 8 && jedis.hgetAll("stopworda").containsValue(word))
      .foreach(t => list :+= ("k" + t, 1))
    list
  }

  override def makeLocationTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val rtbprovince = row.getAs[String]("rtbprovince")
    val rtbcity = row.getAs[String]("rtbcity")
    if (StringUtils.isNotBlank(rtbprovince)) {
      list :+= ("ZP" + rtbprovince, 1)
    }
    if (StringUtils.isNotBlank(rtbprovince)) {
      list :+= ("ZC" + rtbcity, 1)
    }

    list
  }

    override def makebuccessTags(args: Any*): List[(String, Int)]= {
//      val row = args(0).asInstanceOf[Row]
//      val long = row.getAs[String]("long")
//      val lat = row.getAs[String]("lat")
      var lists =List[(String,Int)]()
//      val list = collection.mutable.ListBuffer[String]()
      val location=args(0)+","+args(1)
//      val location=long+","+lat
      val urlstr= "https://restapi.amap.com/v3/geocode/regeo?&location="+location+"&key=ee8a55244015eb7dbb402c232381cbc9"

      //调用请求获取处理俄国
      val jois: String = HttpUtil.get(urlstr)

      //获取json穿，解析json
      val jsonstr: JSONObject = JSON.parseObject(jois)

      val stutus = jsonstr.getIntValue("status")
      if(stutus==0){
        lists :+= ("",1)
      }

      //判断key的value不能为空
      val regeocode = jsonstr.getJSONObject("regeocode")
      if(regeocode==null || regeocode.keySet().isEmpty) {
        lists :+= ("",1)
      }

      val addressComponent = regeocode.getJSONObject("addressComponent")
      if(addressComponent==null || addressComponent.keySet().isEmpty){
        lists :+= ("",1)
      }

      val businessAreas = addressComponent.getJSONArray("businessAreas")
      if(businessAreas==null || businessAreas.isEmpty){
        lists :+= ("",1)
      }
      //循环输出
      //创建集合保存数据

      for (item<-businessAreas.toArray()){
        val json = item.asInstanceOf[JSONObject]
        lists:+=(json.getString("name"),1)
      }

      lists



    }




}
