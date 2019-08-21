package requet

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * @author catter
  * @date 2019/8/21 10:28
  */
object LocationRPT {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("目录参数不正确,退出程序")
      sys.exit()
    }

    // 创建一个集合保存输出和输出列表
    val Array(inputPath) = args
    val conf=new SparkConf().setAppName("ddd").setMaster("local[*]")
      // 设置序列化方式  采用Kryo序列化方式,比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 创建执行入口
    val sc = new SparkContext(conf)


    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val df = spark.read.parquet(inputPath).rdd
    val tup1 = df.map(row => {
      //去出需要的字段
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")

      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

//            val pro = row.getAs[String]("provincename")
//            val city1 = row.getAs[String]("cityname")
      val d1: List[Double] = RpUtiles.request(requestmode, processnode)
      val d2: List[Double] = RpUtiles.click(requestmode, iseffective)
      val d3 = RpUtiles.Ad(iseffective, isbilling, isbid, iswin, adordeerid,winprice,adpayment)
      d1 ++ d2 ++ d3
    })
//    val res1: RDD[((String, String), List[Double])] = tup.reduceByKey((a, b)=>List(a(0)+b(0),a(1)+b(1),a(2)+b(2),a(3)+b(3),a(4)+b(4),a(5)+b(5),a(6)+b(6),a(7)+b(7),a(8)+b(8)))
//
//    res1.map(x=>(x._1._1,x._1._2,x._2(0),x._2(1),x._2(2),x._2(3),x._2(4),x._2(5),x._2(6),x._2(7),x._2(8))).collect().foreach(println)
val prop = new Properties()
    prop.put("user","root")
    prop.put("password","catter")
    val url="jdbc:mysql://localhost:3306/test"

    /**
      * *****************
      * 地域分布
      * *****************
      */
//    val citypro = df.map(city => {
//      val pro = city.getAs[String]("provincename")
//      val city1 = city.getAs[String]("cityname")
//      (pro, city1)
//    })
//    val tup = citypro.zip(tup1)
//    val city = tup.reduceByKey((list1, list2) => list1.zip(list2).map(t => t._1 + t._2)).map(x => (x._1._1, x._1._2, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)))
//      .toDF("省市", "城市", "原始请问", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "展示量", "点击量", "广告消费", "广告成本")
//


//
//    city.write.jdbc(url,"city1",prop)
//
//
    /**
      * *****************
      * 终端设备
      * *****************
      */
//
//    val isp = df.map(iphone => {
//      val ispname: String = iphone.getAs[String]("ispname")
//      ispname
//    })
//    isp.zip(tup1)
//      .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2)).map(x => (x._1,x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)))
//      .toDF("运营商", "原始请问", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "展示量", "点击量", "广告消费", "广告成本")
//      .write.jdbc(url,"isp",prop)
//
//
//
    /**
      * *****************
      * 网络类
      * *****************
      */
//
//    val tupnetworkmannername = df.map(iphone => {
//      val networkmannername: String = iphone.getAs[String]("networkmannername")
//      networkmannername
//    })
//    tupnetworkmannername.zip(tup1)
//      .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2)).map(x => (x._1,x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)))
//      .toDF("网络类型", "原始请问", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "展示量", "点击量", "广告消费", "广告成本")
//      .write.jdbc(url,"networkmannername",prop)
//

    /**
      * *****************
      * 设备类
      * *****************
      */
//    val tupdevicetype = df.map(iphone => {
//      var str=""
//      val devicetype1: Int = iphone.getAs[Int]("devicetype")
//      if(devicetype1==1){
//       str="手机"
//      }else if(devicetype1==2){
//        str="平板"
//      }else{
//        str="其他"
//      }
//      str
//    })
//    val tupisp = tupdevicetype.zip(tup1)
//      .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2)).map(x => (x._1,x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)))
//      .toDF("设备类型", "原始请问", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "展示量", "点击量", "广告消费", "广告成本")
//      .write.jdbc(url,"devicetype",prop)
//

    /**
      * *****************
      * 操作系
      * *****************
      */
//    val osversion = df.map(iphone => {
//      var client=""
//      val client1: Int = iphone.getAs[Int]("client")
//      if(client1==1){
//        client ="android"
//      }else if (client1==2){
//        client="ios"
//      }else if (client1==3){
//        client="wp"
//      }else{
//        client="其他"
//      }
//      client
//    })
//    osversion.zip(tup1)
//      .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2)).map(x => (x._1,x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)))
//      .toDF("操作系桶", "原始请问", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "展示量", "点击量", "广告消费", "广告成本")
//      .write.jdbc(url,"client",prop)
//

    /**
      * *****************
      * 媒体分析
      * *****************
      */
    val nume= sc.textFile("D:/feQ/share/Spark用户画像分析/app.txt")
      .map(line => {
        val appname = line.substring(1, line.indexOf(","))
        val idd = line.substring(line.indexOf(",") + 1, line.length - 1)
        (idd, appname)

      }).collect().toMap

    val mapp: Broadcast[Map[String, String]] = sc.broadcast(nume)

    val tupmediatype: RDD[String] = df.map(appna => {
      val app = appna.getAs[String]("appname")
      val appid = appna.getAs[String]("appid")
      mapp.value.getOrElse(appid,app)


    })

    tupmediatype.zip(tup1)
          .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2)).map(x => (x._1,x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)))
          .toDF("媒体类型", "原始请问", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "展示量", "点击量", "广告消费", "广告成本")
          .write.jdbc(url,"mediatype",prop)



    /**
      * *****************
      * 渠道报表
      * *****************
      */

//        val tupappname = df.map(iphone => {
//          val appname: String = iphone.getAs[String]("appname")
//          appname
//        })
//    tupappname.zip(tup1)
//          .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2)).map(x => (x._1,x._2(0), x._2(1), x._2(2), x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)))
//          .toDF("渠道名称", "原始请问", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "展示量", "点击量", "广告消费", "广告成本")
//          .write.jdbc(url,"appname",prop)
  }

}
