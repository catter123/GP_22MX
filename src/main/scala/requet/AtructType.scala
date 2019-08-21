package requet

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}



/**
  * @author catter
  * @date 2019/8/20 12:04
  */
object ADD {
  def main(args: Array[String]): Unit = {
    // 1 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确,退出程序")
      sys.exit()
    }

    // 创建一个集合保存输出和输出列表
    val Array(inputPath,outputPath) = args
    val conf=new SparkConf().setAppName("ddd").setMaster("local[*]")
      // 设置序列化方式  采用Kryo序列化方式,比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 创建执行入口
    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)

    // 设置压缩方式 使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 进行数据的读取,处理分析数据
    val lines =sc.textFile(inputPath)
    // 按要求切割,并且保证数据的长度大于等于85个字段,
    // -1: 如果切割的时候遇到相同切割条件重复的情况下,那么后面需要加上对应的匹配参数
    // 这样切割才会准确, 比如,,,,,,会当成一个字符切割,需要加上对应的匹配参数
    //    val count: Long = lines.map(_.split(",",-1)).filter(_.length >= 85).count()
    val count= lines.map(t =>t.split(",",t.length)).filter(_.length >= 85)
      .map(arr=>{
        Row(
          arr(0),
          Utils2Type.toInt(arr(1)),
          Utils2Type.toInt(arr(2)),
          Utils2Type.toInt(arr(3)),
          Utils2Type.toInt(arr(4)),
          arr(5),
          arr(6),
          Utils2Type.toInt(arr(7)),
          Utils2Type.toInt(arr(8)),
          Utils2Type.toDouble(arr(9)),
          Utils2Type.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          Utils2Type.toInt(arr(17)),
          arr(18),
          arr(19),
          Utils2Type.toInt(arr(20)),
          Utils2Type.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          Utils2Type.toInt(arr(26)),
          arr(27),
          Utils2Type.toInt(arr(28)),
          arr(29),
          Utils2Type.toInt(arr(30)),
          Utils2Type.toInt(arr(31)),
          Utils2Type.toInt(arr(32)),
          arr(33),
          Utils2Type.toInt(arr(34)),
          Utils2Type.toInt(arr(35)),
          Utils2Type.toInt(arr(36)),
          arr(37),
          Utils2Type.toInt(arr(38)),
          Utils2Type.toInt(arr(39)),
          Utils2Type.toDouble(arr(40)),
          Utils2Type.toDouble(arr(41)),
          Utils2Type.toInt(arr(42)),
          arr(43),
          Utils2Type.toDouble(arr(44)),
          Utils2Type.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          Utils2Type.toInt(arr(57)),
          Utils2Type.toDouble(arr(58)),
          Utils2Type.toInt(arr(59)),
          Utils2Type.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          Utils2Type.toInt(arr(73)),
          Utils2Type.toDouble(arr(74)),
          Utils2Type.toDouble(arr(75)),
          Utils2Type.toDouble(arr(76)),
          Utils2Type.toDouble(arr(77)),
          Utils2Type.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          Utils2Type.toInt(arr(84))
        )
      })
  val dataFrame = sQLContext.createDataFrame(count,SchemaUtils.structtype)

    dataFrame.write.parquet(outputPath)

    val spark = SparkSession.builder().config(conf).getOrCreate()

    //获取数据
    val frame: DataFrame = spark.read.parquet(outputPath).select("provincename","cityname")
    frame.createOrReplaceTempView("test")
    spark.sql("select * from test").show()
    val tup = frame.rdd.map(x => {
      val provincename = x(0).toString
      val cityname = x(1).toString
      ((provincename, cityname), 1)
    })
    //分组，统计
    val end: RDD[((String, String), Int)] = tup.reduceByKey(_+_)

    val ffm= end.map(x=>Row(x._1._1,x._1._2,x._2))
    val structType = StructType(
      Seq(
        StructField("provincename",StringType),
        StructField("cityname", StringType),
        StructField("count", IntegerType)
      )
    )
    val fv = spark.createDataFrame(ffm,structType)

    //导出到HDFS
//    fv.write.mode("append").json("")


    //导出到数据库
    ////    val pp = new Properties()
    ////    pp.put("user","root")
    ////    pp.put("password","catter")
    ////
    ////    fv.write.jdbc("jdbc:mysql://localhost:3306/test","add1",pp)
    //
    //
    ////    fv.write.format("jdbc")
    ////          .option("url","jdbc:mysql://localhost:3306/test")
    ////          .option("dbtable","add1")
    ////          .option("user","root")
    ////          .option("password","catter")
    ////          .save()
    //////
//        sc.stop()


  }

}






object Utils2Type {
  // String转换int
  def toInt(str: String): Int = {
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }
  }

  // String转换Double
  def toDouble(str: String): Double = {
    try {
      str.toDouble
    } catch {
      case _: Exception => 0
    }
  }
}

import org.apache.spark.sql.types._

object SchemaUtils {

  val structtype = StructType(
    Seq(
      StructField("sessionid",StringType),
      StructField("advertisersid",IntegerType),
      StructField("adorderid",IntegerType),
      StructField("adcreativeid",IntegerType),
      StructField("adplatformproviderid",IntegerType),
      StructField("sdkversion",StringType),
      StructField("adplatformkey",StringType),
      StructField("putinmodeltype",IntegerType),
      StructField("requestmode",IntegerType),
      StructField("adprice",DoubleType),
      StructField("adppprice",DoubleType),
      StructField("requestdate",StringType),
      StructField("ip",StringType),
      StructField("appid",StringType),
      StructField("appname",StringType),
      StructField("uuid",StringType),
      StructField("device",StringType),
      StructField("client",IntegerType),
      StructField("osversion",StringType),
      StructField("density",StringType),
      StructField("pw",IntegerType),
      StructField("ph",IntegerType),
      StructField("long",StringType),
      StructField("lat",StringType),
      StructField("provincename",StringType),
      StructField("cityname",StringType),
      StructField("ispid",IntegerType),
      StructField("ispname",StringType),
      StructField("networkmannerid",IntegerType),
      StructField("networkmannername",StringType),
      StructField("iseffective",IntegerType),
      StructField("isbilling",IntegerType),
      StructField("adspacetype",IntegerType),
      StructField("adspacetypename",StringType),
      StructField("devicetype",IntegerType),
      StructField("processnode",IntegerType),
      StructField("apptype",IntegerType),
      StructField("district",StringType),
      StructField("paymode",IntegerType),
      StructField("isbid",IntegerType),
      StructField("bidprice",DoubleType),
      StructField("winprice",DoubleType),
      StructField("iswin",IntegerType),
      StructField("cur",StringType),
      StructField("rate",DoubleType),
      StructField("cnywinprice",DoubleType),
      StructField("imei",StringType),
      StructField("mac",StringType),
      StructField("idfa",StringType),
      StructField("openudid",StringType),
      StructField("androidid",StringType),
      StructField("rtbprovince",StringType),
      StructField("rtbcity",StringType),
      StructField("rtbdistrict",StringType),
      StructField("rtbstreet",StringType),
      StructField("storeurl",StringType),
      StructField("realip",StringType),
      StructField("isqualityapp",IntegerType),
      StructField("bidfloor",DoubleType),
      StructField("aw",IntegerType),
      StructField("ah",IntegerType),
      StructField("imeimd5",StringType),
      StructField("macmd5",StringType),
      StructField("idfamd5",StringType),
      StructField("openudidmd5",StringType),
      StructField("androididmd5",StringType),
      StructField("imeisha1",StringType),
      StructField("macsha1",StringType),
      StructField("idfasha1",StringType),
      StructField("openudidsha1",StringType),
      StructField("androididsha1",StringType),
      StructField("uuidunknow",StringType),
      StructField("userid",StringType),
      StructField("iptype",IntegerType),
      StructField("initbidprice",DoubleType),
      StructField("adpayment",DoubleType),
      StructField("agentrate",DoubleType),
      StructField("lomarkrate",DoubleType),
      StructField("adxrate",DoubleType),
      StructField("title",StringType),
      StructField("keywords",StringType),
      StructField("tagid",StringType),
      StructField("callbackdate",StringType),
      StructField("channelid",StringType),
      StructField("mediatype",IntegerType)
    )
  )
}
