package Tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis


/**
  * @author catter
  * @date 2019/8/23 10:20
  *
  *       标签上下文
  */
object TagContext {
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("目录不匹配")
    }

    val Array(inputPath, outputPath, dirPath, stopPath) = args

    //创建上下文
    val conf = new SparkConf().setAppName("Tages").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()


    //获取数据字典
    val nume = sc.textFile(dirPath)
      .mapPartitions(rdd => {
        rdd.map(line => {
          val appname = line.substring(1, line.indexOf(","))
          val idd = line.substring(line.indexOf(",") + 1, line.length - 1)
          (idd, appname)
        })
      })
    val broad: Broadcast[Map[String, String]] = sc.broadcast(nume.collect().toMap)


    /**
      * jedis实现数据字典
      */
    nume.mapPartitions(rdd => {
      val jedis: Jedis = Pool.getRedisPool()
      try {
        rdd.map(app => {
          jedis.hset("app", app._1, app._2)
        })
      } finally {
        jedis.close()
      }
    })

    //停用词库
    val stopWorda: RDD[(String, Int)] = sc.textFile(stopPath).map(t => (t, 1))
    val stopword: Broadcast[Map[String, Int]] = sc.broadcast(stopWorda.collect().toMap)

    /**
      * jedis实现停用词库
      */


    stopWorda.mapPartitions(rdd => {
      val jedis: Jedis = Pool.getRedisPool()
      try {
        rdd.map(t => {
          jedis.hset("stopworda", s"$t._2", s"$t._1")
        })
      } finally {
        jedis.close()
      }
    })

    /**
      * 商圈
      */
//    val list = List("116.310003,39.991957")
//    val rdd = sc.makeRDD(list)
//    rdd.map(t=>{
//      val str = t.split(",")
//      TagsAD.makebuccessTags(str(0).toDouble,str(1).toDouble)
//    })
//    println(TagsAD.makebuccessTags(116.310003, 39.991957))

    //读取数据
    val df = spark.read.parquet(inputPath)


    //过滤符合id的数据
    df.filter(TagUtils.OneUserId)
      //编写标签
      .rdd.mapPartitions(rdd => {
      val jedis: Jedis = Pool.getRedisPool()
      try {
        rdd.map(row => {
          //取出用户id
          val id = TagUtils.getOneUserId(row)
          //打标签
          (
            id,
            TagsAD.makeAdTags(row) ++
              TagsAD.makeAndroidTags(row) ++
              TagsAD.makeAppTags(row, jedis) ++
              TagsAD.makeAPTags(row) ++
              TagsAD.makeispidTags(row) ++
              TagsAD.makeKeyTags(row, jedis) ++
              TagsAD.makeLocationTags(row) ++
              TagsAD.makenetworkTags(row)
//              ++ TagsAD.makebuccessTags(row)
          )
        })
      } finally {
        jedis.close()
      }
    }).reduceByKey((list1,list2)=>(list1:::list2).groupBy(_._1).mapValues(_.foldLeft[Int](0)(_+_._2)).toList)
//        .collect().foreach(println)


    spark.stop()
    sc.stop()

  }

}
