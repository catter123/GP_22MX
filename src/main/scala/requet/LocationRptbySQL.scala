package requet

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author catter
  * @date 2019/8/21 11:13
  */
object LocationRptbySQL {
  def main(args: Array[String]): Unit = {
//    if(args.length != 2){
//      println("目录参数不正确,退出程序")
//      sys.exit()
//    }
//
//    // 创建一个集合保存输出和输出列表
//    val Array(inputPath,outputPath) = args
    //初始化配置
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //获取数据
    val df: DataFrame = spark.read.parquet("D:/test/kk1")

    //处理数据
    df.createOrReplaceTempView("test2")

    //地域分布
    spark.sql(
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode==1 and processnode>=1 then 1 else 0 end) as `原始请求`,
        |sum(case when requestmode==1 and processnode>=2 then 1 else 0 end) as `有效请求`,
        |sum(case when requestmode==1 and processnode>=3 then 1 else 0 end) as `广告请求`,
        |sum(case when iseffective=='1' and isbilling =='1' and isbid=='1' then 1 else 0 end) as `参与竞标数`,
        |sum(case when iseffective=='1' and isbilling =='1' and iswin=='1' and adorderid !=0 then 1 else 0 end) as `成功竞标数`,
        |sum(case when iseffective=='1' and requestmode==2 then 1 else 0 end) as `展示数`,
        |sum(case when iseffective=='1' and requestmode==3 then 1 else 0 end) as `点击数`,
        |sum(case when iseffective=='1' and isbilling =='1' and iswin=='1' then 1 else 0 end)/1000 as `DSP广告消费`,
        |sum(case when iseffective=='1' and isbilling =='1' and iswin=='1' then 1 else 0 end)/1000 as `DSP广告成本`
        |from
        |test2
        |group by provincename,cityname
      """.stripMargin).show()

    //终端设备
    spark.sql(
      """
        |select
        |ispname,
        |sum(case when requestmode==1 and processnode>=1 then 1 else 0 end) as `原始请求`,
        |sum(case when requestmode==1 and processnode>=2 then 1 else 0 end) as `有效请求`,
        |sum(case when requestmode==1 and processnode>=3 then 1 else 0 end) as `广告请求`,
        |sum(case when iseffective=='1' and isbilling =='1' and isbid=='1' then 1 else 0 end) as `参与竞标数`,
        |sum(case when iseffective=='1' and isbilling =='1' and iswin=='1' and adorderid !=0 then 1 else 0 end) as `成功竞标数`,
        |sum(case when iseffective=='1' and requestmode==2 then 1 else 0 end) as `展示数`,
        |sum(case when iseffective=='1' and requestmode==3 then 1 else 0 end) as `点击数`,
        |sum(case when iseffective=='1' and isbilling =='1' and iswin=='1' then 1 else 0 end)/1000 as `DSP广告消费`,
        |sum(case when iseffective=='1' and isbilling =='1' and iswin=='1' then 1 else 0 end)/1000 as `DSP广告成本`
        |from
        |test2
        |group by ispname
      """.stripMargin).show()



  }

}
