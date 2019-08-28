import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * @author catter
  * @date 2019/8/26 18:49
  */
class TestHbase {
  @Test
  def testOne(): Unit = {
    // 加载配置文件
    val load = ConfigFactory.load("application.conf")
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    println(hbconn)
  }

}
