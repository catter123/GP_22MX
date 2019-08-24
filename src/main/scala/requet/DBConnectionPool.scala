package requet

/**
  * @author catter
  * @date 2019/8/22 14:10
  */
import java.util.concurrent.ConcurrentHashMap
import com.jolbox.bonecp.{ BoneCPConfig, BoneCP }
import java.util.ResourceBundle
import java.util.LinkedList
import java.sql.DriverManager
import java.sql.Connection

/**f
  * 数据库连接池工具类
  * 语言：scala
  * 时间：2016-07-09
  */
object DBConnectionPool {
  private val reader = ResourceBundle.getBundle("connection")
  private val max_connection = reader.getString("jdbc.max_connection") //连接池总数
  private val connection_num = reader.getString("jdbc.connection_num") //产生连接数
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new LinkedList[Connection]() //连接池
  private val driver = reader.getString("jdbc.driver")
  private val url = reader.getString("jdbc.url")
  private val username = reader.getString("jdbc.username")
  private val password = reader.getString("jdbc.password")
  /**
    * 加载驱动
    */
  private def before() {
    if (current_num > max_connection.toInt && pools.isEmpty()) {
      print("busyness")
      Thread.sleep(2000)
      before()
    } else {
      Class.forName(driver)
    }
  }
  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    val conn = DriverManager.getConnection(url, username, password)
    conn
  }
  /**
    * 初始化连接池
    */
  private def initConnectionPool(): LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }
  /**
    * 获得连接
    */
  def getConn():Connection={
    initConnectionPool()
    pools.poll()
  }
  /**
    * 释放连接
    */
  def releaseCon(con:Connection){
    pools.push(con)
  }

}