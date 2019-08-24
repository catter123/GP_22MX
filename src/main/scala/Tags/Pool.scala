package Tags

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @author catter
  * @date 2019/8/23 20:33
  */
object Pool {
  private val conf = new JedisPoolConfig()
  conf.setMaxIdle(10)
  conf.setMaxTotal(30)
  val pool = new JedisPool(conf,"hadoop01",6379)
  def getRedisPool():Jedis={
    pool.getResource
  }

}
