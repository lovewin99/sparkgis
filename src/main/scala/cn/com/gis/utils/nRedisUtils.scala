package cn.com.gis.utils

/**
 * Created by wangxy on 15-12-28.
 */

import com.utils.ConfigUtils
import redis.clients.jedis.{Pipeline, Jedis, JedisPool, JedisPoolConfig}
import scala.collection.JavaConversions._

object nRedisUtils {
  val propFile = "/config/redis.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val host = prop.getOrElse("REDIS.HOST", "10.95.3.139")
  val port = prop.getOrElse("REDIS.PORT", "6379").toInt

  val config: JedisPoolConfig = new JedisPoolConfig
  config.setMaxActive(200)
  config.setMaxIdle(100)
  config.setMaxWait(10000)
  config.setTestOnBorrow(true)

  var pool : JedisPool = null

  def initPool = {
    //    println(s"host=$host  port=$port")
    pool = new JedisPool(config, host, port)
  }

  def getJedis: Jedis = {
    pool.getResource()
  }

  def close(pool: JedisPool, r: Jedis) = {
    if (r != null)
      pool.returnResourceObject(r)
  }

  def withConnection[A](block: Jedis => Unit) = {
    implicit var redis = this.getJedis
    try {
      block(redis)
    } catch{
      case e : Exception => System.err.println(e)  //should use log in production
      //      case _ => //never should happen
    }finally {
      this.close(pool, redis)
    }
  }

  def destroyPool = {
    pool.destroy
  }

  def getMapFromRedis(tableName: String): scala.collection.immutable.Map[String, String] = {
    initPool
    val redis = this.getJedis
    try {
      val m = redis.hgetAll(tableName)
      m.toMap
    } catch{
      case e : Exception => System.err.println(e); scala.collection.immutable.Map[String, String]() //should use log in production
      //      case _ => //never should happen
    }finally {
      this.close(pool, redis)
      destroyPool
    }
  }

  def putMap2Redis(tableName: String, map: Map[String, String]) : Unit ={
    initPool

    val j: Jedis = getJedis
    withConnection{j =>
      val pipe: Pipeline = j.pipelined

      map.foreach(x => {
        pipe.hset(tableName, x._1, x._2)
      })
      pipe.sync
    }

    destroyPool
  }

  def deltable(tableName: String): Unit = {
    initPool
    val j: Jedis = getJedis
    withConnection{j =>
      val start1: Long = System.currentTimeMillis
      val pipe: Pipeline = j.pipelined

      pipe.del(tableName)
      pipe.sync
    }
    destroyPool
  }
}
