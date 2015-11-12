package cn.com.gis.utils

/**
 * Created by wangxy on 15-10-21.
 */

import com.utils.ConfigUtils
import redis.clients.jedis.{Pipeline, Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable.Map

object tRedisPutMap {

  val propFile = "/config/redis.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val host = prop.getOrElse("REDIS.HOST", "127.0.0.1")
  val port = prop.getOrElse("REDIS.PORT", "6379").toInt

  val config: JedisPoolConfig = new JedisPoolConfig
  config.setMaxActive(60)
  config.setMaxIdle(1000)
  config.setMaxWait(10000)
  config.setTestOnBorrow(true)

  var pool : JedisPool = null

  def initPool = {
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

//  def withConnection1[A](block: Jedis => scala.collection.immutable.Map[String, String]): scala.collection.immutable.Map[String, String] = {
//    implicit var redis = this.getJedis
//    try {
//      val res = block(redis)
//      res match {
//        case Some(map) => res
//        case _ => scala.collection.immutable.Map()
//      }
//    } catch{
//      case e : Exception => System.err.println(e);  scala.collection.immutable.Map() //should use log in production
//      //      case _ => //never should happen
//    }finally {
//      this.close(pool, redis)
//    }
//  }

//  import scala.collection.JavaConversions.

//  def getResultMap(tableName: String): scala.collection.immutable.Map[String, String] = {
//    initPool
//
//    val j: Jedis = getJedis
//
//    val result = withConnection1{j =>
//      val start1: Long = System.currentTimeMillis
//      val res = j.hgetAll(tableName)
//      res
//    }
//    result
//  }

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
