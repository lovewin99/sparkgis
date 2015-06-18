package com.utils

//import java.util.Map
import scala.collection.mutable.Map

import com.redis.RedisClientPool

/**
 * Created by wangxy on 15-6-11.
 */
object RedisUtils {
  val propFile = "/config/redis.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val host = prop.getOrElse("REDIS.HOST", "127.0.0.1")
  val port = prop.getOrElse("REDIS.PORT", "6379").toInt

  val pool = new RedisClientPool(host, port).pool

  def getClient = {
    pool.getFactory.makeObject()
  }

  def putMap2RedisTable(tableName: String, map: Map[String, String]) = {
    val client = getClient
    client.pipeline {
      f => f.hmset(tableName, map)
    }
    pool.returnObject(client)
  }

  def getResultMap(tableName: String): scala.collection.immutable.Map[String, String] = {
    val client = getClient
    val res = client.hgetall(tableName)
    pool.returnObject(client)
    res match {
      case Some(map) => map
      case None => scala.collection.immutable.Map()
    }
  }

  def delTable(name: String) {
    val client = getClient
    if (client.exists(name))
      client.del(name)
  }

  def main(args: Array[String]): Unit ={

  }

}
